using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Cluster.Client;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Counter representing the Recovery state for the cluster.
    /// 
    /// Key layout as follows:
    ///
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                     Leadership Term ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                  Log position for Snapshot                    |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |              Timestamp at beginning of Recovery               |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                    Replay required flag                       |
    ///  +---------------------------------------------------------------+
    ///  |                     Count of Services                         |
    ///  +---------------------------------------------------------------+
    ///  |             Snapshot Recording ID (Service ID 0)              |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |             Snapshot Recording ID (Service ID n)              |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///
    /// </summary>
    public class RecoveryState
    {
        /// <summary>
        /// Type id of a recovery state counter.
        /// </summary>
        public const int RECOVERY_STATE_TYPE_ID = 204;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "cluster recovery: leadershipTermId=";

        public const int LEADERSHIP_TERM_ID_OFFSET = 0;
        public static readonly int LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int REPLAY_FLAG_OFFSET = TIMESTAMP_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int SERVICE_COUNT_OFFSET = REPLAY_FLAG_OFFSET + BitUtil.SIZE_OF_INT;
        public static readonly int SNAPSHOT_RECORDING_IDS_OFFSET = SERVICE_COUNT_OFFSET + BitUtil.SIZE_OF_INT;


        /// <summary>
        /// Allocate a counter to represent the snapshot services should load on start.
        /// </summary>
        /// <param name="aeron">                to allocate the counter. </param>
        /// <param name="tempBuffer">           to use for building the key and label without allocation. </param>
        /// <param name="leadershipTermId">     at which the snapshot was taken. </param>
        /// <param name="logPosition">          at which the snapshot was taken. </param>
        /// <param name="timestamp">            the snapshot was taken. </param>
        /// <param name="hasReplay">            flag is true if all or part of the log must be replayed. </param>
        /// <param name="snapshotRecordingIds"> for the services to use during recovery indexed by service id. </param>
        /// <returns> the <seealso cref="Counter"/> for the recovery state. </returns>
        public static Counter Allocate(
            Aeron.Aeron aeron,
            IMutableDirectBuffer tempBuffer,
            long leadershipTermId,
            long logPosition,
            long timestamp,
            bool hasReplay,
            params long[] snapshotRecordingIds)
        {
            tempBuffer.PutLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
            tempBuffer.PutLong(LOG_POSITION_OFFSET, logPosition);
            tempBuffer.PutLong(TIMESTAMP_OFFSET, timestamp);
            tempBuffer.PutInt(REPLAY_FLAG_OFFSET, hasReplay ? 1 : 0);

            int serviceCount = snapshotRecordingIds.Length;
            tempBuffer.PutInt(SERVICE_COUNT_OFFSET, serviceCount);

            int keyLength = SNAPSHOT_RECORDING_IDS_OFFSET + (serviceCount * BitUtil.SIZE_OF_LONG);
            int maxRecordingIdsLength = SNAPSHOT_RECORDING_IDS_OFFSET + (serviceCount * BitUtil.SIZE_OF_LONG);

            if (maxRecordingIdsLength > CountersReader.MAX_KEY_LENGTH)
            {
                throw new ClusterException(maxRecordingIdsLength + " execeeds max key length " + CountersReader.MAX_KEY_LENGTH);
            }

            for (int i = 0; i < serviceCount; i++)
            {
                tempBuffer.PutLong(SNAPSHOT_RECORDING_IDS_OFFSET + (i * BitUtil.SIZE_OF_LONG), snapshotRecordingIds[i]);
            }


            int labelOffset = 0;
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelOffset, NAME);
            labelOffset += tempBuffer.PutLongAscii(keyLength + labelOffset, leadershipTermId);
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelOffset, " logPosition=");
            labelOffset += tempBuffer.PutLongAscii(keyLength + labelOffset, logPosition);
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelOffset, " hasReplay=" + hasReplay);

            return aeron.AddCounter(RECOVERY_STATE_TYPE_ID, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelOffset);
        }

        /// <summary>
        /// Find the active counter id for recovery state.
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterId(CountersReader counters)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                if (counters.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                    {
                        return i;
                    }
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }

        /// <summary>
        /// Get the leadership term id for the snapshot state. <see cref="Aeron.NULL_VALUE"/> if no snapshot for recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the leadership term id if found otherwise <see cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetLeadershipTermId(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        ///  Get the position at which the snapshot was taken. <seealso cref="Aeron.NULL_VALUE"/> if no snapshot for recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the log position if found otherwise <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetLogPosition(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + LOG_POSITION_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }


        /// <summary>
        /// Get the timestamp at the beginning of recovery. <seealso cref="Aeron.NULL_VALUE"/> if no snapshot for recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active recovery counter. </param>
        /// <returns> the timestamp if found otherwise <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetTimestamp(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + TIMESTAMP_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// Has the recovery process got a log to replay?
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active recovery counter. </param>
        /// <returns> true if a replay is required. </returns>
        public static bool HasReplay(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + REPLAY_FLAG_OFFSET) == 1;
                }
            }

            return false;
        }

        /// <summary>
        /// Get the recording id of the snapshot for a service.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active recovery counter. </param>
        /// <param name="serviceId"> for the snapshot required. </param>
        /// <returns> the count of replay terms if found otherwise <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetSnapshotRecordingId(CountersReader counters, int counterId, int serviceId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    int serviceCount = buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + SERVICE_COUNT_OFFSET);
                    if (serviceId < 0 || serviceId >= serviceCount)
                    {
                        throw new ClusterException("invalid serviceId " + serviceId + " for count of " + serviceCount);
                    }

                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + SNAPSHOT_RECORDING_IDS_OFFSET + (serviceId * BitUtil.SIZE_OF_LONG));
                }
            }

            throw new ClusterException("Active counter not found " + counterId);
        }
    }
}