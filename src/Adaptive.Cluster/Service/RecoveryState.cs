using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Cluster.Client;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Counter representing the Recovery State for the cluster.
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
    ///  |                         Cluster ID                            |
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
    public static class RecoveryState
    {
        /// <summary>
        /// Type id of a recovery state counter.
        /// </summary>
        public const int RECOVERY_STATE_TYPE_ID = AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID;

        /// <summary>
        /// Human-readable name for the counter.
        /// </summary>
        public const string NAME = "Cluster recovery: leadershipTermId=";

        /// <summary>
        /// Offset of the <c>term-id</c> field.
        /// </summary>
        public const int LEADERSHIP_TERM_ID_OFFSET = 0;

        /// <summary>
        /// Offset of the <c>log-position</c> field.
        /// </summary>
        public const int LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;

        /// <summary>
        /// Offset of the <c>timestamp</c> field.
        /// </summary>
        public const int TIMESTAMP_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;

        /// <summary>
        /// Offset of the <c>cluster-id</c> field.
        /// </summary>
        public const int CLUSTER_ID_OFFSET = TIMESTAMP_OFFSET + SIZE_OF_LONG;

        /// <summary>
        /// Offset of the <c>service-count</c> field.
        /// </summary>
        public const int SERVICE_COUNT_OFFSET = CLUSTER_ID_OFFSET + SIZE_OF_INT;

        /// <summary>
        /// Offset of the <c>snapshot-recording-ids</c> field.
        /// </summary>
        public const int SNAPSHOT_RECORDING_IDS_OFFSET = SERVICE_COUNT_OFFSET + SIZE_OF_INT;

        /// <summary>
        /// Find the active counter id for recovery state.
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <param name="clusterId"> to constrain the search. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterId(CountersReader counters, int clusterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                var counterState = counters.GetCounterState(i);
                if (CountersReader.RECORD_ALLOCATED == counterState &&
                    RECOVERY_STATE_TYPE_ID == counters.GetCounterTypeId(i))
                {
                    if (buffer.GetInt(CountersReader.MetaDataOffset(i) + CountersReader.KEY_OFFSET +
                                      CLUSTER_ID_OFFSET) == clusterId)
                    {
                        return i;
                    }
                }
                else if (CountersReader.RECORD_UNUSED == counterState)
                {
                    break;
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }

        /// <summary>
        /// Get the leadership term id for the snapshot state. <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if no snapshot for recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the leadership term id if found otherwise <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </returns>
        public static long GetLeadershipTermId(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED &&
                counters.GetCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
            {
                return buffer.GetLong(CountersReader.MetaDataOffset(counterId) + CountersReader.KEY_OFFSET +
                                      LEADERSHIP_TERM_ID_OFFSET);
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        ///  Get the position at which the snapshot was taken. <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if no snapshot for recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the log position if found otherwise <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </returns>
        public static long GetLogPosition(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED &&
                counters.GetCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
            {
                return buffer.GetLong(CountersReader.MetaDataOffset(counterId) + CountersReader.KEY_OFFSET +
                                      LOG_POSITION_OFFSET);
            }

            return Aeron.Aeron.NULL_VALUE;
        }


        /// <summary>
        /// Get the timestamp at the beginning of recovery. <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if no snapshot for recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active recovery counter. </param>
        /// <returns> the timestamp if found otherwise <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </returns>
        public static long GetTimestamp(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED &&
                counters.GetCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
            {
                return buffer.GetLong(CountersReader.MetaDataOffset(counterId) + CountersReader.KEY_OFFSET +
                                      TIMESTAMP_OFFSET);
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// Get the recording id of the snapshot for a service.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active recovery counter. </param>
        /// <param name="serviceId"> for the snapshot required. </param>
        /// <returns> the count of replay terms if found otherwise <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </returns>
        public static long GetSnapshotRecordingId(CountersReader counters, int counterId, int serviceId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED &&
                counters.GetCounterTypeId(counterId) == RECOVERY_STATE_TYPE_ID)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                int serviceCount = buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + SERVICE_COUNT_OFFSET);
                if (serviceId < 0 || serviceId >= serviceCount)
                {
                    throw new ClusterException("invalid serviceId " + serviceId + " for count of " + serviceCount);
                }

                return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + SNAPSHOT_RECORDING_IDS_OFFSET +
                                      (serviceId * SIZE_OF_LONG));
            }

            throw new ClusterException("active counter not found " + counterId);
        }
    }
}