using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

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
    ///  |            Log Position at beginning for snapshot             |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     Leadership Term ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |              Timestamp at beginning of recovery               |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |               Count of leadership replay terms                |
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
        /// Represents a null counter id when not found.
        /// </summary>
        public const int NULL_COUNTER_ID = -1;

        /// <summary>
        /// Represents a null value if the counter is not found.
        /// </summary>
        public const int NULL_VALUE = -1;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "cluster recovery: ";

        public const int RECORDING_ID_OFFSET = 0;
        public static readonly int LOG_POSITION_OFFSET = RECORDING_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int LEADERSHIP_TERM_ID_OFFSET = LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int TIMESTAMP_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int REPLAY_TERM_COUNT_OFFSET = TIMESTAMP_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int KEY_LENGTH = REPLAY_TERM_COUNT_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Allocate a counter to represent the snapshot services should load on start.
        /// </summary>
        /// <param name="aeron">           to allocate the counter. </param>
        /// <param name="tempBuffer">      to use for building the key and label without allocation. </param>
        /// <param name="logPosition">     at which the snapshot was taken. </param>
        /// <param name="leadershipTermId">    at which the snapshot was taken. </param>
        /// <param name="timestamp">       the snapshot was taken. </param>
        /// <param name="replayTermCount"> for the count of terms to be replayed during recovery after snapshot. </param>
        /// <returns> the <seealso cref="Counter"/> for the consensus position. </returns>
        public static Counter Allocate(Aeron.Aeron aeron, IMutableDirectBuffer tempBuffer, long logPosition, long leadershipTermId, long timestamp, int replayTermCount)
        {
            tempBuffer.PutLong(LOG_POSITION_OFFSET, logPosition);
            tempBuffer.PutLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
            tempBuffer.PutLong(TIMESTAMP_OFFSET, timestamp);
            tempBuffer.PutInt(REPLAY_TERM_COUNT_OFFSET, replayTermCount);

            int labelOffset = 0;
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
            labelOffset += tempBuffer.PutLongAscii(KEY_LENGTH + labelOffset, logPosition);
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " ");
            labelOffset += tempBuffer.PutLongAscii(KEY_LENGTH + labelOffset, leadershipTermId);
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " ");
            labelOffset += tempBuffer.PutIntAscii(KEY_LENGTH + labelOffset, replayTermCount);

            return aeron.AddCounter(RECOVERY_STATE_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
        }

        /// <summary>
        /// Find the active counter id for a snapshot.
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <returns> the counter id if found otherwise <seealso cref="#NULL_COUNTER_ID"/>. </returns>
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

            return NULL_COUNTER_ID;
        }

        /// <summary>
        /// Get the recording id for the current leadership term.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the counter id if found otherwise <seealso cref="#NULL_VALUE"/>. </returns>
        public static long GetRecordingId(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + RECORDING_ID_OFFSET);
                }
            }

            return NULL_VALUE;
        }

        /// <summary>
        /// Get the log position for the snapshot.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the log position if found otherwise <seealso cref="#NULL_VALUE"/>. </returns>
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

            return NULL_VALUE;
        }

        /// <summary>
        /// Get the leadership term id for the snapshot.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the message index if found otherwise <seealso cref="#NULL_VALUE"/>. </returns>
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

            return NULL_VALUE;
        }

        /// <summary>
        /// Get the timestamp for when the snapshot was taken.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the timestamp if found otherwise <seealso cref="#NULL_VALUE"/>. </returns>
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

            return NULL_VALUE;
        }

        /// <summary>
        /// Get the count of terms that will be replayed during recovery.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active consensus position. </param>
        /// <returns> the count of replay terms if found otherwise <seealso cref="#NULL_VALUE"/>. </returns>
        public static int GetReplayTermCount(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECOVERY_STATE_TYPE_ID)
                {
                    return buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + REPLAY_TERM_COUNT_OFFSET);
                }
            }

            return NULL_VALUE;
        }
    }
}