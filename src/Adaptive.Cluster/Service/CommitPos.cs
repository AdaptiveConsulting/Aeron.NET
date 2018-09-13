using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Cluster.Client;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Counter representing the commit position that can consumed by a state machine on a stream for the current
    /// leadership term.
    ///
    /// Key layout as follows:
    ///
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |             Recording ID for the leadership term              |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                        Log Position                           |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                       Max Log Position                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// 
    /// </summary>
    public class CommitPos
    {
        /// <summary>
        /// Type id of a consensus position counter.
        /// </summary>
        public const int COMMIT_POSITION_TYPE_ID = 203;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "commit-pos: leadershipTermId=";

        public static readonly int LEADERSHIP_TERM_ID_OFFSET = 0;
        public static readonly int LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int MAX_LOG_POSITION_OFFSET = LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int KEY_LENGTH = MAX_LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Allocate a counter to represent the commit position on stream for the current leadership term.
        /// </summary>
        /// <param name="aeron">                to allocate the counter. </param>
        /// <param name="tempBuffer">           to use for building the key and label without allocation. </param>
        /// <param name="leadershipTermId">     of the log at the beginning of the leadership term. </param>
        /// <param name="logPosition">  of the log at the beginning of the leadership term. </param>
        /// <param name="maxLogPosition"> length in bytes of the leadership term for the log. </param>
        /// <returns> the <seealso cref="Counter"/> for the commit position. </returns>
        public static Counter Allocate(
            Aeron.Aeron aeron,
            IMutableDirectBuffer tempBuffer,
            long leadershipTermId,
            long logPosition,
            long maxLogPosition)
        {
            tempBuffer.PutLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
            tempBuffer.PutLong(LOG_POSITION_OFFSET, logPosition);
            tempBuffer.PutLong(MAX_LOG_POSITION_OFFSET, maxLogPosition);

            int labelOffset = BitUtil.Align(KEY_LENGTH, BitUtil.SIZE_OF_INT);
            int labelLength = 0;
            labelLength += tempBuffer.PutStringWithoutLengthAscii(labelOffset + labelLength, NAME);
            labelLength += tempBuffer.PutLongAscii(labelOffset + labelLength, leadershipTermId);

            return aeron.AddCounter(COMMIT_POSITION_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, labelOffset, labelLength);
        }

        /// <summary>
        /// Get the leadership term id for the given commit position.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <returns> the leadership term id if found otherwise <see cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetLeadershipTermId(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// Get the log position at which the commit tracking will begin.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <returns> the base log position if found otherwise <see cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetLogPosition(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + LOG_POSITION_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// Get the maximum log position that a tracking session can reach. The get operation has volatile semantics.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <returns> the log position if found otherwise <see cref="Aeron.NULL_VALUE"/>. </returns>
        public static long GetMaxLogPosition(CountersReader counters, int counterId)
        {
            IAtomicBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    return buffer.GetLongVolatile(recordOffset + CountersReader.KEY_OFFSET + MAX_LOG_POSITION_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }

        /// <summary>
        /// Set the maximum log position that a tracking session can reach. The set operation has volatile semantics.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <param name="value">     to set for the new max position. </param>
        /// <exception cref="ClusterException"> if the counter id is not valid. </exception>
        public static void SetMaxLogPosition(CountersReader counters, int counterId, long value)
        {
            IAtomicBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    buffer.PutLongVolatile(recordOffset + CountersReader.KEY_OFFSET + MAX_LOG_POSITION_OFFSET, value);
                    return;
                }
            }

            throw new ClusterException("Counter id not valid: " + counterId);
        }


        /// <summary>
        /// Is the counter still active and log still recording?
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> to search for. </param>
        /// <returns> true if the counter is still active otherwise false. </returns>
        public static bool IsActive(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                return buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID;
            }

            return false;
        }
    }
}