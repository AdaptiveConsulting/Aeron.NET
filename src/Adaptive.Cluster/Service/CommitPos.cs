using System;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

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
    ///  |            Log Position at base of leadership term            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     Leadership Term ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                      Log Session ID                           |
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
        /// Represents a null value if the counter is not found.
        /// </summary>
        public const long NULL_VALUE = -1;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "commit-pos: leadershipTermId=";

        public static readonly int TERM_BASE_LOG_POSITION_OFFSET = 0;
        public static readonly int LEADERSHIP_TERM_ID_OFFSET = TERM_BASE_LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int LEADERSHIP_TERM_LENGTH_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int KEY_LENGTH = LEADERSHIP_TERM_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Allocate a counter to represent the commit position on stream for the current leadership term.
        /// </summary>
        /// <param name="aeron">                to allocate the counter. </param>
        /// <param name="tempBuffer">           to use for building the key and label without allocation. </param>
        /// <param name="leadershipTermId">     of the log at the beginning of the leadership term. </param>
        /// <param name="termBaseLogPosition">  of the log at the beginning of the leadership term. </param>
        /// <param name="leadershipTermLength"> length in bytes of the leadership term for the log. </param>
        /// <returns> the <seealso cref="Counter"/> for the commit position. </returns>
        public static Counter Allocate(
            Aeron.Aeron aeron,
            IMutableDirectBuffer tempBuffer,
            long leadershipTermId,
            long termBaseLogPosition,
            long leadershipTermLength)
        {
            tempBuffer.PutLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
            tempBuffer.PutLong(TERM_BASE_LOG_POSITION_OFFSET, termBaseLogPosition);
            tempBuffer.PutLong(LEADERSHIP_TERM_LENGTH_OFFSET, leadershipTermLength);


            int labelOffset = 0;
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
            labelOffset += tempBuffer.PutLongAscii(KEY_LENGTH + labelOffset, leadershipTermId);

            return aeron.AddCounter(COMMIT_POSITION_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
        }

        /// <summary>
        /// Get the leadership term id for the given commit position.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <returns> the leadership term id if found otherwise <seealso cref="NULL_VALUE"/>. </returns>
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

            return NULL_VALUE;
        }

        /// <summary>
        /// Get the accumulated log position as a base for this leadership term.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <returns> the base log position if found otherwise <seealso cref="NULL_VALUE"/>. </returns>
        public static long GetTermBaseLogPosition(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + TERM_BASE_LOG_POSITION_OFFSET);
                }
            }

            return NULL_VALUE;
        }

        /// <summary>
        /// Get the length in bytes for the leadership term.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active commit position. </param>
        /// <returns> the base log position if found otherwise <seealso cref="#NULL_VALUE"/>. </returns>
        public static long GetLeadershipTermLength(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + LEADERSHIP_TERM_LENGTH_OFFSET);
                }
            }

            return NULL_VALUE;
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