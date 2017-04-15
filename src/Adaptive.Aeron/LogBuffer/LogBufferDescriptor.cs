/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.CompilerServices;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Layout description for log buffers which contains partitions of terms with associated term meta data,
    /// plus ending with overall log meta data.
    /// 
    /// <pre>
    ///  +----------------------------+
    ///  |           Term 0           |
    ///  +----------------------------+
    ///  |           Term 1           |
    ///  +----------------------------+
    ///  |           Term 2           |
    ///  +----------------------------+
    ///  |        Log Meta Data       |
    ///  +----------------------------+
    /// </pre>
    /// </summary>
    public class LogBufferDescriptor
    {
        /// <summary>
        /// The number of partitions the log is divided into terms and a meta data buffer.
        /// </summary>
        public const int PARTITION_COUNT = 3;

        /// <summary>
        /// Section index for which buffer contains the log meta data.
        /// </summary>
        public static readonly int LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

        /// <summary>
        /// Minimum buffer length for a log term
        /// </summary>
        public const int TERM_MIN_LENGTH = 64 * 1024;

        static LogBufferDescriptor()
        {
            var offset = 0;
            TERM_TAIL_COUNTERS_OFFSET = offset;

            offset += (BitUtil.SIZE_OF_LONG * PARTITION_COUNT);
            LOG_ACTIVE_PARTITION_INDEX_OFFSET = offset;

            offset = (BitUtil.CACHE_LINE_LENGTH * 2);
            LOG_TIME_OF_LAST_SM_OFFSET = offset;

            offset += (BitUtil.CACHE_LINE_LENGTH*2);
            LOG_CORRELATION_ID_OFFSET = offset;
            LOG_INITIAL_TERM_ID_OFFSET = LOG_CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
            LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = LOG_INITIAL_TERM_ID_OFFSET + BitUtil.SIZE_OF_INT;
            LOG_MTU_LENGTH_OFFSET = LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

            offset += BitUtil.CACHE_LINE_LENGTH;
            LOG_DEFAULT_FRAME_HEADER_OFFSET = offset;

            LOG_META_DATA_LENGTH = offset + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH;
        }

        // *******************************
        // *** Log Meta Data Constants ***
        // *******************************

        /// <summary>
        /// Offset within the meta data where the tail values are stored.
        /// </summary>
        public static readonly int TERM_TAIL_COUNTERS_OFFSET;

        /// <summary>
        /// Offset within the log meta data where the active partition index is stored.
        /// </summary>
        public static readonly int LOG_ACTIVE_PARTITION_INDEX_OFFSET;

        /// <summary>
        /// Offset within the log meta data where the time of last SM is stored.
        /// </summary>
        public static readonly int LOG_TIME_OF_LAST_SM_OFFSET;

        /// <summary>
        /// Offset within the log meta data where the active term id is stored.
        /// </summary>
        public static readonly int LOG_INITIAL_TERM_ID_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the length field for the frame header is stored.
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the MTU length is stored;
        /// </summary>
        public static readonly int LOG_MTU_LENGTH_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the
        /// </summary>
        public static readonly int LOG_CORRELATION_ID_OFFSET;

        /// <summary>
        /// Offset at which the default frame headers begin.
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_OFFSET;

        /// <summary>
        ///  Maximum length of a frame header
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = BitUtil.CACHE_LINE_LENGTH*2;


        /// <summary>
        /// Total length of the log meta data buffer in bytes.
        /// 
        /// <pre>
        ///   0                   1                   2                   3
        ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        ///  |                       Tail Counter 0                          |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                       Tail Counter 1                          |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                       Tail Counter 2                          |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                      Cache Line Padding                      ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                   Active Partition Index                      |
        ///  +---------------------------------------------------------------+
        ///  |                      Cache Line Padding                      ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                 Time of Last Status Message                   |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                      Cache Line Padding                      ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                 Registration / Correlation ID                 |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                        Initial Term Id                        |
        ///  +---------------------------------------------------------------+
        ///  |                  Default Frame Header Length                  |
        ///  +---------------------------------------------------------------+
        ///  |                          MTU Length                           |
        ///  +---------------------------------------------------------------+
        ///  |                      Cache Line Padding                      ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                    Default Frame Header                      ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        /// </pre>
        /// </summary>
        public static readonly int LOG_META_DATA_LENGTH;

        /// <summary>
        /// Check that term length is valid and alignment is valid.
        /// </summary>
        /// <param name="termLength"> to be checked. </param>
        /// <exception cref="InvalidOperationException"> if the length is not as expected. </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CheckTermLength(int termLength)
        {
            if (termLength < TERM_MIN_LENGTH)
            {
                string s = $"Term length less than min length of {TERM_MIN_LENGTH:D}, length={termLength:D}";
                throw new InvalidOperationException(s);
            }

            if ((termLength & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
            {
                string s = $"Term length not a multiple of {FrameDescriptor.FRAME_ALIGNMENT:D}, length={termLength:D}";
                throw new InvalidOperationException(s);
            }
        }

        /// <summary>
        /// Get the value of the initial Term id used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the initial Term id used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialTermId(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetInt(LOG_INITIAL_TERM_ID_OFFSET);
        }

        /// <summary>
        /// Set the initial term at which this log begins. Initial should be randomised so that stream does not get
        /// reused accidentally.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="initialTermId">     value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialTermId(UnsafeBuffer logMetaDataBuffer, int initialTermId)
        {
            logMetaDataBuffer.PutInt(LOG_INITIAL_TERM_ID_OFFSET, initialTermId);
        }

        /// <summary>
        /// Get the value of the MTU length used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the MTU length used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MtuLength(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetInt(LOG_MTU_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the MTU length used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="mtuLength">         value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void MtuLength(UnsafeBuffer logMetaDataBuffer, int mtuLength)
        {
            logMetaDataBuffer.PutInt(LOG_MTU_LENGTH_OFFSET, mtuLength);
        }

        /// <summary>
        /// Get the value of the correlation ID for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the correlation ID used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long CorrelationId(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetLong(LOG_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation ID used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="id">                value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CorrelationId(UnsafeBuffer logMetaDataBuffer, long id)
        {
            logMetaDataBuffer.PutLong(LOG_CORRELATION_ID_OFFSET, id);
        }

        /// <summary>
        /// Get the value of the time of last SM in <seealso cref="System#currentTimeMillis()"/>.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of time of last SM </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long TimeOfLastStatusMessage(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetLongVolatile(LOG_TIME_OF_LAST_SM_OFFSET);
        }

        /// <summary>
        /// Set the value of the time of last SM used by the producer of this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="timeInMillis">      value of the time of last SM in <seealso cref="System#currentTimeMillis()"/> </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void TimeOfLastStatusMessage(UnsafeBuffer logMetaDataBuffer, long timeInMillis)
        {
            logMetaDataBuffer.PutLongOrdered(LOG_TIME_OF_LAST_SM_OFFSET, timeInMillis);
        }

        /// <summary>
        /// Get the value of the active partition index used by the producer of this log. Consumers may have a different active
        /// index if they are running behind. The read is done with volatile semantics.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the active partition index used by the producer of this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ActivePartitionIndex(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetIntVolatile(LOG_ACTIVE_PARTITION_INDEX_OFFSET);
        }

        /// <summary>
        /// Set the value of the current active partition index for the producer using memory ordered semantics.
        /// </summary>
        /// <param name="logMetaDataBuffer">    containing the meta data. </param>
        /// <param name="activePartitionIndex"> value of the active partition index used by the producer of this log. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ActivePartitionIndex(UnsafeBuffer logMetaDataBuffer, int activePartitionIndex)
        {
            logMetaDataBuffer.PutIntOrdered(LOG_ACTIVE_PARTITION_INDEX_OFFSET, activePartitionIndex);
        }

        /// <summary>
        /// Rotate to the next partition in sequence for the term id.
        /// </summary>
        /// <param name="currentIndex"> partition index </param>
        /// <returns> the next partition index </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NextPartitionIndex(int currentIndex)
        {
            return (currentIndex + 1)%PARTITION_COUNT;
        }

        /// <summary>
        /// Determine the partition index to be used given the initial term and active term ids.
        /// </summary>
        /// <param name="initialTermId"> at which the log buffer usage began </param>
        /// <param name="activeTermId">  that is in current usage </param>
        /// <returns> the index of which buffer should be used </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByTerm(int initialTermId, int activeTermId)
        {
            return (activeTermId - initialTermId)%PARTITION_COUNT;
        }

        /// <summary>
        /// Determine the partition index based on number of terms that have passed.
        /// </summary>
        /// <param name="termCount"> for the number of terms that have passed. </param>
        /// <returns> the partition index for the term count. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByTermCount(long termCount)
        {
            return (int)(termCount%PARTITION_COUNT);
        }

        /// <summary>
        /// Determine the partition index given a stream position.
        /// </summary>
        /// <param name="position"> in the stream in bytes. </param>
        /// <param name="positionBitsToShift"> number of times to right shift the position for term count </param>
        /// <returns> the partition index for the position </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByPosition(long position, int positionBitsToShift)
        {
            return (int) (((long) ((ulong) position >> positionBitsToShift))%PARTITION_COUNT);
        }

        /// <summary>
        /// Compute the current position in absolute number of bytes.
        /// </summary>
        /// <param name="activeTermId">        active term id. </param>
        /// <param name="termOffset">          in the term. </param>
        /// <param name="positionBitsToShift"> number of times to left shift the term count </param>
        /// <param name="initialTermId">       the initial term id that this stream started on </param>
        /// <returns> the absolute position in bytes </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ComputePosition(int activeTermId, int termOffset, int positionBitsToShift, int initialTermId)
        {
            long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

            return (termCount << positionBitsToShift) + termOffset;
        }

        /// <summary>
        /// Compute the current position in absolute number of bytes for the beginning of a term.
        /// </summary>
        /// <param name="activeTermId">        active term id. </param>
        /// <param name="positionBitsToShift"> number of times to left shift the term count </param>
        /// <param name="initialTermId">       the initial term id that this stream started on </param>
        /// <returns> the absolute position in bytes </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ComputeTermBeginPosition(int activeTermId, int positionBitsToShift, int initialTermId)
        {
            long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

            return termCount << positionBitsToShift;
        }

        /// <summary>
        /// Compute the term id from a position.
        /// </summary>
        /// <param name="position">            to calculate from </param>
        /// <param name="positionBitsToShift"> number of times to right shift the position </param>
        /// <param name="initialTermId">       the initial term id that this stream started on </param>
        /// <returns> the term id according to the position </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeTermIdFromPosition(long position, int positionBitsToShift, int initialTermId)
        {
            return ((int) ((long) ((ulong) position >> positionBitsToShift)) + initialTermId);
        }

        /// <summary>
        /// Compute the term offset from a given position.
        /// </summary>
        /// <param name="position">            to calculate from </param>
        /// <param name="positionBitsToShift"> number of times to right shift the position </param>
        /// <returns> the offset within the term that represents the position </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeTermOffsetFromPosition(long position, int positionBitsToShift)
        {
            var mask = (1L << positionBitsToShift) - 1L;

            return (int) (position & mask);
        }

        /// <summary>
        /// Compute the total length of a log file given the term length.
        /// </summary>
        /// <param name="termLength"> on which to base the calculation. </param>
        /// <returns> the total length of the log file. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ComputeLogLength(int termLength)
        {
            return (termLength * PARTITION_COUNT) + LOG_META_DATA_LENGTH;
        }

        /// <summary>
        /// Compute the term length based on the total length of the log.
        /// </summary>
        /// <param name="logLength"> the total length of the log. </param>
        /// <returns> length of an individual term buffer in the log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeTermLength(long logLength)
        {
            return (int)((logLength - LOG_META_DATA_LENGTH) / PARTITION_COUNT);
        }

        /// <summary>
        /// Store the default frame header to the log meta data buffer.
        /// </summary>
        /// <param name="logMetaDataBuffer"> into which the default headers should be stored. </param>
        /// <param name="defaultHeader">     to be stored. </param>
        /// <exception cref="ArgumentException"> if the default header is larger than <seealso cref="LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH"/> </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void StoreDefaultFrameHeader(UnsafeBuffer logMetaDataBuffer, IDirectBuffer defaultHeader)
        {
            if (defaultHeader.Capacity != DataHeaderFlyweight.HEADER_LENGTH)
            {
                throw new ArgumentException(
                    $"Default header of {defaultHeader.Capacity:D} not equal to {DataHeaderFlyweight.HEADER_LENGTH:D}");
            }

            logMetaDataBuffer.PutInt(LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
            logMetaDataBuffer.PutBytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, defaultHeader, 0, DataHeaderFlyweight.HEADER_LENGTH);
        }

        /// <summary>
        /// Get a wrapper around the default frame header from the log meta data.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the raw bytes for the default frame header. </param>
        /// <returns> a buffer wrapping the raw bytes. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static UnsafeBuffer DefaultFrameHeader(UnsafeBuffer logMetaDataBuffer)
        {
            return new UnsafeBuffer(logMetaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
        }

        /// <summary>
        /// Apply the default header for a message in a term.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the default headers. </param>
        /// <param name="termBuffer">        to which the default header should be applied. </param>
        /// <param name="termOffset">        at which the default should be applied. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ApplyDefaultHeader(UnsafeBuffer logMetaDataBuffer, UnsafeBuffer termBuffer, int termOffset)
        {
            termBuffer.PutBytes(termOffset, logMetaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
        }

        /// <summary>
        /// Rotate the log and update the default headers for the new term.
        /// </summary>
        /// <param name="logPartitions">     for the partitions of the log. </param>
        /// <param name="logMetaDataBuffer"> for the meta data. </param>
        /// <param name="activeIndex">       current active index. </param>
        /// <param name="newTermId">         to be used in the default headers. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void RotateLog(UnsafeBuffer logMetaDataBuffer, int activePartitionIndex, int termId)
        {
            var nextIndex = NextPartitionIndex(activePartitionIndex);
            InitialiseTailWithTermId(logMetaDataBuffer, nextIndex, termId);
            ActivePartitionIndex(logMetaDataBuffer, nextIndex);
        }

        /// <summary>
        /// Set the initial value for the termId in the upper bits of the tail counter.
        /// </summary>
        /// <param name="termMetaData">  contain the tail counter. </param>
        /// <param name="initialTermId"> to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialiseTailWithTermId(UnsafeBuffer logMetaData, int partitionIndex, int termId)
        {
            logMetaData.PutLong(TERM_TAIL_COUNTERS_OFFSET + partitionIndex * BitUtil.SIZE_OF_LONG, (long)termId << 32);
        }

        /// <summary>
        /// Get the termId from a packed raw tail value.
        /// </summary>
        /// <param name="rawTail"> containing the termId </param>
        /// <returns> the termId from a packed raw tail value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermId(long rawTail)
        {
            return (int) ((long) ((ulong) rawTail >> 32));
        }

        /// <summary>
        /// Read the termOffset from a packed raw tail value.
        /// </summary>
        /// <param name="rawTail">    containing the termOffset. </param>
        /// <param name="termLength"> that the offset cannot exceed. </param>
        /// <returns> the termOffset value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermOffset(long rawTail, long termLength)
        {
            var tail = rawTail & 0xFFFFFFFFL;

            return (int) Math.Min(tail, termLength);
        }

        /// <summary>
        /// Get the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="logMetaDataBuffer">containing the tail counters.</param>
        /// <param name="partitionIndex">for the tail counter.</param>
        /// <returns>the raw value of the tail for the current active partition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long RawTailVolatile(UnsafeBuffer logMetaDataBuffer, int partitionIndex)
        {
            return logMetaDataBuffer.GetLongVolatile(TERM_TAIL_COUNTERS_OFFSET + BitUtil.SIZE_OF_LONG*partitionIndex);
        }

        /// <summary>
        /// Get the raw value of the tail for the current active partition.
        /// </summary>
        /// <param name="logMetaDataBuffer">logMetaDataBuffer containing the tail counters.</param>
        /// <returns>the raw value of the tail for the current active partition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long RawTailVolatile(UnsafeBuffer logMetaDataBuffer)
        {
            int partitionIndex = ActivePartitionIndex(logMetaDataBuffer);
            return logMetaDataBuffer.GetLongVolatile(TERM_TAIL_COUNTERS_OFFSET + BitUtil.SIZE_OF_LONG * partitionIndex);
        }
    }
}