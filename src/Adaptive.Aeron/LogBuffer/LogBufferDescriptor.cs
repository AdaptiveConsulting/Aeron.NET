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
using Adaptive.Agrona.Util;

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

        /// <summary>
        /// Maximum buffer length for a log term
        /// </summary>
        public const int TERM_MAX_LENGTH = 1024 * 1024 * 1024;
        
        /// <summary>
        /// Minimum page size
        /// </summary>
        public const int PAGE_MIN_SIZE = 4 * 1024;

        /// <summary>
        /// Maximum page size
        /// </summary>
        public const int PAGE_MAX_SIZE = 1024 * 1024 * 1024;

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
        public static readonly int LOG_ACTIVE_TERM_COUNT_OFFSET;

        /// <summary>
        /// Offset within the log meta data where the position of the End of Stream is stored.
        /// </summary>
        public static readonly int LOG_END_OF_STREAM_POSITION_OFFSET;
        
        /// <summary>
        /// Offset within the log meta data where whether the log is connected or not is stored.
        /// </summary>
        public static readonly int LOG_IS_CONNECTED_OFFSET;

        /// <summary>
        /// Offset within the log meta data where the active term id is stored.
        /// </summary>
        public static readonly int LOG_INITIAL_TERM_ID_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the length field for the frame header is stored.
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the MTU length is stored.
        /// </summary>
        public static readonly int LOG_MTU_LENGTH_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the correlation id is stored.
        /// </summary>
        public static readonly int LOG_CORRELATION_ID_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the term length is stored.
        /// </summary>
        public static readonly int LOG_TERM_LENGTH_OFFSET;

        /// <summary>
        /// Offset within the log meta data which the page size is stored.
        /// </summary>
        public static readonly int LOG_PAGE_SIZE_OFFSET;
        
        /// <summary>
        /// Offset at which the default frame headers begin.
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_OFFSET;

        /// <summary>
        ///  Maximum length of a frame header
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = BitUtil.CACHE_LINE_LENGTH * 2;


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
        ///  |                      Active Term Count                        |
        ///  +---------------------------------------------------------------+
        ///  |                     Cache Line Padding                       ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                    End of Stream Position                     |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                        Is Connected                           |
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
        ///  |                         Term Length                           |
        ///  +---------------------------------------------------------------+
        ///  |                          Page Size                            |
        ///  +---------------------------------------------------------------+
        ///  |                      Cache Line Padding                      ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                     Default Frame Header                     ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        /// </pre>
        /// </summary>
        public static readonly int LOG_META_DATA_LENGTH;
        
        static LogBufferDescriptor()
        {
            int offset = 0;
            TERM_TAIL_COUNTERS_OFFSET = offset;

            offset += (BitUtil.SIZE_OF_LONG * PARTITION_COUNT);
            LOG_ACTIVE_TERM_COUNT_OFFSET = offset;

            offset = (BitUtil.CACHE_LINE_LENGTH * 2);
            LOG_END_OF_STREAM_POSITION_OFFSET = offset;
            LOG_IS_CONNECTED_OFFSET = LOG_END_OF_STREAM_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;

            offset += (BitUtil.CACHE_LINE_LENGTH * 2);
            LOG_CORRELATION_ID_OFFSET = offset;
            LOG_INITIAL_TERM_ID_OFFSET = LOG_CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
            LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = LOG_INITIAL_TERM_ID_OFFSET + BitUtil.SIZE_OF_INT;
            LOG_MTU_LENGTH_OFFSET = LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
            LOG_TERM_LENGTH_OFFSET = LOG_MTU_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
            LOG_PAGE_SIZE_OFFSET = LOG_TERM_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

            offset += BitUtil.CACHE_LINE_LENGTH;
            LOG_DEFAULT_FRAME_HEADER_OFFSET = offset;

            LOG_META_DATA_LENGTH = BitUtil.Align(offset + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH, PAGE_MIN_SIZE);
        }

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
                ThrowHelper.ThrowInvalidOperationException(
                    $"Term length less than min length of {TERM_MIN_LENGTH:D}, length={termLength:D}");
            }

            if (termLength > TERM_MAX_LENGTH)
            {
                ThrowHelper.ThrowInvalidOperationException(
                    $"Term length more than max length of {TERM_MAX_LENGTH:D}: length = {termLength:D}");
            }

            if (!BitUtil.IsPowerOfTwo(termLength))
            {
                ThrowHelper.ThrowInvalidOperationException("Term length not a power of 2: length=" + termLength);
            }
        }
        
        /// <summary>
        /// Check that page size is valid and alignment is valid.
        /// </summary>
        /// <param name="pageSize"> to be checked. </param>
        /// <exception cref="InvalidOperationException"> if the size is not as expected. </exception>
        public static void CheckPageSize(int pageSize)
        {
            if (pageSize < PAGE_MIN_SIZE)
            {
                ThrowHelper.ThrowInvalidOperationException($"Page size less than min size of {PAGE_MIN_SIZE}: page size={pageSize}");
            }

            if (pageSize > PAGE_MAX_SIZE)
            {
                ThrowHelper.ThrowInvalidOperationException($"Page size more than max size of {PAGE_MAX_SIZE}: page size={pageSize}");
            }

            if (!BitUtil.IsPowerOfTwo(pageSize))
            {
                ThrowHelper.ThrowInvalidOperationException($"Page size not a power of 2: page size={pageSize}");
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
        /// Get the value of the Term Length used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the term length used for this log. </returns>
        public static int TermLength(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetInt(LOG_TERM_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the term length used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="termLength">        value to be set. </param>
        public static void TermLength(UnsafeBuffer logMetaDataBuffer, int termLength)
        {
            logMetaDataBuffer.PutInt(LOG_TERM_LENGTH_OFFSET, termLength);
        }

        /// <summary>
        /// Get the value of the page size used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the page size used for this log. </returns>
        public static int PageSize(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetInt(LOG_PAGE_SIZE_OFFSET);
        }

        /// <summary>
        /// Set the page size used for this log.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="pageSize">          value to be set. </param>
        public static void PageSize(UnsafeBuffer logMetaDataBuffer, int pageSize)
        {
            logMetaDataBuffer.PutInt(LOG_PAGE_SIZE_OFFSET, pageSize);
        }

        /// <summary>
        /// Get the value of the correlation ID for this log relating to the command which created it.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the correlation ID used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long CorrelationId(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetLong(LOG_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation ID used for this log relating to the command which created it.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="id">                value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CorrelationId(UnsafeBuffer logMetaDataBuffer, long id)
        {
            logMetaDataBuffer.PutLong(LOG_CORRELATION_ID_OFFSET, id);
        }

        /// <summary>
        /// Get whether the log is considered connected or not by the driver.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> whether the log is considered connected or not by the driver. </returns>
        public static bool IsConnected(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetIntVolatile(LOG_IS_CONNECTED_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is considered connected or not by the driver.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="isConnected">       or not </param>
        public static void IsConnected(UnsafeBuffer logMetaDataBuffer, bool isConnected)
        {
            logMetaDataBuffer.PutIntOrdered(LOG_IS_CONNECTED_OFFSET, isConnected ? 1 : 0);
        }

        /// <summary>
        /// Get the value of the end of stream position.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of end of stream position </returns>
        public static long EndOfStreamPosition(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetLongVolatile(LOG_END_OF_STREAM_POSITION_OFFSET);
        }

        /// <summary>
        /// Set the value of the end of stream position.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="position">          value of the end of stream position </param>
        public static void EndOfStreamPosition(UnsafeBuffer logMetaDataBuffer, long position)
        {
            logMetaDataBuffer.PutLongOrdered(LOG_END_OF_STREAM_POSITION_OFFSET, position);
        }

        /// <summary>
        /// Get the value of the active term count used by the producer of this log. Consumers may have a different
        /// active term count if they are running behind. The read is done with volatile semantics.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <returns> the value of the active term count used by the producer of this log. </returns>
        public static int ActiveTermCount(UnsafeBuffer logMetaDataBuffer)
        {
            return logMetaDataBuffer.GetIntVolatile(LOG_ACTIVE_TERM_COUNT_OFFSET);
        }
        
        /// <summary>
        /// Set the value of the current active term count for the producer using memory ordered semantics.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="termCount">         value of the active term count used by the producer of this log. </param>
        public static void ActiveTermCountOrdered(UnsafeBuffer logMetaDataBuffer, int termCount)
        {
            logMetaDataBuffer.PutIntOrdered(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
        }
        
        /// <summary>
        /// Compare and set the value of the current active term count.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="expectedTermCount"> value of the active term count expected in the log </param>
        /// <param name="updateTermCount">   value of the active term count to be updated in the log </param>
        /// <returns> true if successful otherwise false. </returns>
        public static bool CasActiveTermCount(UnsafeBuffer logMetaDataBuffer, int expectedTermCount, int updateTermCount)
        {
            return logMetaDataBuffer.CompareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, expectedTermCount, updateTermCount);
        }
        
        /// <summary>
        /// Set the value of the current active partition index for the producer.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the meta data. </param>
        /// <param name="termCount">         value of the active term count used by the producer of this log. </param>
        public static void ActiveTermCount(UnsafeBuffer logMetaDataBuffer, int termCount)
        {
            logMetaDataBuffer.PutInt(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
        }

        /// <summary>
        /// Rotate to the next partition in sequence for the term id.
        /// </summary>
        /// <param name="currentIndex"> partition index </param>
        /// <returns> the next partition index </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NextPartitionIndex(int currentIndex)
        {
            return (currentIndex + 1) % PARTITION_COUNT;
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
            return (activeTermId - initialTermId) % PARTITION_COUNT;
        }

        /// <summary>
        /// Determine the partition index based on number of terms that have passed.
        /// </summary>
        /// <param name="termCount"> for the number of terms that have passed. </param>
        /// <returns> the partition index for the term count. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByTermCount(long termCount)
        {
            return (int) (termCount % PARTITION_COUNT);
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
            return (int) (((long) ((ulong) position >> positionBitsToShift)) % PARTITION_COUNT);
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
        /// Compute the total length of a log file given the term length.
        ///     
        /// Assumes <see cref="TERM_MAX_LENGTH"/> is 1GB and that filePageSize is 1GB or less and a power of 2.
        /// </summary>
        /// <param name="termLength"> on which to base the calculation. </param>
        /// <param name="filePageSize"> to use for log. </param>
        /// <returns> the total length of the log file. </returns>
        public static long ComputeLogLength(int termLength, int filePageSize)
        {
            if (termLength < (1024 * 1024 * 1024))
            {
                return BitUtil.Align((termLength * PARTITION_COUNT) + LOG_META_DATA_LENGTH, filePageSize);
            }

            return (PARTITION_COUNT * (long)termLength) + BitUtil.Align(LOG_META_DATA_LENGTH, filePageSize);
        }


        /// <summary>
        /// Store the default frame header to the log meta data buffer.
        /// </summary>
        /// <param name="logMetaDataBuffer"> into which the default headers should be stored. </param>
        /// <param name="defaultHeader">     to be stored. </param>
        /// <exception cref="ArgumentException"> if the defaultHeader is larger than <seealso cref="LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH"/> </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void StoreDefaultFrameHeader(UnsafeBuffer logMetaDataBuffer, IDirectBuffer defaultHeader)
        {
            if (defaultHeader.Capacity != DataHeaderFlyweight.HEADER_LENGTH)
            {
                ThrowHelper.ThrowArgumentException(
                    $"Default header length not equal to HEADER_LENGTH: length={defaultHeader.Capacity:D}");
                return;
            }

            logMetaDataBuffer.PutInt(LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
            logMetaDataBuffer.PutBytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, defaultHeader, 0,
                DataHeaderFlyweight.HEADER_LENGTH);
        }

        /// <summary>
        /// Get a wrapper around the default frame header from the log meta data.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the raw bytes for the default frame header. </param>
        /// <returns> a buffer wrapping the raw bytes. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static UnsafeBuffer DefaultFrameHeader(UnsafeBuffer logMetaDataBuffer)
        {
            return new UnsafeBuffer(logMetaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET,
                DataHeaderFlyweight.HEADER_LENGTH);
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
            termBuffer.PutBytes(termOffset, logMetaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET,
                DataHeaderFlyweight.HEADER_LENGTH);
        }

        /// <summary>
        /// Rotate the log and update the tail counter for the new term.
        ///    
        /// This method is safe for concurrent use.
        /// </summary>
        /// <param name="logMetaDataBuffer"> for the meta data. </param>
        /// <param name="currentTermCount">  from which to rotate. </param>
        /// <param name="currentTermId">     to be used in the default headers. </param>
        /// <returns> true if log was rotated. </returns>
        public static bool RotateLog(UnsafeBuffer logMetaDataBuffer, int currentTermCount, int currentTermId)
        {
            int nextTermId = currentTermId + 1;
            int nextTermCount = currentTermCount + 1;
            int nextIndex = IndexByTermCount(nextTermCount);
            int expectedTermId = nextTermId - PARTITION_COUNT;

            long rawTail;
            do
            {
                rawTail = RawTail(logMetaDataBuffer, nextIndex);
                if (expectedTermId != TermId(rawTail))
                {
                    break;
                }
            } while (!CasRawTail(logMetaDataBuffer, nextIndex, rawTail, PackTail(nextTermId, 0)));

            return CasActiveTermCount(logMetaDataBuffer, currentTermCount, nextTermCount);
        }

        /// <summary>
        /// Set the initial value for the termId in the upper bits of the tail counter.
        /// </summary>
        /// <param name="logMetaData"> contain the tail counter. </param>
        /// <param name="partitionIndex"> to be intialized. </param>
        /// <param name="termId"> to be set.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialiseTailWithTermId(UnsafeBuffer logMetaData, int partitionIndex, int termId)
        {
            logMetaData.PutLong(TERM_TAIL_COUNTERS_OFFSET + partitionIndex * BitUtil.SIZE_OF_LONG, PackTail(termId, 0));
        }

        /// <summary>
        /// Get the termId from a packed raw tail value.
        /// </summary>
        /// <param name="rawTail"> containing the termId </param>
        /// <returns> the termId from a packed raw tail value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermId(long rawTail)
        {
            return (int) (rawTail >> 32);
        }

        /// <summary>
        /// Read the termOffset from a packed raw tail value.
        /// </summary>
        /// <param name="rawTail">    containing the termOffset. </param>
        /// <param name="termLength"> that the offset cannot exceed. </param>
        /// <returns> the termOffset value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermOffset(long rawTail, long termLength)
        {
            var tail = rawTail & 0xFFFFFFFFL;

            return (int) Math.Min(tail, termLength);
        }

        /// <summary>
        /// The termOffset as a result of the append
        /// </summary>
        /// <param name="result"> into which the termOffset value has been packed.</param>
        /// <returns> the termOffset after the append</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermOffset(long result)
        {
            return (int) result;
        }

        /// <summary>
        /// Pack a termId and termOffset into a raw tail value.
        /// </summary>
        /// <param name="termId">     to be packed. </param>
        /// <param name="termOffset"> to be packed. </param>
        /// <returns> the packed value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long PackTail(int termId, int termOffset)
        {
            return ((long) termId << 32) | (termOffset & 0xFFFFFFFFL);
        }

        /// <summary>
        /// Set the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <param name="rawTail">           to be stored </param>
        public static void RawTail(UnsafeBuffer logMetaDataBuffer, int partitionIndex, long rawTail)
        {
            logMetaDataBuffer.PutLong(TERM_TAIL_COUNTERS_OFFSET + (BitUtil.SIZE_OF_LONG * partitionIndex), rawTail);
        }

        /// <summary>
        /// Get the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <returns> the raw value of the tail for the current active partition. </returns>
        public static long RawTail(UnsafeBuffer logMetaDataBuffer, int partitionIndex)
        {
            return logMetaDataBuffer.GetLong(TERM_TAIL_COUNTERS_OFFSET + (BitUtil.SIZE_OF_LONG * partitionIndex));
        }


        /// <summary>
        /// Set the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <param name="rawTail">           to be stored </param>
        public static void RawTailVolatile(UnsafeBuffer logMetaDataBuffer, int partitionIndex, long rawTail)
        {
            logMetaDataBuffer.PutLongVolatile(TERM_TAIL_COUNTERS_OFFSET + (BitUtil.SIZE_OF_LONG * partitionIndex),
                rawTail);
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
            return logMetaDataBuffer.GetLongVolatile(TERM_TAIL_COUNTERS_OFFSET + BitUtil.SIZE_OF_LONG * partitionIndex);
        }

        /// <summary>
        /// Get the raw value of the tail for the current active partition.
        /// </summary>
        /// <param name="logMetaDataBuffer">logMetaDataBuffer containing the tail counters.</param>
        /// <returns>the raw value of the tail for the current active partition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long RawTailVolatile(UnsafeBuffer logMetaDataBuffer)
        {
            var partitionIndex = IndexByTermCount(ActiveTermCount(logMetaDataBuffer));
            return logMetaDataBuffer.GetLongVolatile(TERM_TAIL_COUNTERS_OFFSET + BitUtil.SIZE_OF_LONG * partitionIndex);
        }
        
        /// <summary>
        /// Compare and set the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="logMetaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <param name="expectedRawTail">   expected current value. </param>
        /// <param name="updateRawTail">     to be applied. </param>
        /// <returns> true if the update was successful otherwise false. </returns>
        /// [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool CasRawTail(UnsafeBuffer logMetaDataBuffer, int partitionIndex, long expectedRawTail, long updateRawTail)
        {
            var index = TERM_TAIL_COUNTERS_OFFSET + (BitUtil.SIZE_OF_LONG * partitionIndex);
            return logMetaDataBuffer.CompareAndSetLong(index, expectedRawTail, updateRawTail);
        }
        
        /// <summary>
        /// Get the number of bits to shift when dividing or multiplying by the term buffer length.
        /// </summary>
        /// <param name="termBufferLength"> to compute the number of bits to shift for. </param>
        /// <returns> the number of bits to shift to divide or multiply by the term buffer length. </returns>
        public static int PositionBitsToShift(int termBufferLength)
        {
            switch (termBufferLength)
            {
                case 64 * 1024:
                    return 16;

                case 128 * 1024:
                    return 17;

                case 256 * 1024:
                    return 18;

                case 512 * 1024:
                    return 19;

                case 1024 * 1024:
                    return 20;

                case 2 * 1024 * 1024:
                    return 21;

                case 4 * 1024 * 1024:
                    return 22;

                case 8 * 1024 * 1024:
                    return 23;

                case 16 * 1024 * 1024:
                    return 24;

                case 32 * 1024 * 1024:
                    return 25;

                case 64 * 1024 * 1024:
                    return 26;

                case 128 * 1024 * 1024:
                    return 27;

                case 256 * 1024 * 1024:
                    return 28;

                case 512 * 1024 * 1024:
                    return 29;

                case 1024 * 1024 * 1024:
                    return 30;
            }

            throw new ArgumentException("Invalid term buffer length: " + termBufferLength);
        }
    }
}