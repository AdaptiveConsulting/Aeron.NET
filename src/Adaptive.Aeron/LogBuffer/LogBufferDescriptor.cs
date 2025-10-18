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
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    ///     Layout description for log buffers which contains partitions of terms with associated term metadata,
    ///     plus ending with overall log metadata.
    ///     <pre>
    ///         +----------------------------+
    ///         |           Term 0           |
    ///         +----------------------------+
    ///         |           Term 1           |
    ///         +----------------------------+
    ///         |           Term 2           |
    ///         +----------------------------+
    ///         |        Log metadata       |
    ///         +----------------------------+
    ///     </pre>
    /// </summary>
    public class LogBufferDescriptor
    {
        private const int PADDING_SIZE = 64;

        /// <summary>
        ///     The number of partitions the log is divided into terms and a metadata buffer.
        /// </summary>
        public const int PARTITION_COUNT = 3;

        /// <summary>
        ///     Minimum buffer length for a log term.
        /// </summary>
        public const int TERM_MIN_LENGTH = 64 * 1024;

        /// <summary>
        ///     Maximum buffer length for a log term.
        /// </summary>
        public const int TERM_MAX_LENGTH = 1024 * 1024 * 1024;

        /// <summary>
        ///     Minimum page size.
        /// </summary>
        public const int PAGE_MIN_SIZE = 4 * 1024;

        /// <summary>
        ///     Maximum page size.
        /// </summary>
        public const int PAGE_MAX_SIZE = 1024 * 1024 * 1024;

        /// <summary>
        ///     Section index for which buffer contains the log metadata.
        /// </summary>
        public static readonly int LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

        // *******************************
        // *** Log metadata Constants ***
        // *******************************

        /// <summary>
        ///     Offset within the metadata where the tail values are stored.
        /// </summary>
        public static readonly int TERM_TAIL_COUNTERS_OFFSET;

        /// <summary>
        ///     Offset within the log metadata where the active partition index is stored.
        /// </summary>
        public static readonly int LOG_ACTIVE_TERM_COUNT_OFFSET;

        /// <summary>
        ///     Offset within the log metadata where the position of the End of Stream is stored.
        /// </summary>
        public static readonly int LOG_END_OF_STREAM_POSITION_OFFSET;

        /// <summary>
        ///     Offset within the log metadata where whether the log is connected or not is stored.
        /// </summary>
        public static readonly int LOG_IS_CONNECTED_OFFSET;

        /// <summary>
        ///     Offset within the log metadata where the count of active transports is stored.
        /// </summary>
        public static readonly int LOG_ACTIVE_TRANSPORT_COUNT;

        /// <summary>
        ///     Offset within the log metadata where the active term id is stored.
        /// </summary>
        public static readonly int LOG_INITIAL_TERM_ID_OFFSET;

        /// <summary>
        ///     Offset within the log metadata which the length field for the frame header is stored.
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET;

        /// <summary>
        ///     Offset within the log metadata which the MTU length is stored.
        /// </summary>
        public static readonly int LOG_MTU_LENGTH_OFFSET;

        /// <summary>
        ///     Offset within the log metadata which the correlation id is stored.
        /// </summary>
        public static readonly int LOG_CORRELATION_ID_OFFSET;

        /// <summary>
        ///     Offset within the log metadata which the term length is stored.
        /// </summary>
        public static readonly int LOG_TERM_LENGTH_OFFSET;

        /// <summary>
        ///     Offset within the log metadata which the page size is stored.
        /// </summary>
        public static readonly int LOG_PAGE_SIZE_OFFSET;

        /// <summary>
        ///     Offset at which the default frame headers begin.
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_OFFSET;

        /// <summary>
        ///     Maximum length of a frame header
        /// </summary>
        public static readonly int LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = PADDING_SIZE * 2;

        /**
         * Offset within the log metadata where the sparse property is stored.
         */
        public static readonly int LOG_SPARSE_OFFSET;

        /**
         * Offset within the log metadata where the tether property is stored.
         */
        public static readonly int LOG_TETHER_OFFSET;

        /// <summary>
        /// Offset within the log metadata where the 'publication revoked' status is indicated.
        /// </summary>
        public static readonly int LOG_IS_PUBLICATION_REVOKED_OFFSET;

        /**
         * Offset within the log metadata where the rejoin property is stored.
         */
        public static readonly int LOG_REJOIN_OFFSET;

        /**
         * Offset within the log metadata where the reliable property is stored.
         */
        public static readonly int LOG_RELIABLE_OFFSET;

        /**
         * Offset within the log metadata where the socket receive buffer length is stored.
         */
        public static readonly int LOG_SOCKET_RCVBUF_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the OS default length for the socket receive buffer is stored.
         */
        public static readonly int LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the OS maximum length for the socket receive buffer is stored.
         */
        public static readonly int LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the socket send buffer length is stored.
         */
        public static readonly int LOG_SOCKET_SNDBUF_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the OS default length for the socket send buffer is stored.
         */
        public static readonly int LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the OS maximum length for the socket send buffer is stored.
         */
        public static readonly int LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the receiver window length is stored.
         */
        public static readonly int LOG_RECEIVER_WINDOW_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the publication window length is stored.
         */
        public static readonly int LOG_PUBLICATION_WINDOW_LENGTH_OFFSET;

        /**
         * Offset within the log metadata where the untethered window limit timeout ns is stored.
         */
        public static readonly int LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET;

        /// <summary>
        /// Offset within the log metadata where the untethered linger timeout ns is stored.
        /// </summary>
        public static readonly int LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET;

        /**
         * Offset within the log metadata where the untethered resting timeout ns is stored.
         */
        public static readonly int LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET;

        /**
         * Offset within the log metadata where the max resend is stored.
         */
        public static readonly int LOG_MAX_RESEND_OFFSET;

        /**
         * Offset within the log metadata where the linger timeout ns is stored.
         */
        public static readonly int LOG_LINGER_TIMEOUT_NS_OFFSET;

        /**
         * Offset within the log metadata where the signal-eos is stored.
         */
        public static readonly int LOG_SIGNAL_EOS_OFFSET;

        /**
         * Offset within the log metadata where the spies-simulate-connection is stored.
         */
        public static readonly int LOG_SPIES_SIMULATE_CONNECTION_OFFSET;

        /**
         * Offset within the log metadata where the group is stored.
         */
        public static readonly int LOG_GROUP_OFFSET;

        /**
         * Offset within the log metadata where the entity tag is stored.
         */
        public static readonly int LOG_ENTITY_TAG_OFFSET;

        /**
         * Offset within the log metadata where the response correlation id is stored.
         */
        public static readonly int LOG_RESPONSE_CORRELATION_ID_OFFSET;

        /**
         * Offset within the log metadata where is-response is stored.
         */
        public static readonly int LOG_IS_RESPONSE_OFFSET;

        /// <summary>
        /// Total length of the log metadata buffer in bytes.
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
        ///  |                    Active Transport Count                     |
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
        ///  |                    Publication Window Length                  |
        ///  +---------------------------------------------------------------+
        ///  |                      Receiver Window Length                   |
        ///  +---------------------------------------------------------------+
        ///  |                    Socket Send Buffer Length                  |
        ///  +---------------------------------------------------------------+
        ///  |               OS Default Socket Send Buffer Length            |
        ///  +---------------------------------------------------------------+
        ///  |                OS Max Socket Send Buffer Length               |
        ///  +---------------------------------------------------------------+
        ///  |                  Socket Receive Buffer Length                 |
        ///  +---------------------------------------------------------------+
        ///  |              OS Default Socket Receive Buffer Length          |
        ///  +---------------------------------------------------------------+
        ///  |               OS Max Socket Receive Buffer Length             |
        ///  +---------------------------------------------------------------+
        ///  |                        Maximum Resend                         |
        ///  +---------------------------------------------------------------+
        ///  |                           Entity tag                          |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                    Response correlation id                    |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                     Default Frame Header                     ...
        /// ...                                                              |
        ///  +---------------------------------------------------------------+
        ///  |                        Linger Timeout (ns)                    |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |               Untethered Window Limit Timeout (ns)            |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                 Untethered Resting Timeout (ns)               |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        ///  |                            Group                              |
        ///  +---------------------------------------------------------------+
        ///  |                          Is response                          |
        ///  +---------------------------------------------------------------+
        ///  |                            Rejoin                             |
        ///  +---------------------------------------------------------------+
        ///  |                           Reliable                            |
        ///  +---------------------------------------------------------------+
        ///  |                            Sparse                             |
        ///  +---------------------------------------------------------------+
        ///  |                         Signal EOS                            |
        ///  +---------------------------------------------------------------+
        ///  |                 Spies Simulate Connection                     |
        ///  +---------------------------------------------------------------+
        ///  |                          Tether                               |
        ///  +---------------------------------------------------------------+
        ///  |                     Is publication revoked                    |
        ///  +---------------------------------------------------------------+
        ///  |                         Alignment gap                         |
        ///  +---------------------------------------------------------------+
        ///  |                  Untethered Linger Timeout (ns)               |
        ///  |                                                               |
        ///  +---------------------------------------------------------------+
        /// 
        /// </pre>
        /// </summary>
        public static readonly int LOG_META_DATA_LENGTH;

        static LogBufferDescriptor()
        {
            TERM_TAIL_COUNTERS_OFFSET = 0;
            LOG_ACTIVE_TERM_COUNT_OFFSET = TERM_TAIL_COUNTERS_OFFSET + (SIZE_OF_LONG * PARTITION_COUNT);

            LOG_END_OF_STREAM_POSITION_OFFSET = PADDING_SIZE * 2;
            LOG_IS_CONNECTED_OFFSET = LOG_END_OF_STREAM_POSITION_OFFSET + SIZE_OF_LONG;
            LOG_ACTIVE_TRANSPORT_COUNT = LOG_IS_CONNECTED_OFFSET + SIZE_OF_INT;

            LOG_CORRELATION_ID_OFFSET = PADDING_SIZE * 4;
            LOG_INITIAL_TERM_ID_OFFSET = LOG_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
            LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = LOG_INITIAL_TERM_ID_OFFSET + SIZE_OF_INT;
            LOG_MTU_LENGTH_OFFSET = LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_TERM_LENGTH_OFFSET = LOG_MTU_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_PAGE_SIZE_OFFSET = LOG_TERM_LENGTH_OFFSET + SIZE_OF_INT;

            LOG_PUBLICATION_WINDOW_LENGTH_OFFSET = LOG_PAGE_SIZE_OFFSET + SIZE_OF_INT;
            LOG_RECEIVER_WINDOW_LENGTH_OFFSET = LOG_PUBLICATION_WINDOW_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_SOCKET_SNDBUF_LENGTH_OFFSET = LOG_RECEIVER_WINDOW_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET = LOG_SOCKET_SNDBUF_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET = LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_SOCKET_RCVBUF_LENGTH_OFFSET = LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET = LOG_SOCKET_RCVBUF_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET = LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET + SIZE_OF_INT;
            LOG_MAX_RESEND_OFFSET = LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET + SIZE_OF_INT;

            LOG_DEFAULT_FRAME_HEADER_OFFSET = PADDING_SIZE * 5;
            LOG_ENTITY_TAG_OFFSET = LOG_DEFAULT_FRAME_HEADER_OFFSET + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH;
            LOG_RESPONSE_CORRELATION_ID_OFFSET = LOG_ENTITY_TAG_OFFSET + SIZE_OF_LONG;
            LOG_LINGER_TIMEOUT_NS_OFFSET = LOG_RESPONSE_CORRELATION_ID_OFFSET + SIZE_OF_LONG;
            LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET = LOG_LINGER_TIMEOUT_NS_OFFSET + SIZE_OF_LONG;
            LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET =
                LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET + SIZE_OF_LONG;
            LOG_GROUP_OFFSET = LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET + SIZE_OF_LONG;
            LOG_IS_RESPONSE_OFFSET = LOG_GROUP_OFFSET + SIZE_OF_BYTE;
            LOG_REJOIN_OFFSET = LOG_IS_RESPONSE_OFFSET + SIZE_OF_BYTE;
            LOG_RELIABLE_OFFSET = LOG_REJOIN_OFFSET + SIZE_OF_BYTE;
            LOG_SPARSE_OFFSET = LOG_RELIABLE_OFFSET + SIZE_OF_BYTE;
            LOG_SIGNAL_EOS_OFFSET = LOG_SPARSE_OFFSET + SIZE_OF_BYTE;
            LOG_SPIES_SIMULATE_CONNECTION_OFFSET = LOG_SIGNAL_EOS_OFFSET + SIZE_OF_BYTE;
            LOG_TETHER_OFFSET = LOG_SPIES_SIMULATE_CONNECTION_OFFSET + SIZE_OF_BYTE;
            LOG_IS_PUBLICATION_REVOKED_OFFSET = LOG_TETHER_OFFSET + SIZE_OF_BYTE;
            LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET = LOG_IS_PUBLICATION_REVOKED_OFFSET + SIZE_OF_INT;

            LOG_META_DATA_LENGTH = PAGE_MIN_SIZE;
        }

        /// <summary>
        ///     Check that term length is valid and alignment is valid.
        /// </summary>
        /// <param name="termLength"> to be checked. </param>
        /// <exception cref="InvalidOperationException"> if the length is not as expected. </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CheckTermLength(int termLength)
        {
            if (termLength < TERM_MIN_LENGTH)
                ThrowHelper.ThrowInvalidOperationException(
                    $"Term length less than min length of {TERM_MIN_LENGTH:D}, length={termLength:D}");

            if (termLength > TERM_MAX_LENGTH)
                ThrowHelper.ThrowInvalidOperationException(
                    $"Term length more than max length of {TERM_MAX_LENGTH:D}: length = {termLength:D}");

            if (!IsPowerOfTwo(termLength))
                ThrowHelper.ThrowInvalidOperationException("Term length not a power of 2: length=" + termLength);
        }

        /// <summary>
        ///     Check that page size is valid and alignment is valid.
        /// </summary>
        /// <param name="pageSize"> to be checked. </param>
        /// <exception cref="InvalidOperationException"> if the size is not as expected. </exception>
        public static void CheckPageSize(int pageSize)
        {
            if (pageSize < PAGE_MIN_SIZE)
                ThrowHelper.ThrowInvalidOperationException(
                    $"Page size less than min size of {PAGE_MIN_SIZE}: page size={pageSize}");

            if (pageSize > PAGE_MAX_SIZE)
                ThrowHelper.ThrowInvalidOperationException(
                    $"Page size more than max size of {PAGE_MAX_SIZE}: page size={pageSize}");

            if (!IsPowerOfTwo(pageSize))
                ThrowHelper.ThrowInvalidOperationException($"Page size not a power of 2: page size={pageSize}");
        }

        /// <summary>
        ///     Get the value of the initial Term id used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of the initial Term id used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialTermId(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetInt(LOG_INITIAL_TERM_ID_OFFSET);
        }

        /// <summary>
        ///     Set the initial term at which this log begins. Initial should be randomised so that stream does not get
        ///     reused accidentally.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="initialTermId">     value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialTermId(UnsafeBuffer metaDataBuffer, int initialTermId)
        {
            metaDataBuffer.PutInt(LOG_INITIAL_TERM_ID_OFFSET, initialTermId);
        }

        /// <summary>
        ///     Get the value of the MTU length used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of the MTU length used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MtuLength(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetInt(LOG_MTU_LENGTH_OFFSET);
        }

        /// <summary>
        ///     Set the MTU length used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="mtuLength">         value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void MtuLength(UnsafeBuffer metaDataBuffer, int mtuLength)
        {
            metaDataBuffer.PutInt(LOG_MTU_LENGTH_OFFSET, mtuLength);
        }

        /// <summary>
        ///     Get the value of the Term Length used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of the term length used for this log. </returns>
        public static int TermLength(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetInt(LOG_TERM_LENGTH_OFFSET);
        }

        /// <summary>
        ///     Set the term length used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="termLength">        value to be set. </param>
        public static void TermLength(UnsafeBuffer metaDataBuffer, int termLength)
        {
            metaDataBuffer.PutInt(LOG_TERM_LENGTH_OFFSET, termLength);
        }

        /// <summary>
        ///     Get the value of the page size used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of the page size used for this log. </returns>
        public static int PageSize(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetInt(LOG_PAGE_SIZE_OFFSET);
        }

        /// <summary>
        ///     Set the page size used for this log.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="pageSize">          value to be set. </param>
        public static void PageSize(UnsafeBuffer metaDataBuffer, int pageSize)
        {
            metaDataBuffer.PutInt(LOG_PAGE_SIZE_OFFSET, pageSize);
        }

        /// <summary>
        ///     Get the value of the correlation ID for this log relating to the command which created it.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of the correlation ID used for this log. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long CorrelationId(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetLong(LOG_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        ///     Set the correlation ID used for this log relating to the command which created it.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="id">                value to be set. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CorrelationId(UnsafeBuffer metaDataBuffer, long id)
        {
            metaDataBuffer.PutLong(LOG_CORRELATION_ID_OFFSET, id);
        }

        /// <summary>
        ///     Get whether the log is considered connected or not by the driver.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> whether the log is considered connected or not by the driver. </returns>
        public static bool IsConnected(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetIntVolatile(LOG_IS_CONNECTED_OFFSET) == 1;
        }

        /// <summary>
        ///     Set whether the log is considered connected or not by the driver.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="isConnected">       or not. </param>
        public static void IsConnected(UnsafeBuffer metaDataBuffer, bool isConnected)
        {
            metaDataBuffer.PutIntRelease(LOG_IS_CONNECTED_OFFSET, isConnected ? 1 : 0);
        }

        /// <summary>
        ///     Get the count of active transports for the Image.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> count of active transports. </returns>
        public static int ActiveTransportCount(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetIntVolatile(LOG_ACTIVE_TRANSPORT_COUNT);
        }

        /// <summary>
        ///     Set the number of active transports for the Image.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="numberOfActiveTransports"> value to be set. </param>
        public static void ActiveTransportCount(UnsafeBuffer metadataBuffer, int numberOfActiveTransports)
        {
            metadataBuffer.PutIntRelease(LOG_ACTIVE_TRANSPORT_COUNT, numberOfActiveTransports);
        }

        /// <summary>
        ///     Get the value of the end of stream position.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of end of stream position </returns>
        public static long EndOfStreamPosition(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetLongVolatile(LOG_END_OF_STREAM_POSITION_OFFSET);
        }

        /// <summary>
        ///     Set the value of the end of stream position.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="position">          value of the end of stream position. </param>
        public static void EndOfStreamPosition(UnsafeBuffer metaDataBuffer, long position)
        {
            metaDataBuffer.PutLongRelease(LOG_END_OF_STREAM_POSITION_OFFSET, position);
        }

        /// <summary>
        ///     Get the value of the active term count used by the producer of this log. Consumers may have a different
        ///     active term count if they are running behind. The read is done with volatile semantics.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <returns> the value of the active term count used by the producer of this log. </returns>
        public static int ActiveTermCount(UnsafeBuffer metaDataBuffer)
        {
            return metaDataBuffer.GetIntVolatile(LOG_ACTIVE_TERM_COUNT_OFFSET);
        }

        /// <summary>
        ///     Set the value of the current active term count for the producer using memory release semantics.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="termCount">         value of the active term count used by the producer of this log. </param>
        public static void ActiveTermCountOrdered(UnsafeBuffer metaDataBuffer, int termCount)
        {
            metaDataBuffer.PutIntRelease(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
        }

        /// <summary>
        ///     Compare and set the value of the current active term count.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="expectedTermCount"> value of the active term count expected in the log. </param>
        /// <param name="updateTermCount">   value of the active term count to be updated in the log. </param>
        /// <returns> true if successful otherwise false. </returns>
        public static bool CasActiveTermCount(UnsafeBuffer metaDataBuffer, int expectedTermCount, int updateTermCount)
        {
            return metaDataBuffer.CompareAndSetInt(LOG_ACTIVE_TERM_COUNT_OFFSET, expectedTermCount, updateTermCount);
        }

        /// <summary>
        ///     Set the value of the current active partition index for the producer.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the metadata. </param>
        /// <param name="termCount">         value of the active term count used by the producer of this log. </param>
        public static void ActiveTermCount(UnsafeBuffer metaDataBuffer, int termCount)
        {
            metaDataBuffer.PutInt(LOG_ACTIVE_TERM_COUNT_OFFSET, termCount);
        }

        /// <summary>
        ///     Rotate to the next partition in sequence for the term id.
        /// </summary>
        /// <param name="currentIndex"> partition index. </param>
        /// <returns> the next partition index. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NextPartitionIndex(int currentIndex)
        {
            return (currentIndex + 1) % PARTITION_COUNT;
        }

        /// <summary>
        ///     Determine the partition index to be used given the initial term and active term ids.
        /// </summary>
        /// <param name="initialTermId"> at which the log buffer usage began. </param>
        /// <param name="activeTermId">  that is in current usage. </param>
        /// <returns> the index of which buffer should be used. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByTerm(int initialTermId, int activeTermId)
        {
            return (activeTermId - initialTermId) % PARTITION_COUNT;
        }

        /// <summary>
        ///     Determine the partition index based on number of terms that have passed.
        /// </summary>
        /// <param name="termCount"> for the number of terms that have passed. </param>
        /// <returns> the partition index for the term count. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByTermCount(long termCount)
        {
            return (int)(termCount % PARTITION_COUNT);
        }

        /// <summary>
        ///     Determine the partition index given a stream position.
        /// </summary>
        /// <param name="position"> in the stream in bytes. </param>
        /// <param name="positionBitsToShift"> number of times to left shift to multiply by term length. </param>
        /// <returns> the partition index for the position. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int IndexByPosition(long position, int positionBitsToShift)
        {
            return (int)((long)((ulong)position >> positionBitsToShift) % PARTITION_COUNT);
        }

        /// <summary>
        ///     Compute the current position in absolute number of bytes.
        /// </summary>
        /// <param name="activeTermId">        active term id. </param>
        /// <param name="termOffset">          in the term. </param>
        /// <param name="positionBitsToShift"> number of times to left shift the term count to multiply by term length. </param>
        /// <param name="initialTermId">       the initial term id that this stream started on. </param>
        /// <returns> the absolute position in bytes. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ComputePosition(int activeTermId, int termOffset, int positionBitsToShift, int initialTermId)
        {
            long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

            return (termCount << positionBitsToShift) + termOffset;
        }

        /// <summary>
        ///     Compute the current position in absolute number of bytes for the beginning of a term.
        /// </summary>
        /// <param name="activeTermId">        active term id. </param>
        /// <param name="positionBitsToShift"> number of times to left shift the term count to multiply by term length.  </param>
        /// <param name="initialTermId">       the initial term id that this stream started on. </param>
        /// <returns> the absolute position in bytes. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ComputeTermBeginPosition(int activeTermId, int positionBitsToShift, int initialTermId)
        {
            long termCount = activeTermId - initialTermId; // copes with negative activeTermId on rollover

            return termCount << positionBitsToShift;
        }

        /// <summary>
        ///     Compute the term id from a position.
        /// </summary>
        /// <param name="position">            to calculate from </param>
        /// <param name="positionBitsToShift"> number of times to left shift the position to multiply by term length. </param>
        /// <param name="initialTermId">       the initial term id that this stream started on. </param>
        /// <returns> the term id according to the position. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeTermIdFromPosition(long position, int positionBitsToShift, int initialTermId)
        {
            return (int)(long)((ulong)position >> positionBitsToShift) + initialTermId;
        }

        /// <summary>
        ///     Compute the total length of a log file given the term length.
        ///     Assumes <see cref="TERM_MAX_LENGTH" /> is 1 GB and that filePageSize is 1 GB or less and a power of 2.
        /// </summary>
        /// <param name="termLength"> on which to base the calculation. </param>
        /// <param name="filePageSize"> to use for log. </param>
        /// <returns> the total length of the log file. </returns>
        public static long ComputeLogLength(int termLength, int filePageSize)
        {
            return Align((PARTITION_COUNT * (long)termLength) + LOG_META_DATA_LENGTH, filePageSize);
        }


        /// <summary>
        ///     Store the default frame header to the log metadata buffer.
        /// </summary>
        /// <param name="metaDataBuffer"> into which the default headers should be stored. </param>
        /// <param name="defaultHeader">     to be stored. </param>
        /// <exception cref="ArgumentException">
        ///     if the defaultHeader is larger than
        ///     <seealso cref="LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH" />.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void StoreDefaultFrameHeader(UnsafeBuffer metaDataBuffer, IDirectBuffer defaultHeader)
        {
            if (defaultHeader.Capacity != HEADER_LENGTH)
            {
                ThrowHelper.ThrowArgumentException(
                    $"Default header length not equal to HEADER_LENGTH: length={defaultHeader.Capacity:D}");
                return;
            }

            metaDataBuffer.PutInt(LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET, HEADER_LENGTH);
            metaDataBuffer.PutBytes(LOG_DEFAULT_FRAME_HEADER_OFFSET, defaultHeader, 0,
                HEADER_LENGTH);
        }

        /// <summary>
        ///     Get a wrapper around the default frame header from the log metadata.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the raw bytes for the default frame header. </param>
        /// <returns> a buffer wrapping the raw bytes. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static UnsafeBuffer DefaultFrameHeader(UnsafeBuffer metaDataBuffer)
        {
            return new UnsafeBuffer(metaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET,
                HEADER_LENGTH);
        }

        /// <summary>
        ///     Apply the default header for a message in a term.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the default headers. </param>
        /// <param name="termBuffer">        to which the default header should be applied. </param>
        /// <param name="termOffset">        at which the default should be applied. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ApplyDefaultHeader(UnsafeBuffer metaDataBuffer, UnsafeBuffer termBuffer, int termOffset)
        {
            termBuffer.PutBytes(termOffset, metaDataBuffer, LOG_DEFAULT_FRAME_HEADER_OFFSET,
                HEADER_LENGTH);
        }

        /// <summary>
        ///     Rotate the log and update the tail counter for the new term.
        ///     This method is safe for concurrent use.
        /// </summary>
        /// <param name="metaDataBuffer"> for the metadata. </param>
        /// <param name="currentTermCount">  from which to rotate. </param>
        /// <param name="currentTermId">     to be used in the default headers. </param>
        /// <returns> true if log was rotated. </returns>
        public static bool RotateLog(UnsafeBuffer metaDataBuffer, int currentTermCount, int currentTermId)
        {
            var nextTermId = currentTermId + 1;
            var nextTermCount = currentTermCount + 1;
            var nextIndex = IndexByTermCount(nextTermCount);
            var expectedTermId = nextTermId - PARTITION_COUNT;

            long rawTail;
            do
            {
                rawTail = RawTailVolatile(metaDataBuffer, nextIndex);
                if (expectedTermId != TermId(rawTail)) break;
            } while (!CasRawTail(metaDataBuffer, nextIndex, rawTail, PackTail(nextTermId, 0)));

            return CasActiveTermCount(metaDataBuffer, currentTermCount, nextTermCount);
        }

        /// <summary>
        ///     Set the initial value for the termId in the upper bits of the tail counter.
        /// </summary>
        /// <param name="logMetaData"> contain the tail counter. </param>
        /// <param name="partitionIndex"> to be intialized. </param>
        /// <param name="termId"> to be set.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialiseTailWithTermId(UnsafeBuffer logMetaData, int partitionIndex, int termId)
        {
            logMetaData.PutLong(TERM_TAIL_COUNTERS_OFFSET + partitionIndex * SIZE_OF_LONG, PackTail(termId, 0));
        }

        /// <summary>
        ///     Get the termId from a packed raw tail value.
        /// </summary>
        /// <param name="rawTail"> containing the termId. </param>
        /// <returns> the termId from a packed raw tail value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermId(long rawTail)
        {
            return (int)(rawTail >> 32);
        }

        /// <summary>
        ///     Read the termOffset from a packed raw tail value.
        /// </summary>
        /// <param name="rawTail">    containing the termOffset. </param>
        /// <param name="termLength"> that the offset cannot exceed. </param>
        /// <returns> the termOffset value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermOffset(long rawTail, long termLength)
        {
            var tail = rawTail & 0xFFFFFFFFL;

            return (int)Math.Min(tail, termLength);
        }

        /// <summary>
        ///     The termOffset as a result of the append operation.
        /// </summary>
        /// <param name="result"> into which the termOffset value has been packed.</param>
        /// <returns> the termOffset after the append operation. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermOffset(long result)
        {
            return (int)result;
        }

        /// <summary>
        ///     Pack a termId and termOffset into a raw tail value.
        /// </summary>
        /// <param name="termId">     to be packed. </param>
        /// <param name="termOffset"> to be packed. </param>
        /// <returns> the packed value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long PackTail(int termId, int termOffset)
        {
#pragma warning disable CS0675
            return ((long)termId << 32) | termOffset;
#pragma warning restore CS0675
        }

        /// <summary>
        ///     Set the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <param name="rawTail">           to be stored. </param>
        public static void RawTail(UnsafeBuffer metaDataBuffer, int partitionIndex, long rawTail)
        {
            metaDataBuffer.PutLong(TERM_TAIL_COUNTERS_OFFSET + SIZE_OF_LONG * partitionIndex, rawTail);
        }

        /// <summary>
        ///     Get the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <returns> the raw value of the tail for the current active partition. </returns>
        public static long RawTail(UnsafeBuffer metaDataBuffer, int partitionIndex)
        {
            return metaDataBuffer.GetLong(TERM_TAIL_COUNTERS_OFFSET + SIZE_OF_LONG * partitionIndex);
        }


        /// <summary>
        ///     Set the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <param name="rawTail">           to be stored. </param>
        public static void RawTailVolatile(UnsafeBuffer metaDataBuffer, int partitionIndex, long rawTail)
        {
            metaDataBuffer.PutLongVolatile(TERM_TAIL_COUNTERS_OFFSET + SIZE_OF_LONG * partitionIndex,
                rawTail);
        }

        /// <summary>
        ///     Get the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="metaDataBuffer">containing the tail counters.</param>
        /// <param name="partitionIndex">for the tail counter.</param>
        /// <returns>the raw value of the tail for the current active partition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long RawTailVolatile(UnsafeBuffer metaDataBuffer, int partitionIndex)
        {
            return metaDataBuffer.GetLongVolatile(TERM_TAIL_COUNTERS_OFFSET + SIZE_OF_LONG * partitionIndex);
        }

        /// <summary>
        ///     Get the raw value of the tail for the current active partition.
        /// </summary>
        /// <param name="metaDataBuffer">metaDataBuffer containing the tail counters.</param>
        /// <returns>the raw value of the tail for the current active partition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long RawTailVolatile(UnsafeBuffer metaDataBuffer)
        {
            var partitionIndex = IndexByTermCount(ActiveTermCount(metaDataBuffer));
            return metaDataBuffer.GetLongVolatile(TERM_TAIL_COUNTERS_OFFSET + SIZE_OF_LONG * partitionIndex);
        }

        /// <summary>
        ///     Compare and set the raw value of the tail for the given partition.
        /// </summary>
        /// <param name="metaDataBuffer"> containing the tail counters. </param>
        /// <param name="partitionIndex">    for the tail counter. </param>
        /// <param name="expectedRawTail">   expected current value. </param>
        /// <param name="updateRawTail">     to be applied. </param>
        /// <returns> true if the update was successful otherwise false. </returns>
        public static bool CasRawTail(UnsafeBuffer metaDataBuffer, int partitionIndex, long expectedRawTail,
            long updateRawTail)
        {
            var index = TERM_TAIL_COUNTERS_OFFSET + SIZE_OF_LONG * partitionIndex;
            return metaDataBuffer.CompareAndSetLong(index, expectedRawTail, updateRawTail);
        }

        /// <summary>
        ///     Get the number of bits to shift when dividing or multiplying by the term buffer length.
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

            throw new ArgumentException("invalid term buffer length: " + termBufferLength);
        }

        /// <summary>
        /// Compute frame length for a message that is fragmented into chunks of {@code maxPayloadSize}.
        /// </summary>
        /// <param name="length">         of the message. </param>
        /// <param name="maxPayloadSize"> fragment size without the header. </param>
        /// <returns> message length after fragmentation. </returns>
        public static int ComputeFragmentedFrameLength(int length, int maxPayloadSize)
        {
            int numMaxPayloads = length / maxPayloadSize;
            int remainingPayload = length % maxPayloadSize;
            int lastFrameLength = remainingPayload > 0 ? Align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;

            return (numMaxPayloads * (maxPayloadSize + HEADER_LENGTH)) + lastFrameLength;
        }


        /// <summary>
        /// Compute frame length for a message that has been reassembled from chunks of {@code maxPayloadSize}.
        /// </summary>
        /// <param name="length">         of the message. </param>
        /// <param name="maxPayloadSize"> fragment size without the header. </param>
        /// <returns> message length after fragmentation. </returns>
        public static int ComputeAssembledFrameLength(int length, int maxPayloadSize)
        {
            int numMaxPayloads = length / maxPayloadSize;
            int remainingPayload = length % maxPayloadSize;

            return HEADER_LENGTH + (numMaxPayloads * maxPayloadSize) + remainingPayload;
        }

        /// <summary>
        /// Get whether the log is sparse from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log is sparse, otherwise false. </returns>
        public static bool Sparse(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_SPARSE_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is sparse in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log is sparse, otherwise false. </param>
        public static void Sparse(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_SPARSE_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get whether the log is tethered from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log is tethered, otherwise false. </returns>
        public static bool Tether(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_TETHER_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is tethered in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log is tethered, otherwise false. </param>
        public static void Tether(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_TETHER_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get whether the log's publication was revoked.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log's publication was revoked, otherwise false. </returns>
        public static bool IsPublicationRevoked(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_IS_PUBLICATION_REVOKED_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log's publication was revoked.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log's publication was revoked, otherwise false. </param>
        public static void IsPublicationRevoked(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_IS_PUBLICATION_REVOKED_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get whether the log is group from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log is group, otherwise false. </returns>
        public static bool Group(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_GROUP_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is group in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log is group, otherwise false. </param>
        public static void Group(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_GROUP_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get whether the log is response from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log is group, otherwise false. </returns>
        public static bool IsResponse(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_IS_RESPONSE_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is response in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log is group, otherwise false. </param>
        public static void IsResponse(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_IS_RESPONSE_OFFSET, (byte)(value ? 1 : 0));
        }


        /// <summary>
        /// Get whether the log is rejoining from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log is rejoining, otherwise false. </returns>
        public static bool Rejoin(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_REJOIN_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is rejoining in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log is rejoining, otherwise false. </param>
        public static void Rejoin(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_REJOIN_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get whether the log is reliable from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if the log is reliable, otherwise false. </returns>
        public static bool Reliable(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_RELIABLE_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the log is reliable in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if the log is reliable, otherwise false. </param>
        public static void Reliable(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_RELIABLE_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get the socket receive buffer length from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the socket receive buffer length. </returns>
        public static int SocketRcvbufLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_SOCKET_RCVBUF_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the socket receive buffer length in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the socket receive buffer length to set. </param>
        public static void SocketRcvbufLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_SOCKET_RCVBUF_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the default length in bytes for the socket receive buffer as per OS configuration from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the default length in bytes for the socket receive buffer. </returns>
        public static int OsDefaultSocketRcvbufLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the default length for the socket receive buffer as per OS configuration in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the default length in bytes for the socket receive buffer. </param>
        public static void OsDefaultSocketRcvbufLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_OS_DEFAULT_SOCKET_RCVBUF_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the maximum length in bytes for the socket receive buffer as per OS configuration from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the maximum allowed length in bytes for the socket receive buffer. </returns>
        public static int OsMaxSocketRcvbufLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the maximum allowed length in bytes for the socket receive buffer as per OS configuration in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the maximum allowed length in bytes for the socket receive buffer. </param>
        public static void OsMaxSocketRcvbufLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_OS_MAX_SOCKET_RCVBUF_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the socket send buffer length from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the socket send buffer length. </returns>
        public static int SocketSndbufLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_SOCKET_SNDBUF_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the socket send buffer length in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the socket send buffer length to set. </param>
        public static void SocketSndbufLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_SOCKET_SNDBUF_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the default length in bytes for the socket send buffer as per OS configuration from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the default length in bytes for the socket send buffer. </returns>
        public static int OsDefaultSocketSndbufLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the default length for the socket send buffer as per OS configuration in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the default length in bytes for the socket send buffer. </param>
        public static void OsDefaultSocketSndbufLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_OS_DEFAULT_SOCKET_SNDBUF_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the maximum length in bytes for the socket send buffer as per OS configuration from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the maximum allowed length in bytes for the socket send buffer. </returns>
        public static int OsMaxSocketSndbufLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the maximum allowed length in bytes for the socket send buffer as per OS configuration in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the maximum allowed length in bytes for the socket send buffer. </param>
        public static void OsMaxSocketSndbufLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_OS_MAX_SOCKET_SNDBUF_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the receiver window length from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the receiver window length. </returns>
        public static int ReceiverWindowLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_RECEIVER_WINDOW_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the receiver window length in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the receiver window length to set. </param>
        public static void ReceiverWindowLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_RECEIVER_WINDOW_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the publication window length from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the publication window length. </returns>
        public static int PublicationWindowLength(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_PUBLICATION_WINDOW_LENGTH_OFFSET);
        }

        /// <summary>
        /// Set the publication window length in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the publication window length to set. </param>
        public static void PublicationWindowLength(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_PUBLICATION_WINDOW_LENGTH_OFFSET, value);
        }

        /// <summary>
        /// Get the untethered window limit timeout in nanoseconds from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the untethered window limit timeout in nanoseconds. </returns>
        public static long UntetheredWindowLimitTimeoutNs(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetLong(LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET);
        }

        /// <summary>
        /// Set the untethered window limit timeout in nanoseconds in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the untethered window limit timeout to set. </param>
        public static void UntetheredWindowLimitTimeoutNs(UnsafeBuffer metadataBuffer, long value)
        {
            metadataBuffer.PutLong(LOG_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_OFFSET, value);
        }

        /// <summary>
        /// Get the untethered linger timeout in nanoseconds from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the untethered window limit timeout in nanoseconds. </returns>
        public static long UntetheredLingerTimeoutNs(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetLong(LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET);
        }

        /// <summary>
        /// Set the untethered linger timeout in nanoseconds in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the untethered linger timeout to set. </param>
        public static void UntetheredLingerTimeoutNs(UnsafeBuffer metadataBuffer, long value)
        {
            metadataBuffer.PutLong(LOG_UNTETHERED_LINGER_TIMEOUT_NS_OFFSET, value);
        }

        /// <summary>
        /// Get the untethered resting timeout in nanoseconds from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the untethered resting timeout in nanoseconds. </returns>
        public static long UntetheredRestingTimeoutNs(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetLong(LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET);
        }

        /// <summary>
        /// Set the untethered resting timeout in nanoseconds in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the untethered resting timeout to set. </param>
        public static void UntetheredRestingTimeoutNs(UnsafeBuffer metadataBuffer, long value)
        {
            metadataBuffer.PutLong(LOG_UNTETHERED_RESTING_TIMEOUT_NS_OFFSET, value);
        }

        /// <summary>
        /// Get the maximum resend count from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the maximum resend count. </returns>
        public static int MaxResend(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetInt(LOG_MAX_RESEND_OFFSET);
        }

        /// <summary>
        /// Set the maximum resend count in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the maximum resend count to set. </param>
        public static void MaxResend(UnsafeBuffer metadataBuffer, int value)
        {
            metadataBuffer.PutInt(LOG_MAX_RESEND_OFFSET, value);
        }

        /// <summary>
        /// Get the linger timeout in nanoseconds from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the linger timeout in nanoseconds. </returns>
        public static long LingerTimeoutNs(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetLong(LOG_LINGER_TIMEOUT_NS_OFFSET);
        }

        /// <summary>
        /// Set the linger timeout in nanoseconds in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the linger timeout to set. </param>
        public static void LingerTimeoutNs(UnsafeBuffer metadataBuffer, long value)
        {
            metadataBuffer.PutLong(LOG_LINGER_TIMEOUT_NS_OFFSET, value);
        }

        /// <summary>
        /// Get the entity tag  from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the entity tag in nanoseconds. </returns>
        public static long EntityTag(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetLong(LOG_ENTITY_TAG_OFFSET);
        }

        /// <summary>
        /// Set the entity tag in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the entity tag to set. </param>
        public static void EntityTag(UnsafeBuffer metadataBuffer, long value)
        {
            metadataBuffer.PutLong(LOG_ENTITY_TAG_OFFSET, value);
        }

        /// <summary>
        /// Get the response correlation id  from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> the entity tag in nanoseconds. </returns>
        public static long ResponseCorrelationId(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetLong(LOG_RESPONSE_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the response correlation id in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          the resonse correlation id to set. </param>
        public static void ResponseCorrelationId(UnsafeBuffer metadataBuffer, long value)
        {
            metadataBuffer.PutLong(LOG_RESPONSE_CORRELATION_ID_OFFSET, value);
        }

        /// <summary>
        /// Get whether the signal EOS is enabled from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if signal EOS is enabled, otherwise false. </returns>
        public static bool SignalEos(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_SIGNAL_EOS_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether the signal EOS is enabled in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if signal EOS is enabled, otherwise false. </param>
        public static void SignalEos(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_SIGNAL_EOS_OFFSET, (byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Get whether spies simulate connection from the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <returns> true if spies simulate connection, otherwise false. </returns>
        public static bool SpiesSimulateConnection(UnsafeBuffer metadataBuffer)
        {
            return metadataBuffer.GetByte(LOG_SPIES_SIMULATE_CONNECTION_OFFSET) == 1;
        }

        /// <summary>
        /// Set whether spies simulate connection in the metadata.
        /// </summary>
        /// <param name="metadataBuffer"> containing the meta data. </param>
        /// <param name="value">          true if spies simulate connection, otherwise false. </param>
        public static void SpiesSimulateConnection(UnsafeBuffer metadataBuffer, bool value)
        {
            metadataBuffer.PutByte(LOG_SPIES_SIMULATE_CONNECTION_OFFSET, (byte)(value ? 1 : 0));
        }
    }
}