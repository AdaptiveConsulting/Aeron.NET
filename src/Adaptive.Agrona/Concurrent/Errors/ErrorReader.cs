/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Adaptive.Agrona.Concurrent.Errors
{
    /// <summary>
    /// Reader for the log created by a <seealso cref="DistinctErrorLog"/>.
    /// 
    /// The read methods are thread safe.
    /// </summary>
    public class ErrorLogReader
    {
        /// <summary>
        /// Has the error buffer any recorded errors?
        /// </summary>
        /// <param name="buffer"> containing the <seealso cref="DistinctErrorLog"/>. </param>
        /// <returns> true if there is at least one error. </returns>
        public static bool HasErrors(IAtomicBuffer buffer)
        {
            return buffer.Capacity >= BitUtil.SIZE_OF_INT && 0 != buffer.GetIntVolatile(DistinctErrorLog.LengthOffset);
        }
        
        /// <summary>
        /// Read all the errors in a log since the creation of the log.
        /// </summary>
        /// <param name="buffer">   containing the <seealso cref="DistinctErrorLog"/>. </param>
        /// <param name="consumer"> to be called for each exception encountered. </param>
        /// <returns> the number of entries that has been read. </returns>
        public static int Read(IAtomicBuffer buffer, ErrorConsumer consumer)
        {
            return Read(buffer, consumer, 0);
        }

        /// <summary>
        /// Read all the errors in a log since a given timestamp.
        /// </summary>
        /// <param name="buffer">         containing the <seealso cref="DistinctErrorLog"/>. </param>
        /// <param name="consumer">       to be called for each exception encountered. </param>
        /// <param name="sinceTimestamp"> for filtering errors that have been recorded since this time. </param>
        /// <returns> the number of entries that has been read. </returns>
        public static int Read(IAtomicBuffer buffer, ErrorConsumer consumer, long sinceTimestamp)
        {
            int entries = 0;
            int offset = 0;
            int capacity = buffer.Capacity;

            while (offset < capacity)
            {
                int length = buffer.GetIntVolatile(offset + DistinctErrorLog.LengthOffset);
                if (0 == length)
                {
                    break;
                }

                long lastObservationTimestamp = buffer.GetLongVolatile(offset + DistinctErrorLog.LastObservationTimestampOffset);
                if (lastObservationTimestamp >= sinceTimestamp)
                {
                    ++entries;

                    consumer(
                        buffer.GetInt(offset + DistinctErrorLog.ObservationCountOffset),
                        buffer.GetLong(offset + DistinctErrorLog.FirstObservationTimestampOffset),
                        lastObservationTimestamp,
                        buffer.GetStringWithoutLengthUtf8(offset + DistinctErrorLog.EncodedErrorOffset, length - DistinctErrorLog.EncodedErrorOffset));
                }

                offset += BitUtil.Align(length, DistinctErrorLog.RecordAlignment);
            }

            return entries;
        }
    }
}