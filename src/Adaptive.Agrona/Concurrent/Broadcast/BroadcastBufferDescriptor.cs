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

using Adaptive.Agrona.Util;
using System;

namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Layout of the broadcast buffer. The buffer consists of a ring of messages that is a power of 2 in size.
    /// This is followed by a trailer section containing state information about the ring.
    /// </summary>
    public class BroadcastBufferDescriptor
    {
        /// <summary>
        /// Offset within the trailer for where the tail intended value is stored. 
        /// </summary>
        public static readonly int TailIntentCounterOffset;

        /// <summary>
        /// Offset within the trailer for where the tail value is stored. 
        /// </summary>
        public static readonly int TailCounterOffset;

        /// <summary>
        /// Offset within the trailer for where the latest sequence value is stored. 
        /// </summary>
        public static readonly int LatestCounterOffset;

        /// <summary> 
        /// Total size of the trailer 
        /// </summary>
        public static readonly int TrailerLength;

        static BroadcastBufferDescriptor()
        {
            var offset = 0;
            TailIntentCounterOffset = offset;

            offset += BitUtil.SIZE_OF_LONG;
            TailCounterOffset = offset;

            offset += BitUtil.SIZE_OF_LONG;
            LatestCounterOffset = offset;

            TrailerLength = BitUtil.CACHE_LINE_LENGTH*2;
        }

        /// <summary>
        /// Check the the buffer capacity is the correct size.
        /// </summary>
        /// <param name="capacity"> to be checked. </param>
        /// <exception cref="InvalidOperationException"> if the buffer capacity is not a power of 2. </exception>
        public static void CheckCapacity(int capacity)
        {
            if (!BitUtil.IsPowerOfTwo(capacity))
            {
                var msg = "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=" + capacity;
                ThrowHelper.ThrowInvalidOperationException(msg);
            }
        }
    }
}