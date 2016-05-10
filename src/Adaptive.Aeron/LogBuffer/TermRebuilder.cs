/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Rebuild a term buffer based on incoming frames that can be out-of-order.
    /// </summary>
    public class TermRebuilder
    {
        /// <summary>
        /// Insert a packet of frames into the log at the appropriate offset as indicated by the term offset header.
        /// </summary>
        /// <param name="termBuffer"> into which the packet should be inserted. </param>
        /// <param name="offset">     offset in the term at which the packet should be inserted. </param>
        /// <param name="packet">     containing a sequence of frames. </param>
        /// <param name="length">     of the sequence of frames in bytes. </param>
        public static void Insert(UnsafeBuffer termBuffer, int offset, UnsafeBuffer packet, int length)
        {
            var firstFrameLength = packet.GetInt(0); // LITTLE_ENDIAN
            packet.PutIntOrdered(0, 0);

            termBuffer.PutBytes(offset, packet, 0, length);
            FrameDescriptor.FrameLengthOrdered(termBuffer, offset, firstFrameLength);
        }
    }
}