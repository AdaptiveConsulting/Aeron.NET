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

using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Rebuild a term buffer from received frames which can be out-of-order. The resulting data structure will only
    /// monotonically increase in state.
    /// </summary>
    public class TermRebuilder
    {
        /// <summary>
        /// Insert a packet of frames into the log at the appropriate termOffset as indicated by the term termOffset header.
        /// 
        /// If the packet has already been inserted then this is a noop.
        /// </summary>
        /// <param name="termBuffer"> into which the packet should be inserted. </param>
        /// <param name="termOffset"> in the term at which the packet should be inserted. </param>
        /// <param name="packet">     containing a sequence of frames. </param>
        /// <param name="length">     of the packet of frames in bytes. </param>
        public static void Insert(IAtomicBuffer termBuffer, int termOffset, UnsafeBuffer packet, int length)
        {
            if (0 == termBuffer.GetInt(termOffset))
            {
                termBuffer.PutBytes(termOffset + DataHeaderFlyweight.HEADER_LENGTH, packet,
                    DataHeaderFlyweight.HEADER_LENGTH, length - DataHeaderFlyweight.HEADER_LENGTH);

                termBuffer.PutLong(termOffset + 24, packet.GetLong(24));
                termBuffer.PutLong(termOffset + 16, packet.GetLong(16));
                termBuffer.PutLong(termOffset + 8, packet.GetLong(8));

                termBuffer.PutLongOrdered(termOffset, packet.GetLong(0));
            }
        }
    }
}