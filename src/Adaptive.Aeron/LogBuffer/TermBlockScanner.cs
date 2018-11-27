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
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Scan a term buffer for a block of message fragments including padding. The block must include complete fragments.
    /// </summary>
    public class TermBlockScanner
    {
        /// <summary>
        /// Scan a term buffer for a block of message fragments from and offset up to a limitOffset.
        ///
        /// A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
        /// for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
        /// at the offset the padding frame begins. Padding frames are delivered singularly in a block.
        ///
        /// Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
        /// relevant length of the frame is <see cref="DataHeaderFlyweight.HEADER_LENGTH"/>
        /// </summary>
        /// <param name="termBuffer"> to scan for message fragments. </param>
        /// <param name="termOffset">     at which the scan should begin. </param>
        /// <param name="limitOffset">      at which the scan should stop. </param>
        /// <returns> the offset at which the scan terminated. </returns>
        public static int Scan(IAtomicBuffer termBuffer, int termOffset, int limitOffset)
        {
            var offset = termOffset;
            while (offset < limitOffset)
            {
                int frameLength = FrameDescriptor.FrameLengthVolatile(termBuffer, offset);
                if (frameLength <= 0)
                {
                    break;
                }

                int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                if (FrameDescriptor.IsPaddingFrame(termBuffer, offset))
                {
                    if (termOffset == offset)
                    {
                        offset += alignedFrameLength;
                    }

                    break;
                }

                if (offset + alignedFrameLength > limitOffset)
                {
                    break;
                }

                offset += alignedFrameLength;
            }


            return offset;
        }
    }

}