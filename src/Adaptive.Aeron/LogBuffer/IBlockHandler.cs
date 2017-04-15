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

using Adaptive.Agrona;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Function for handling a block of message fragments scanned from the log.
    /// </summary>
    public interface IBlockHandler
    {
        /// <summary>
        /// Callback for handling a block of messages being read from a log.
        /// </summary>
        /// <param name="buffer">    containing the block of message fragments. </param>
        /// <param name="offset">    at which the block begins, including any frame headers. </param>
        /// <param name="length">    of the block in bytes, including any frame headers that is aligned up to
        ///                  <seealso cref="FrameDescriptor.FRAME_ALIGNMENT"/>. </param>
        /// <param name="sessionId"> of the stream containing this block of message fragments. </param>
        /// <param name="termId">    of the stream containing this block of message fragments. </param>
        void OnBlock(IDirectBuffer buffer, int offset, int length, int sessionId, int termId);
    }
}