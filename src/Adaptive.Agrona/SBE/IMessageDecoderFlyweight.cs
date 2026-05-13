/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// A flyweight for decoding an SBE message from a buffer.
    /// </summary>
    public interface IMessageDecoderFlyweight : IMessageFlyweight, IDecoderFlyweight
    {
        /// <summary>
        /// Wrap a buffer containing an encoded message for decoding.
        /// </summary>
        /// <param name="buffer">            containing the encoded message. </param>
        /// <param name="offset">            in the buffer at which the decoding should begin. </param>
        /// <param name="actingBlockLength"> the root block length the decoder should act on. </param>
        /// <param name="actingVersion">     the version of the encoded message. </param>
        /// <returns> the <seealso cref="IMessageDecoderFlyweight"/> for fluent API design. </returns>
        IMessageDecoderFlyweight Wrap(IDirectBuffer buffer, int offset, int actingBlockLength, int actingVersion);
    }
}
