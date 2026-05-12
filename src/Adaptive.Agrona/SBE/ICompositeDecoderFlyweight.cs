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
    /// A flyweight for decoding an SBE Composite type.
    /// </summary>
    public interface ICompositeDecoderFlyweight : IDecoderFlyweight
    {
        /// <summary>
        /// Wrap a buffer for decoding at a given offset.
        /// </summary>
        /// <param name="buffer"> containing the encoded SBE Composite type. </param>
        /// <param name="offset"> at which the encoded SBE Composite type begins. </param>
        /// <returns> the <seealso cref="ICompositeDecoderFlyweight"/> for fluent API design. </returns>
        ICompositeDecoderFlyweight Wrap(IDirectBuffer buffer, int offset);
    }
}
