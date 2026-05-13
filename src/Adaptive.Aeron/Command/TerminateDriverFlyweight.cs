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

using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Command message flyweight to ask the driver process to terminate.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents"/>
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                          Client ID                            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                        Correlation ID                         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                         Token Length                          |
    ///  +---------------------------------------------------------------+
    ///  |                         Token Buffer                         ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// <see cref="ControlProtocolEvents"/>
    public class TerminateDriverFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int TokenLengthOffset = CorrelationIdFieldOffset + BitUtil.SIZE_OF_LONG;
        internal static readonly int TokenBufferFieldOffset = TokenLengthOffset + BitUtil.SIZE_OF_INT;
        private static readonly int MinimumLength = TokenLengthOffset + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new TerminateDriverFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);
            return this;
        }

        /// <summary>
        /// Relative _offset of the token _buffer.
        /// </summary>
        /// <returns> relative _offset of the token _buffer. </returns>
        public int TokenBufferOffset()
        {
            return TokenBufferFieldOffset;
        }

        /// <summary>
        /// Length of the token _buffer in bytes.
        /// </summary>
        /// <returns> length of token _buffer in bytes. </returns>
        public int TokenBufferLength()
        {
            return _buffer.GetInt(_offset + TokenLengthOffset);
        }

        /// <summary>
        /// Fill the token _buffer.
        /// </summary>
        /// <param name="tokenBuffer"> containing the optional token for the request. </param>
        /// <param name="tokenOffset"> within the tokenBuffer at which the token begins. </param>
        /// <param name="tokenLength"> of the token in the tokenBuffer. </param>
        /// <returns> this for a fluent API. </returns>
        public TerminateDriverFlyweight TokenBuffer(IDirectBuffer tokenBuffer, int tokenOffset, int tokenLength)
        {
            _buffer.PutInt(_offset + TokenLengthOffset, tokenLength);
            if (null != tokenBuffer && tokenLength > 0)
            {
                _buffer.PutBytes(_offset + TokenBufferOffset(), tokenBuffer, tokenOffset, tokenLength);
            }

            return this;
        }

        /// <summary>
        /// Get the length of the current message.
        /// <para>
        /// NB: must be called after the data is written in order to be correct.
        ///
        /// </para>
        /// </summary>
        /// <returns> the length of the current message </returns>
        public int Length()
        {
            return TokenBufferOffset() + TokenBufferLength();
        }

        /// <summary>
        /// Compute the length of the command message for a given token length.
        /// </summary>
        /// <param name="tokenLength"> to be appended to the header. </param>
        /// <returns> the length of the command message for a given token length. </returns>
        public static int ComputeLength(int tokenLength)
        {
            if (tokenLength < 0)
            {
                throw new ConfigurationException("token length must be >= 0: " + tokenLength);
            }

            return LENGTH + BitUtil.SIZE_OF_INT + tokenLength;
        }
    }
}
