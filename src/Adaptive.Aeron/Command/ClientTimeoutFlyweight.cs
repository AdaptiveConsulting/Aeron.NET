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

using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Indicate a client has timed out by the driver.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents"/>
    /// <pre>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                         Client Id                             |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// </pre>
    public class ClientTimeoutFlyweight
    {
        /// <summary>
        /// Length of the header
        /// </summary>
        public static readonly int LENGTH = BitUtil.SIZE_OF_LONG;
        private const int ClientIdFieldOffset = 0;

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API </returns>
        public ClientTimeoutFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// Get client id field.
        /// </summary>
        /// <returns> client id field. </returns>
        public long ClientId()
        {
            return _buffer.GetLong(_offset + ClientIdFieldOffset);
        }

        /// <summary>
        /// Set client id field.
        /// </summary>
        /// <param name="clientId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public ClientTimeoutFlyweight ClientId(long clientId)
        {
            _buffer.PutLong(_offset + ClientIdFieldOffset, clientId);

            return this;
        }
    }
}
