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
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Message to denote a response to get next session id command
    /// (<seealso cref="ControlProtocolEvents.ON_NEXT_AVAILABLE_SESSION_ID"/>).
    /// </summary>
    /// <seealso cref="ControlProtocolEvents"/>
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                         Correlation ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                         Next Session ID                        |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// <remarks>Since 1.49.0</remarks>
    public class NextAvailableSessionIdFlyweight
    {
        private const int CorrelationIdOffset = 0;
        private static readonly int SessionIdOffset = CorrelationIdOffset + SIZE_OF_LONG;

        /// <summary>
        /// Length of the header.
        /// </summary>
        public static readonly int LENGTH = SessionIdOffset + SIZE_OF_INT;

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public NextAvailableSessionIdFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// Get the correlation id field.
        /// </summary>
        /// <returns> correlation id field. </returns>
        public long CorrelationId()
        {
            return _buffer.GetLong(_offset + CorrelationIdOffset);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public NextAvailableSessionIdFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CorrelationIdOffset, correlationId);

            return this;
        }

        /// <summary>
        /// The session id.
        /// </summary>
        /// <returns> session id. </returns>
        public int NextSessionId()
        {
            return _buffer.GetInt(_offset + SessionIdOffset);
        }

        /// <summary>
        /// Set session id field.
        /// </summary>
        /// <param name="sessionId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public NextAvailableSessionIdFlyweight NextSessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SessionIdOffset, sessionId);

            return this;
        }

		/// <summary>
		/// {@inheritDoc}
		/// </summary>
		public override string ToString()
		{
			return
                "NextSessionIdFlyweight{" +
                "correlationId=" + CorrelationId() +
                ", sessionId=" + NextSessionId() +
                "}";
		}
	}
}
