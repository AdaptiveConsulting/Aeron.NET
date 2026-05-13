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
using static Adaptive.Agrona.BitUtil;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for getting next available session id from the media driver
    /// (<seealso cref="ControlProtocolEvents.GET_NEXT_AVAILABLE_SESSION_ID"/>).
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                          Client ID                            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                    Command Correlation ID                     |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                         Stream Id                             |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public sealed class GetNextAvailableSessionIdMessageFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int StreamIdOffset = CorrelatedMessageFlyweight.LENGTH;

        /// <summary>
        /// Length of the header.
        /// </summary>
        public static new readonly int LENGTH = StreamIdOffset + SIZE_OF_INT;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new GetNextAvailableSessionIdMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);

            return this;
        }

        /// <summary>
        /// Get the stream id.
        /// </summary>
        /// <returns> the stream id. </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + StreamIdOffset);
        }

        /// <summary>
        /// Set the stream id.
        /// </summary>
        /// <param name="streamId"> the channel id. </param>
        /// <returns> this for a fluent API. </returns>
        public GetNextAvailableSessionIdMessageFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + StreamIdOffset, streamId);

            return this;
        }

        /// <summary>
        /// Length of the message in bytes. Only valid after the channel is set.
        /// </summary>
        /// <returns> length of the message in bytes. </returns>
        public int Length()
        {
            return LENGTH;
        }

        /// <summary>
        /// Validate _buffer length is long enough for message.
        /// </summary>
        /// <param name="msgTypeId"> type of message. </param>
        /// <param name="length"> of message in bytes to validate. </param>
        public new void ValidateLength(int msgTypeId, int length)
        {
            if (length < LENGTH)
            {
                throw new ControlProtocolException(
                    ErrorCode.MALFORMED_COMMAND,
                    "command=" + msgTypeId + " too short: length=" + length
                );
            }
        }
    }
}
