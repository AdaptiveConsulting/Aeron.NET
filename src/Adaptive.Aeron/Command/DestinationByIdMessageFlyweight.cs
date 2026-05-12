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
    /// Control message for removing a destination for a Publication in multi-destination-cast or a Subscription in
    /// multi-destination Subscription.
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
    ///  |                    Resource Correlation ID                    |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                   Destination Correlation ID                  |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </summary>
    public class DestinationByIdMessageFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int ResourceRegistrationIdOffset = CorrelationIdFieldOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int DestinationRegistrationIdOffset =
            ResourceRegistrationIdOffset + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Length of the encoded message in bytes.
        /// </summary>
        public static readonly int MESSAGE_LENGTH = LENGTH + (2 * BitUtil.SIZE_OF_LONG);

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new DestinationByIdMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);

            return this;
        }

        /// <summary>
        /// Get the registration id used for the resource that the destination has been registered to. Typically, a
        /// subscription or publication.
        /// </summary>
        /// <returns> resource registration id field. </returns>
        public long ResourceRegistrationId()
        {
            return _buffer.GetLong(_offset + ResourceRegistrationIdOffset);
        }

        /// <summary>
        /// Set the registration id used for the resource that the destination has been registered to. Typically, a
        /// subscription or publication.
        /// </summary>
        /// <param name="registrationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public DestinationByIdMessageFlyweight ResourceRegistrationId(long registrationId)
        {
            _buffer.PutLong(_offset + ResourceRegistrationIdOffset, registrationId);

            return this;
        }

        /// <summary>
        /// Returns the registration id for the destination.
        /// </summary>
        /// <returns> destination registration id. </returns>
        public long DestinationRegistrationId()
        {
            return _buffer.GetLong(_offset + DestinationRegistrationIdOffset);
        }

        /// <summary>
        /// Sets the registration id for the destination.
        /// </summary>
        /// <param name="destinationRegistrationId"> to reference the destination. </param>
        /// <returns> this for a fluent API. </returns>
        public DestinationByIdMessageFlyweight DestinationRegistrationId(long destinationRegistrationId)
        {
            _buffer.PutLong(_offset + DestinationRegistrationIdOffset, destinationRegistrationId);
            return this;
        }

        /// <summary>
        /// Validate _buffer length is long enough for message.
        /// </summary>
        /// <param name="msgTypeId"> type of message. </param>
        /// <param name="length"> of message in bytes to validate. </param>
        public new void ValidateLength(int msgTypeId, int length)
        {
            if (length < MESSAGE_LENGTH)
            {
                throw new ControlProtocolException(
                    ErrorCode.MALFORMED_COMMAND,
                    "command=" + msgTypeId + " too short: length=" + length
                );
            }
        }
    }
}
