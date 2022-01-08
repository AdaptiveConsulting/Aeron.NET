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

using System.Text;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for adding or removing a destination for a Publication in multi-destination-cast or a Subscription
    /// in multi-destination Subscription.
    /// 
    ///  0                   1                   2                   3
    ///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                          Client ID                            |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                    Command Correlation ID                     |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                  Registration Correlation ID                  |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                       Channel Length                          |
    /// +---------------------------------------------------------------+
    /// |                       Channel(ASCII)                        ...
    /// ..                                                              |
    /// +---------------------------------------------------------------+
    /// 
    /// </summary>
    public class DestinationMessageFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int REGISTRATION_CORRELATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int CHANNEL_OFFSET = REGISTRATION_CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int MINIMUM_LENGTH = CHANNEL_OFFSET + BitUtil.SIZE_OF_INT;

        private int lengthOfChannel;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new DestinationMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);

            return this;
        }
        
        /// <summary>
        /// Return correlation id used in registration field.
        /// </summary>
        /// <returns> correlation id field. </returns>
        public long RegistrationCorrelationId()
        {
            return buffer.GetLong(offset + REGISTRATION_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set registration correlation id field.
        /// </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public DestinationMessageFlyweight RegistrationCorrelationId(long correlationId)
        {
            buffer.PutLong(offset + REGISTRATION_CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// Get the channel field in ASCII.
        /// </summary>
        /// <returns> channel field. </returns>
        public string Channel()
        {
            return buffer.GetStringAscii(offset + CHANNEL_OFFSET);
        }
        
        /// <summary>
        /// Append the channel value to a <seealso cref="StringBuilder"/>.
        /// </summary>
        /// <param name="stringBuilder"> to append channel to. </param>
        public void AppendChannel(StringBuilder stringBuilder)
        {
            buffer.GetStringAscii(offset + CHANNEL_OFFSET, stringBuilder);
        }

        /// <summary>
        /// Set channel field in ASCII.
        /// </summary>
        /// <param name="channel"> field value </param>
        /// <returns> this for a fluent API. </returns>
        public DestinationMessageFlyweight Channel(string channel)
        {
            lengthOfChannel = buffer.PutStringAscii(offset + CHANNEL_OFFSET, channel);

            return this;
        }

        /// <summary>
        /// Length of the frame in bytes.
        /// </summary>
        /// <returns> length of the frame in bytes. </returns>
        public int Length()
        {
            return CHANNEL_OFFSET + lengthOfChannel;
        }
        
        /// <summary>
        /// Compute the length of the command message for a given channel length.
        /// </summary>
        /// <param name="channelLength"> to be appended to the header. </param>
        /// <returns> the length of the command message for a given channel length. </returns>
        public static int ComputeLength(int channelLength)
        {
            return MINIMUM_LENGTH + channelLength;
        }
    }
}