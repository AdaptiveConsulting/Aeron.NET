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

        private int lengthOfChannel;

        /// <summary>
        /// return correlation id used in registration field
        /// </summary>
        /// <returns> correlation id field </returns>
        public long RegistrationCorrelationId()
        {
            return buffer.GetLong(offset + REGISTRATION_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// set registration correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> flyweight </returns>
        public DestinationMessageFlyweight RegistrationCorrelationId(long correlationId)
        {
            buffer.PutLong(offset + REGISTRATION_CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// Get the channel field in ASCII
        /// </summary>
        /// <returns> channel field </returns>
        public string Channel()
        {
            return buffer.GetStringAscii(offset + CHANNEL_OFFSET);
        }

        /// <summary>
        /// Set channel field in ASCII
        /// </summary>
        /// <param name="channel"> field value </param>
        /// <returns> flyweight </returns>
        public DestinationMessageFlyweight Channel(string channel)
        {
            lengthOfChannel = buffer.PutStringAscii(offset + CHANNEL_OFFSET, channel);

            return this;
        }

        public int Length()
        {
            return CHANNEL_OFFSET + lengthOfChannel;
        }
    }
}