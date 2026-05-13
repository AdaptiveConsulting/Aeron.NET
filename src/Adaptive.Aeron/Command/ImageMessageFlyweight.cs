/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message flyweight for any message that needs to represent a connection.
    /// <para>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                        Correlation ID                         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                 Subscription Registration ID                  |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                          Stream ID                            |
    ///  +---------------------------------------------------------------+
    ///  |                       Channel Length                          |
    ///  +---------------------------------------------------------------+
    ///  |                       Channel (ASCII)                        ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </para>
    /// </summary>
    public class ImageMessageFlyweight
    {
        private const int CorrelationIdOffset = 0;
        private const int SubscriptionRegistrationIdOffset = CorrelationIdOffset + BitUtil.SIZE_OF_LONG;
        private const int StreamIdFieldOffset = SubscriptionRegistrationIdOffset + BitUtil.SIZE_OF_LONG;
        private const int ChannelOffset = StreamIdFieldOffset + BitUtil.SIZE_OF_INT;

        private IMutableDirectBuffer _buffer;
        private int _offset;
        private int _lengthOfChannel;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for fluent API. </returns>
        public ImageMessageFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// The correlation id field. </summary>
        /// <returns> correlation id field. </returns>
        public long CorrelationId()
        {
            return _buffer.GetLong(_offset + CorrelationIdOffset);
        }

        /// <summary>
        /// Set the correlation id field. </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageMessageFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CorrelationIdOffset, correlationId);

            return this;
        }

        /// <summary>
        /// Registration ID for the subscription.
        /// </summary>
        /// <returns> registration ID for the subscription. </returns>
        public long SubscriptionRegistrationId()
        {
            return _buffer.GetLong(_offset + SubscriptionRegistrationIdOffset);
        }

        /// <summary>
        /// Set the registration ID for the subscription.
        /// </summary>
        /// <param name="registrationId"> for the subscription. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageMessageFlyweight SubscriptionRegistrationId(long registrationId)
        {
            _buffer.PutLong(_offset + SubscriptionRegistrationIdOffset, registrationId);

            return this;
        }

        /// <summary>
        /// The stream id field
        /// </summary>
        /// <returns> stream id field. </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + StreamIdFieldOffset);
        }

        /// <summary>
        /// Set the stream id field
        /// </summary>
        /// <param name="streamId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageMessageFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + StreamIdFieldOffset, streamId);

            return this;
        }

        /// <summary>
        /// Get the channel field as ASCII.
        /// </summary>
        /// <returns> channel field. </returns>
        public string Channel()
        {
            int length = _buffer.GetInt(_offset + ChannelOffset);
            _lengthOfChannel = BitUtil.SIZE_OF_INT + length;

            return _buffer.GetStringAscii(_offset + ChannelOffset, length);
        }

        /// <summary>
        /// Append the channel value to a <seealso cref="StringBuilder"/> .
        /// </summary>
        /// <param name="stringBuilder"> to append channel to. </param>
        public void AppendChannel(StringBuilder stringBuilder)
        {
            int length = _buffer.GetInt(_offset + ChannelOffset);
            _lengthOfChannel = BitUtil.SIZE_OF_INT + length;

            _buffer.GetStringAscii(_offset + ChannelOffset, stringBuilder);
        }

        /// <summary>
        /// Set the channel field as ASCII
        /// </summary>
        /// <param name="channel"> field value </param>
        /// <returns> this for a fluent API. </returns>
        public ImageMessageFlyweight Channel(string channel)
        {
            _lengthOfChannel = _buffer.PutStringAscii(_offset + ChannelOffset, channel);

            return this;
        }

        /// <summary>
        /// Get the length of the current message.
        ///
        /// NB: must be called after the data is written in order to be accurate.
        /// </summary>
        /// <returns> the length of the current message. </returns>
        public int Length()
        {
            return ChannelOffset + _lengthOfChannel;
        }
    }
}
