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
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Message to denote that new buffers for a publication image are ready for a subscription.
    /// 
    /// <b>Note:</b> Layout should be SBE 2.0 compliant so that the source identity length is aligned.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents" />
    /// 
    ///  0                   1                   2                   3
    ///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                       Correlation ID                          |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                         Session ID                            |
    /// +---------------------------------------------------------------+
    /// |                          Stream ID                            |
    /// +---------------------------------------------------------------+
    /// |                Subscription Registration Id                   |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                    Subscriber Position Id                     |
    /// +---------------------------------------------------------------+
    /// |                       Log File Length                         |
    /// +---------------------------------------------------------------+
    /// |                     Log File Name(ASCII)                    ..
    /// ..                                                              |
    /// +---------------------------------------------------------------+
    /// |                    Source identity Length                     |
    /// +---------------------------------------------------------------+
    /// |                    Source identity(ASCII)                   ..
    /// ..                                                              |
    /// +---------------------------------------------------------------+
    /// 
    public class ImageBuffersReadyFlyweight
    {
        private static readonly int CORRELATION_ID_OFFSET = 0;
        private static readonly int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int SUBSCRIPTION_REGISTRATION_ID_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int SUBSCRIBER_POSITION_ID_OFFSET = SUBSCRIPTION_REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int LOG_FILE_NAME_OFFSET = SUBSCRIBER_POSITION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        
        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// The correlation id field.
        /// </summary>
        /// <returns> correlation id field. </returns>
        public long CorrelationId()
        {
            return _buffer.GetLong(_offset + CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// Get the session id field.
        /// </summary>
        /// <returns> session id field. </returns>
        public int SessionId()
        {
            return _buffer.GetInt(_offset + SESSION_ID_OFFSET);
        }

        /// <summary>
        /// Set the session id field. </summary>
        /// <param name="sessionId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight SessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SESSION_ID_OFFSET, sessionId);

            return this;
        }

        /// <summary>
        /// Get the stream id field
        /// </summary>
        /// <returns> stream id field </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + STREAM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// Set the stream id field.
        /// </summary>
        /// <param name="streamId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }
        
        /// <summary>
        /// Set the position counter id for the subscriber.
        /// </summary>
        /// <param name="id"> for the subscriber position counter. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight SubscriberPositionId(int id)
        {
            _buffer.PutInt(_offset + SUBSCRIBER_POSITION_ID_OFFSET, id);

            return this;
        }

        /// <summary>
        /// The the position counter id for the subscriber.
        /// </summary>
        /// <returns> position counter id for the subscriber. </returns>
        public int SubscriberPositionId()
        {
            return _buffer.GetInt(_offset + SUBSCRIBER_POSITION_ID_OFFSET);
        }

        /// <summary>
        /// Set the registration id for the Subscription.
        /// </summary>
        /// <param name="id"> for the Subscription. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight SubscriptionRegistrationId(long id)
        {
            _buffer.PutLong(_offset + SUBSCRIPTION_REGISTRATION_ID_OFFSET, id);

            return this;
        }

        /// <summary>
        /// Return the registration id for the Subscription.
        /// </summary>
        /// <returns> registration id for the Subscription. </returns>
        public long SubscriptionRegistrationId()
        {
            return _buffer.GetLong(_offset + SUBSCRIPTION_REGISTRATION_ID_OFFSET);
        }

        /// <summary>
        /// The Log Filename in ASCII.
        /// </summary>
        /// <returns> log filename. </returns>
        public string LogFileName()
        {
            return _buffer.GetStringAscii(_offset + LOG_FILE_NAME_OFFSET);
        }
        
        /// <summary>
        /// Append the log file name to a <seealso cref="StringBuilder"/>.
        /// </summary>
        /// <param name="stringBuilder"> to append log file name to. </param>
        public void AppendLogFileName(StringBuilder stringBuilder)
        {
            _buffer.GetStringAscii(_offset + LOG_FILE_NAME_OFFSET, stringBuilder);
        }

        /// <summary>
        /// Set the log filename in ASCII.
        /// </summary>
        /// <param name="logFileName"> for the image. </param>
        /// <returns> this for a fluent API. </returns>
        public ImageBuffersReadyFlyweight LogFileName(string logFileName)
        {
            _buffer.PutStringAscii(_offset + LOG_FILE_NAME_OFFSET, logFileName);
            return this;
        }

        /// <summary>
        /// The source identity string in ASCII.
        /// </summary>
        /// <returns> source identity string. </returns>
        public string SourceIdentity()
        {
            return _buffer.GetStringAscii(_offset + SourceIdentityOffset());
        }

        /// <summary>
        /// Append the source identity to an <seealso cref="StringBuilder"/>.
        /// </summary>
        /// <param name="stringBuilder"> to append source identity to. </param>
        public void AppendSourceIdentity(StringBuilder stringBuilder)
        {
            _buffer.GetStringAscii(_offset + SourceIdentityOffset(), stringBuilder);
        }

        /// <summary>
        /// Set the source identity string in ASCII.
        /// Note: Can be called only after log file name was set!
        /// </summary>
        /// <param name="value"> for the source identity. </param>
        /// <returns> this for a fluent API. </returns>
        /// <see cref="LogFileName(string)"/>
        public ImageBuffersReadyFlyweight SourceIdentity(string value)
        {
            _buffer.PutStringAscii(_offset + SourceIdentityOffset(), value);
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
            int sourceIdentityOffset = SourceIdentityOffset();
            return sourceIdentityOffset + _buffer.GetInt(_offset + sourceIdentityOffset) + BitUtil.SIZE_OF_INT;
        }
        
        private int SourceIdentityOffset()
        {
            int alignedLength = BitUtil.Align(_buffer.GetInt(_offset + LOG_FILE_NAME_OFFSET), BitUtil.SIZE_OF_INT);
            
            return LOG_FILE_NAME_OFFSET + BitUtil.SIZE_OF_INT + alignedLength;
        }
    }
}