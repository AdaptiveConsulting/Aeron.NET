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
    /// Message to denote that new buffers for a publication image are ready for a subscription.
    /// 
    /// NOTE: Layout should be SBE compliant
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
    /// |                  Subscriber Registration Id                   |
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
        private static readonly int SUBSCRIBER_REGISTRATION_ID_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int SUBSCRIBER_POSITION_ID_OFFSET = SUBSCRIBER_REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int LOG_FILE_NAME_OFFSET = SUBSCRIBER_POSITION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        
        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public ImageBuffersReadyFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// return correlation id field
        /// </summary>
        /// <returns> correlation id field </returns>
        public long CorrelationId()
        {
            return _buffer.GetLong(_offset + CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// set correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// return session id field
        /// </summary>
        /// <returns> session id field </returns>
        public int SessionId()
        {
            return _buffer.GetInt(_offset + SESSION_ID_OFFSET);
        }

        /// <summary>
        /// set session id field </summary>
        /// <param name="sessionId"> field value </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SESSION_ID_OFFSET, sessionId);

            return this;
        }

        /// <summary>
        /// return stream id field
        /// </summary>
        /// <returns> stream id field </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + STREAM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set stream id field
        /// </summary>
        /// <param name="streamId"> field value </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }
        
        /// <summary>
        /// Set the position Id for the subscriber
        /// </summary>
        /// <param name="id"> for the subscriber position </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SubscriberPositionId(int id)
        {
            _buffer.PutInt(_offset + SUBSCRIBER_POSITION_ID_OFFSET, id);

            return this;
        }

        /// <summary>
        /// Return the position Id for the subscriber
        /// </summary>
        /// <returns> position Id for the subscriber </returns>
        public int SubscriberPositionId()
        {
            return _buffer.GetInt(_offset + SUBSCRIBER_POSITION_ID_OFFSET);
        }

        /// <summary>
        /// Set the registration Id for the subscriber position
        /// </summary>
        /// <param name="id"> for the subscriber position </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SubscriberRegistrationId(long id)
        {
            _buffer.PutLong(_offset + SUBSCRIBER_REGISTRATION_ID_OFFSET, id);

            return this;
        }

        /// <summary>
        /// Return the registration Id for the subscriber position
        /// </summary>
        /// <returns> registration Id for the subscriber position </returns>
        public long SubscriberRegistrationId()
        {
            return _buffer.GetLong(_offset + SUBSCRIBER_REGISTRATION_ID_OFFSET);
        }

        /// <summary>
        /// Return the Log Filename in ASCII
        /// </summary>
        /// <returns> log filename </returns>
        public string LogFileName()
        {
            return _buffer.GetStringAscii(_offset + LOG_FILE_NAME_OFFSET);
        }

        /// <summary>
        /// Set the log filename in ASCII
        /// </summary>
        /// <param name="logFileName"> for the image </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight LogFileName(string logFileName)
        {
            _buffer.PutStringAscii(_offset + LOG_FILE_NAME_OFFSET, logFileName);
            return this;
        }

        /// <summary>
        /// Return the source identity string in ASCII
        /// </summary>
        /// <returns> source identity string </returns>
        public string SourceIdentity()
        {
            return _buffer.GetStringAscii(_offset + SourceIdentityOffset());
        }

        /// <summary>
        /// Set the source identity string in ASCII
        /// </summary>
        /// <param name="value"> for the source identity </param>
        /// <returns> flyweight </returns>
        public ImageBuffersReadyFlyweight SourceIdentity(string value)
        {
            _buffer.PutStringAscii(_offset + SourceIdentityOffset(), value);
            return this;
        }

        /// <summary>
        /// Get the length of the current message
        /// 
        /// NB: must be called after the data is written in order to be accurate.
        /// </summary>
        /// <returns> the length of the current message </returns>
        public int Length()
        {
            int sourceIdentityOffset = SourceIdentityOffset();
            return sourceIdentityOffset + _buffer.GetInt(_offset + sourceIdentityOffset) + BitUtil.SIZE_OF_INT;
        }
        
        private int SourceIdentityOffset()
        {
            return LOG_FILE_NAME_OFFSET + _buffer.GetInt(_offset + LOG_FILE_NAME_OFFSET) + BitUtil.SIZE_OF_INT;
        }
    }
}