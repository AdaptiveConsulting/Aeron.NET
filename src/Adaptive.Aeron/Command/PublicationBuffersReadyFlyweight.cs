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
    /// Message to denote that new buffers have been created for a publication.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents" />
    /// 
    ///  0                   1                   2                   3
    ///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                         Correlation ID                        |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                        Registration ID                        |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                          Session ID                           |
    /// +---------------------------------------------------------------+
    /// |                           Stream ID                           |
    /// +---------------------------------------------------------------+
    /// |                  Publication Limit Counter ID                 |
    /// +---------------------------------------------------------------+
    /// |                  Channel Status Indicator ID                  |
    /// +---------------------------------------------------------------+
    /// |                        Log File Length                        |
    /// +---------------------------------------------------------------+
    /// |                     Log File Name(ASCII)                    ...
    /// ...                                                              |
    /// +---------------------------------------------------------------+
    public class PublicationBuffersReadyFlyweight
    {
        private const int CORRELATION_ID_OFFSET = 0;
        private static readonly int REGISTRATION_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int SESSION_ID_OFFSET = REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int PUBLICATION_LIMIT_COUNTER_ID_OFFSET = STREAM_ID_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int CHANNEL_STATUS_INDICATOR_ID_OFFSET = PUBLICATION_LIMIT_COUNTER_ID_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int LOGFILE_FIELD_OFFSET = CHANNEL_STATUS_INDICATOR_ID_OFFSET + BitUtil.SIZE_OF_INT;

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
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
            return _buffer.GetLong(_offset + CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// Get the registration id field.
        /// </summary>
        /// <returns> correlation id field. </returns>
        public long RegistrationId()
        {
            return _buffer.GetLong(_offset + REGISTRATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="registrationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight RegistrationId(long registrationId)
        {
            _buffer.PutLong(_offset + REGISTRATION_ID_OFFSET, registrationId);

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
        /// Set the session id field.
        /// </summary>
        /// <param name="sessionId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight SessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SESSION_ID_OFFSET, sessionId);

            return this;
        }

        /// <summary>
        /// Get the stream id field.
        /// </summary>
        /// <returns> stream id field. </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + STREAM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// Set the stream id field.
        /// </summary>
        /// <param name="streamId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }

        /// <summary>
        /// The publication limit counter id.
        /// </summary>
        /// <returns> publication limit counter id. </returns>
        public int PublicationLimitCounterId()
        {
            return _buffer.GetInt(_offset + PUBLICATION_LIMIT_COUNTER_ID_OFFSET);
        }

        /// <summary>
        /// Set the position counter id field.
        /// </summary>
        /// <param name="positionCounterId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight PublicationLimitCounterId(int positionCounterId)
        {
            _buffer.PutInt(_offset + PUBLICATION_LIMIT_COUNTER_ID_OFFSET, positionCounterId);

            return this;
        }
        
        /// <summary>
        /// The channel status counter id.
        /// </summary>
        /// <returns> channel status counter id. </returns>
        public int ChannelStatusCounterId()
        {
            return _buffer.GetInt(_offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET);
        }

        /// <summary>
        /// Set channel status counter id field.
        /// </summary>
        /// <param name="counterId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight ChannelStatusCounterId(int counterId)
        {
            _buffer.PutInt(_offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET, counterId);

            return this;
        }

        /// <summary>
        /// Get the log file name in ASCII.
        /// </summary>
        /// <returns> the log file name in ASCII. </returns>
        public string LogFileName()
        {
            return _buffer.GetStringAscii(_offset + LOGFILE_FIELD_OFFSET);
        }
        
        /// <summary>
        /// Append the log file name to a <seealso cref="StringBuilder"/>.
        /// </summary>
        /// <param name="stringBuilder"> to append log file name to. </param>
        public void AppendLogFileName(StringBuilder stringBuilder)
        {
            _buffer.GetStringAscii(_offset + LOGFILE_FIELD_OFFSET, stringBuilder);
        }

        /// <summary>
        /// Set the log file name in ASCII.
        /// </summary>
        /// <param name="logFileName"> for the publication buffers. </param>
        /// <returns> the log file name in ASCII. </returns>
        public PublicationBuffersReadyFlyweight LogFileName(string logFileName)
        {
            _buffer.PutStringAscii(_offset + LOGFILE_FIELD_OFFSET, logFileName);
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
            return _buffer.GetInt(_offset + LOGFILE_FIELD_OFFSET) + LOGFILE_FIELD_OFFSET + BitUtil.SIZE_OF_INT;
        }

    }
}