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
        private const int CorrelationIdOffset = 0;
        private static readonly int RegistrationIdOffset = CorrelationIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int SessionIdOffset = RegistrationIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int StreamIdFieldOffset = SessionIdOffset + BitUtil.SIZE_OF_INT;
        private static readonly int PublicationLimitCounterIdOffset = StreamIdFieldOffset + BitUtil.SIZE_OF_INT;
        private static readonly int ChannelStatusIndicatorIdOffset =
            PublicationLimitCounterIdOffset + BitUtil.SIZE_OF_INT;
        private static readonly int LogfileFieldOffset = ChannelStatusIndicatorIdOffset + BitUtil.SIZE_OF_INT;

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
            return _buffer.GetLong(_offset + CorrelationIdOffset);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight CorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + CorrelationIdOffset, correlationId);

            return this;
        }

        /// <summary>
        /// Get the registration id field.
        /// </summary>
        /// <returns> correlation id field. </returns>
        public long RegistrationId()
        {
            return _buffer.GetLong(_offset + RegistrationIdOffset);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="registrationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight RegistrationId(long registrationId)
        {
            _buffer.PutLong(_offset + RegistrationIdOffset, registrationId);

            return this;
        }

        /// <summary>
        /// Get the session id field.
        /// </summary>
        /// <returns> session id field. </returns>
        public int SessionId()
        {
            return _buffer.GetInt(_offset + SessionIdOffset);
        }

        /// <summary>
        /// Set the session id field.
        /// </summary>
        /// <param name="sessionId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight SessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SessionIdOffset, sessionId);

            return this;
        }

        /// <summary>
        /// Get the stream id field.
        /// </summary>
        /// <returns> stream id field. </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + StreamIdFieldOffset);
        }

        /// <summary>
        /// Set the stream id field.
        /// </summary>
        /// <param name="streamId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + StreamIdFieldOffset, streamId);

            return this;
        }

        /// <summary>
        /// The publication limit counter id.
        /// </summary>
        /// <returns> publication limit counter id. </returns>
        public int PublicationLimitCounterId()
        {
            return _buffer.GetInt(_offset + PublicationLimitCounterIdOffset);
        }

        /// <summary>
        /// Set the position counter id field.
        /// </summary>
        /// <param name="positionCounterId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight PublicationLimitCounterId(int positionCounterId)
        {
            _buffer.PutInt(_offset + PublicationLimitCounterIdOffset, positionCounterId);

            return this;
        }

        /// <summary>
        /// The channel status counter id.
        /// </summary>
        /// <returns> channel status counter id. </returns>
        public int ChannelStatusCounterId()
        {
            return _buffer.GetInt(_offset + ChannelStatusIndicatorIdOffset);
        }

        /// <summary>
        /// Set channel status counter id field.
        /// </summary>
        /// <param name="counterId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationBuffersReadyFlyweight ChannelStatusCounterId(int counterId)
        {
            _buffer.PutInt(_offset + ChannelStatusIndicatorIdOffset, counterId);

            return this;
        }

        /// <summary>
        /// Get the log file name in ASCII.
        /// </summary>
        /// <returns> the log file name in ASCII. </returns>
        public string LogFileName()
        {
            return _buffer.GetStringAscii(_offset + LogfileFieldOffset);
        }

        /// <summary>
        /// Append the log file name to a <seealso cref="StringBuilder"/> .
        /// </summary>
        /// <param name="stringBuilder"> to append log file name to. </param>
        public void AppendLogFileName(StringBuilder stringBuilder)
        {
            _buffer.GetStringAscii(_offset + LogfileFieldOffset, stringBuilder);
        }

        /// <summary>
        /// Set the log file name in ASCII.
        /// </summary>
        /// <param name="logFileName"> for the publication buffers. </param>
        /// <returns> the log file name in ASCII. </returns>
        public PublicationBuffersReadyFlyweight LogFileName(string logFileName)
        {
            _buffer.PutStringAscii(_offset + LogfileFieldOffset, logFileName);
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
            return _buffer.GetInt(_offset + LogfileFieldOffset) + LogfileFieldOffset + BitUtil.SIZE_OF_INT;
        }
    }
}
