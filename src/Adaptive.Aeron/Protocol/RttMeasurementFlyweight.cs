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
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Protocol
{
    /// <summary>
    /// Flyweight for an RTT Measurement Packet
    /// 
    /// <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#rtt-measurement-header">
    /// RTT Measurement Header</a>
    /// </summary>
    public class RttMeasurementFlyweight : HeaderFlyweight
    {
        public const short REPLY_FLAG = 0x80;
        public const int HEADER_LENGTH = 40;

        private const int SESSION_ID_FIELD_OFFSET = 8;
        private const int STREAM_ID_FIELD_OFFSET = 12;
        private const int ECHO_TIMESTAMP_FIELD_OFFSET = 16;
        private const int RECEPTION_DELTA_FIELD_OFFSET = 24;
        private const int RECEIVER_ID_FIELD_OFFSET = 32;

        public RttMeasurementFlyweight()
        {
        }

        public RttMeasurementFlyweight(UnsafeBuffer buffer) : base(buffer)
        {
        }

        /// <summary>
        /// return session id field
        /// </summary>
        /// <returns> session id field </returns>
        public virtual int SessionId()
        {
            return GetInt(SESSION_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set session id field
        /// </summary>
        /// <param name="sessionId"> field value </param>
        /// <returns> flyweight </returns>
        public virtual RttMeasurementFlyweight SessionId(int sessionId)
        {
            PutInt(SESSION_ID_FIELD_OFFSET, sessionId);

            return this;
        }

        /// <summary>
        /// return stream id field
        /// </summary>
        /// <returns> stream id field </returns>
        public virtual int StreamId()
        {
            return GetInt(STREAM_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set stream id field
        /// </summary>
        /// <param name="streamId"> field value </param>
        /// <returns> flyweight </returns>
        public virtual RttMeasurementFlyweight StreamId(int streamId)
        {
            PutInt(STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }

        public virtual long EchoTimestamp()
        {
            return GetLong(ECHO_TIMESTAMP_FIELD_OFFSET);
        }

        public virtual RttMeasurementFlyweight EchoTimestamp(long timestamp)
        {
            PutLong(ECHO_TIMESTAMP_FIELD_OFFSET, timestamp);

            return this;
        }

        public virtual long ReceptionDelta()
        {
            return GetLong(RECEPTION_DELTA_FIELD_OFFSET);
        }

        public virtual RttMeasurementFlyweight ReceptionDeltaFlyweight(long delta)
        {
            PutLong(RECEPTION_DELTA_FIELD_OFFSET, delta);

            return this;
        }

        public virtual long ReceiverId()
        {
            return GetLong(RECEIVER_ID_FIELD_OFFSET);
        }

        public virtual RttMeasurementFlyweight ReceiverId(long id)
        {
            PutLong(RECEIVER_ID_FIELD_OFFSET, id);

            return this;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            // TODO string formattedFlags = string.Format("{0,8}", Integer.toBinaryString(Flags())).Replace(' ', '0');
            string formattedFlags = string.Empty;

            sb.Append("RTT Measure Message{")
                .Append("frame_length=").Append(FrameLength())
                .Append(" version=").Append(Version())
                .Append(" flags=").Append(formattedFlags)
                .Append(" type=").Append(HeaderType())
                .Append(" session_id=").Append(SessionId())
                .Append(" stream_id=").Append(StreamId())
                .Append(" echo_timestamp=").Append(EchoTimestamp())
                .Append(" reception_delta=").Append(ReceptionDelta())
                .Append(" receiver_id=").Append(ReceiverId()).Append("}");

            return sb.ToString();
        }

}
}