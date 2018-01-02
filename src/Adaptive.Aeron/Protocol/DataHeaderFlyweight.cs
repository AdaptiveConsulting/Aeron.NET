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

using System;
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Protocol
{
    /// <summary>
    /// HeaderFlyweight for Data Header
    /// 
    /// <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-frame">Data Frame</a>
    /// </summary>
    public class DataHeaderFlyweight : HeaderFlyweight
    {
        /// <summary>
        /// Length of the Data Header
        /// </summary>
        public new const int HEADER_LENGTH = 32;

        /// <summary>
        /// Begin Flag
        /// </summary>
        public const short BEGIN_FLAG = 0x80;

        /// <summary>
        /// End Flag
        /// </summary>
        public const short END_FLAG = 0x40;

        /// <summary>
        /// Begin and End Flags
        /// </summary>
        public static readonly short BEGIN_AND_END_FLAGS = BEGIN_FLAG | END_FLAG;

        /// <summary>
        /// End of Stream Flag
        /// </summary>
        public const short EOS_FLAG = 0x20;

        /// <summary>
        /// Begin, End, and End of Stream Flags
        /// </summary>
        public static readonly short BEGIN_END_AND_EOS_FLAGS = BEGIN_FLAG | END_FLAG | EOS_FLAG;

        public const long DEFAULT_RESERVE_VALUE = 0L;

        public const int TERM_OFFSET_FIELD_OFFSET = 8;
        public const int SESSION_ID_FIELD_OFFSET = 12;
        public const int STREAM_ID_FIELD_OFFSET = 16;
        public const int TERM_ID_FIELD_OFFSET = 20;
        public const int RESERVED_VALUE_OFFSET = 24;
        public const int DATA_OFFSET = HEADER_LENGTH;

        public DataHeaderFlyweight()
        {
        }

        public DataHeaderFlyweight(UnsafeBuffer buffer) : base(buffer)
        {
        }
        
        /// <summary>
        /// Is the frame at data frame at the beginning of packet a heartbeat message?
        /// </summary>
        /// <param name="packet"> containing the data frame. </param>
        /// <param name="length"> of the data frame. </param>
        /// <returns> true if a heartbeat otherwise false. </returns>
        public static bool IsHeartbeat(UnsafeBuffer packet, int length)
        {
            return length == HEADER_LENGTH && packet.GetInt(0) == 0;
        }

        /// <summary>
        /// Does the data frame in the packet have the EOS flag set?
        /// </summary>
        /// <param name="packet"> containing the data frame. </param>
        /// <returns> true if the EOS flag is set otherwise false. </returns>
        public static bool IsEndOfStream(UnsafeBuffer packet)
        {
            return BEGIN_END_AND_EOS_FLAGS == (packet.GetByte(FLAGS_FIELD_OFFSET) & 0xFF);
        }

        /// <summary>
        /// return session id field
        /// </summary>
        /// <returns> session id field </returns>
        public int SessionId()
        {
            return GetInt(SESSION_ID_FIELD_OFFSET);
        }
        
        public static int SessionId(UnsafeBuffer termBuffer, int frameOffset)
        {
            return termBuffer.GetInt(frameOffset + SESSION_ID_FIELD_OFFSET, ByteOrder.LittleEndian);
        }

        /// <summary>
        /// set session id field
        /// </summary>
        /// <param name="sessionId"> field value </param>
        /// <returns> flyweight </returns>
        public DataHeaderFlyweight SessionId(int sessionId)
        {
            PutInt(SESSION_ID_FIELD_OFFSET, sessionId);

            return this;
        }

        /// <summary>
        /// return stream id field
        /// </summary>
        /// <returns> stream id field </returns>
        public int StreamId()
        {
            return GetInt(STREAM_ID_FIELD_OFFSET);
        }
        
        public static int StreamId(UnsafeBuffer termBuffer, int frameOffset)
        {
            return termBuffer.GetInt(frameOffset + STREAM_ID_FIELD_OFFSET, ByteOrder.LittleEndian);
        }

        /// <summary>
        /// set stream id field
        /// </summary>
        /// <param name="streamId"> field value </param>
        /// <returns> flyweight </returns>
        public DataHeaderFlyweight StreamId(int streamId)
        {
            PutInt(STREAM_ID_FIELD_OFFSET, streamId);

            return this;
        }

        /// <summary>
        /// return term id field
        /// </summary>
        /// <returns> term id field </returns>
        public int TermId()
        {
            return GetInt(TERM_ID_FIELD_OFFSET);
        }

        public static int TermId(UnsafeBuffer termBuffer, int frameOffset)
        {
            return termBuffer.GetInt(frameOffset + TERM_ID_FIELD_OFFSET, ByteOrder.LittleEndian);
        }
        
        /// <summary>
        /// set term id field
        /// </summary>
        /// <param name="termId"> field value </param>
        /// <returns> flyweight </returns>
        public DataHeaderFlyweight TermId(int termId)
        {
            PutInt(TERM_ID_FIELD_OFFSET, termId);

            return this;
        }

        /// <summary>
        /// return term offset field
        /// </summary>
        /// <returns> term offset field </returns>
        public int TermOffset()
        {
            return GetInt(TERM_OFFSET_FIELD_OFFSET);
        }

        public static int TermOffset(UnsafeBuffer termBuffer, int frameOffset)
        {
            return termBuffer.GetInt(frameOffset + TERM_OFFSET_FIELD_OFFSET, ByteOrder.LittleEndian);
        }
        
        /// <summary>
        /// set term offset field
        /// </summary>
        /// <param name="termOffset"> field value </param>
        /// <returns> flyweight </returns>
        public DataHeaderFlyweight TermOffset(int termOffset)
        {
            PutInt(TERM_OFFSET_FIELD_OFFSET, termOffset);

            return this;
        }

        /// <summary>
        /// Get the reserved value in LITTLE_ENDIAN format.
        /// </summary>
        /// <returns> value of the reserved value. </returns>
        public long ReservedValue()
        {
            return GetLong(RESERVED_VALUE_OFFSET);
        }

        /// <summary>
        /// Set the reserved value in LITTLE_ENDIAN format.
        /// </summary>
        /// <param name="reservedValue"> to be stored </param>
        /// <returns> flyweight </returns>
        public DataHeaderFlyweight ReservedValue(long reservedValue)
        {
            PutLong(RESERVED_VALUE_OFFSET, reservedValue);

            return this;
        }

        /// <summary>
        /// Return offset in buffer for data
        /// </summary>
        /// <returns> offset of data in the buffer </returns>
        public int DataOffset()
        {
            return DATA_OFFSET;
        }

        /// <summary>
        /// Return an initialised default Data Frame Header.
        /// </summary>
        /// <param name="sessionId"> for the header </param>
        /// <param name="streamId">  for the header </param>
        /// <param name="termId">    for the header </param>
        /// <returns> byte array containing the header </returns>
	    public static UnsafeBuffer CreateDefaultHeader(int sessionId, int streamId, int termId)
        {
            var buffer = new UnsafeBuffer(BufferUtil.AllocateDirectAligned(HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT));

            buffer.PutByte(VERSION_FIELD_OFFSET, CURRENT_VERSION);
            buffer.PutByte(FLAGS_FIELD_OFFSET, (byte)BEGIN_AND_END_FLAGS);
            buffer.PutShort(TYPE_FIELD_OFFSET, HDR_TYPE_DATA);
            buffer.PutInt(SESSION_ID_FIELD_OFFSET, sessionId);
            buffer.PutInt(STREAM_ID_FIELD_OFFSET, streamId);
            buffer.PutInt(TERM_ID_FIELD_OFFSET, termId);
            buffer.PutLong(RESERVED_VALUE_OFFSET, DEFAULT_RESERVE_VALUE);

            return buffer;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            var formattedFlags = $"{Convert.ToString(Flags(), 2),8}".Replace(' ', '0');

            sb.Append("DATA Header{")
                .Append("frame_length=").Append(FrameLength())
                .Append(" version=").Append(Version())
                .Append(" flags=").Append(formattedFlags)
                .Append(" type=").Append(HeaderType())
                .Append(" term_offset=").Append(TermOffset())
                .Append(" session_id=").Append(SessionId())
                .Append(" stream_id=").Append(StreamId())
                .Append(" term_id=").Append(TermId())
                .Append(" reserved_value=").Append(ReservedValue())
                .Append("}");

            return sb.ToString();
        }
}
}