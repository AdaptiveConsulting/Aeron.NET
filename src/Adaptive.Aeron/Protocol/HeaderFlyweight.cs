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

using System.Runtime.CompilerServices;
using System.Text;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Protocol
{
    /// <summary>
    /// Flyweight for general Aeron network protocol header of a message frame.
    /// 
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                        Frame Length                           |
    /// +---------------------------------------------------------------+
    /// |  Version    |     Flags     |               Type              |
    /// +-------------+---------------+---------------------------------+
    /// |                       Depends on Type                        ...
    /// 
    /// </summary>
    public class HeaderFlyweight : UnsafeBuffer
    {
        public static readonly byte[] EMPTY_BUFFER = new byte[0];

        /// <summary>
        /// Header type PAD. 
        /// </summary>
        public const int HDR_TYPE_PAD = 0x00;

        /// <summary>
        /// Header type DATA.
        /// </summary>
        public const int HDR_TYPE_DATA = 0x01;

        /// <summary>
        /// Header type NAK.
        /// </summary>
        public const int HDR_TYPE_NAK = 0x02;

        /// <summary>
        /// Header type SM.
        /// </summary>
        public const int HDR_TYPE_SM = 0x03;

        /// <summary>
        /// Header type ERR. 
        /// </summary>
        public const int HDR_TYPE_ERR = 0x04;

        /// <summary>
        /// Header type SETUP.
        /// </summary>
        public const int HDR_TYPE_SETUP = 0x05;

        /// <summary>
        /// Header type RTT Measurement. 
        /// </summary>
        public const int HDR_TYPE_RTTM = 0x06;
        
        /// <summary>
        /// Header type RESOLUTION.
        /// </summary>
        public const int HDR_TYPE_RES = 0x07;

        /// <summary>
        /// Header type ATS Data.
        /// </summary>
        public const int HDR_TYPE_ATS_DATA = 0x08;

        /// <summary>
        /// Header type ATS Status Message.
        /// </summary>
        public const int HDR_TYPE_ATS_SM = 0x09;

        /// <summary>
        /// Header type ATS Setup.
        /// </summary>
        public const int HDR_TYPE_ATS_SETUP = 0x0A;

        /// <summary>
        /// Header type Response Setup.
        /// </summary>
        public const int HDR_TYPE_RSP_SETUP = 0x0B;
        
        /// <summary>
        /// Header type EXT. 
        /// </summary>
        public const int HDR_TYPE_EXT = 0xFFFF;

        /// <summary>
        /// Default version.
        /// </summary>
        public const byte CURRENT_VERSION = 0x0;

        /// <summary>
        /// Offset in the frame at which the frame length field begins.
        /// </summary>
        public const int FRAME_LENGTH_FIELD_OFFSET = 0;
        
        /// <summary>
        /// Offset in the frame at which the version field begins.
        /// </summary>
        public const int VERSION_FIELD_OFFSET = 4;
        
        /// <summary>
        /// Offset in the frame at which the flags field begins.
        /// </summary>
        public const int FLAGS_FIELD_OFFSET = 5;
        
        /// <summary>
        /// Offset in the frame at which the frame type field begins.
        /// </summary>
        public const int TYPE_FIELD_OFFSET = 6;
        
        /// <summary>
        /// Minimum length of any Aeron frame.
        /// </summary>
        public static readonly int MIN_HEADER_LENGTH = TYPE_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;

        /// <summary>
        /// Default constructor which can later be used to wrap a frame.
        /// </summary>
        public HeaderFlyweight()
        {
        }

        /// <summary>
        /// Construct a flyweight which wraps a <seealso cref="UnsafeBuffer"/> over the frame.
        /// </summary>
        /// <param name="buffer"> to wrap for the flyweight. </param>
        public HeaderFlyweight(UnsafeBuffer buffer) : base(buffer)
        {
        }

        /// <summary>
        /// The version field value.
        /// </summary>
        /// <returns> version field value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short Version()
        {
            return (short) (GetByte(VERSION_FIELD_OFFSET) & 0xFF);
        }

        /// <summary>
        /// Set the version field value.
        /// </summary>
        /// <param name="version"> field value to be set. </param>
        /// <returns> this for a fluent API. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public HeaderFlyweight Version(short version)
        {
            PutByte(VERSION_FIELD_OFFSET, (byte) version);

            return this;
        }

        /// <summary>
        /// The flags field value.
        /// </summary>
        /// <returns> the flags field value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short Flags()
        {
            return (short) (GetByte(FLAGS_FIELD_OFFSET) & 0xFF);
        }

        /// <summary>
        /// Set the flags field value.
        /// </summary>
        /// <param name="flags"> field value </param>
        /// <returns> this for a fluent API. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public HeaderFlyweight Flags(short flags)
        {
            PutByte(FLAGS_FIELD_OFFSET, (byte) flags);

            return this;
        }

        /// <summary>
        /// The type field value.
        /// </summary>
        /// <returns> the type field value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int HeaderType()
        {
            return GetShort(TYPE_FIELD_OFFSET) & 0xFFFF;
        }

        /// <summary>
        /// Set the type field value.
        /// </summary>
        /// <param name="type"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public HeaderFlyweight HeaderType(int type)
        {
            PutShort(TYPE_FIELD_OFFSET, (short) type);

            return this;
        }

        /// <summary>
        /// The length of the frame field value.
        /// </summary>
        /// <returns> length of the frame field value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int FrameLength()
        {
            return GetInt(FRAME_LENGTH_FIELD_OFFSET);
        }

        /// <summary>
        /// Set the length of the frame field value.
        /// </summary>
        /// <param name="length"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public HeaderFlyweight FrameLength(int length)
        {
            PutInt(FRAME_LENGTH_FIELD_OFFSET, length);

            return this;
        }
        
        /// <summary>
        /// Convert header flags to an array of chars to be human-readable.
        /// </summary>
        /// <param name="flags"> to be converted. </param>
        /// <returns> header flags converted to an array of chars to be human-readable. </returns>
        public static char[] FlagsToChars(short flags)
        {
            char[] chars = {'0', '0', '0', '0', '0', '0', '0', '0'};
            int length = chars.Length;
            short mask = (short)(1 << (length - 1));

            for (int i = 0; i < length; i++)
            {
                if ((flags & mask) == mask)
                {
                    chars[i] = '1';
                }

                mask >>= 1;
            }

            return chars;
        }

        /// <summary>
        /// Append header flags to an <seealso cref="StringBuilder"/> to be human-readable.
        /// </summary>
        /// <param name="flags">      to be converted. </param>
        /// <param name="stringBuilder"> to append flags to. </param>
        public static void AppendFlagsAsChars(short flags, StringBuilder stringBuilder)
        {
            const int length = 8;
            short mask = (short) (1 << (length - 1));

            for (int i = 0; i < length; i++)
            {
                stringBuilder.Append((flags & mask) == mask ? '1' : '0');
                mask >>= 1;
            }
        }
    }
}