﻿using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Protocol
{
    /// <summary>
    /// Flyweight for general Aeron network protocol header
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
        /// header type PAD </summary>
        public const int HDR_TYPE_PAD = 0x00;

        /// <summary>
        /// header type DATA </summary>
        public const int HDR_TYPE_DATA = 0x01;

        /// <summary>
        /// header type NAK </summary>
        public const int HDR_TYPE_NAK = 0x02;

        /// <summary>
        /// header type SM </summary>
        public const int HDR_TYPE_SM = 0x03;

        /// <summary>
        /// header type ERR </summary>
        public const int HDR_TYPE_ERR = 0x04;

        /// <summary>
        /// header type SETUP </summary>
        public const int HDR_TYPE_SETUP = 0x05;

        /// <summary>
        /// header type EXT </summary>
        public const int HDR_TYPE_EXT = 0xFFFF;

        /// <summary>
        /// default version </summary>
        public const byte CURRENT_VERSION = 0x0;

        public const int FRAME_LENGTH_FIELD_OFFSET = 0;
        public const int VERSION_FIELD_OFFSET = 4;
        public const int FLAGS_FIELD_OFFSET = 5;
        public const int TYPE_FIELD_OFFSET = 6;
        public static readonly int HEADER_LENGTH = TYPE_FIELD_OFFSET + BitUtil.SIZE_OF_SHORT;

        public HeaderFlyweight() : base(EMPTY_BUFFER)
        {
        }

        public HeaderFlyweight(UnsafeBuffer buffer) : base(buffer)
        {
        }

        /// <summary>
        /// return version field value
        /// </summary>
        /// <returns> ver field value </returns>
        public short Version()
        {
            return (short) (GetByte(VERSION_FIELD_OFFSET) & 0xFF);
        }

        /// <summary>
        /// set version field value
        /// </summary>
        /// <param name="version"> field value </param>
        /// <returns> flyweight </returns>
        public HeaderFlyweight Version(short version)
        {
            PutByte(VERSION_FIELD_OFFSET, (byte) version);

            return this;
        }

        /// <summary>
        /// return flags field value
        /// </summary>
        /// <returns> flags field value </returns>
        public short Flags()
        {
            return (short) (GetByte(FLAGS_FIELD_OFFSET) & 0xFF);
        }

        /// <summary>
        /// set the flags field value
        /// </summary>
        /// <param name="flags"> field value </param>
        /// <returns> flyweight </returns>
        public HeaderFlyweight Flags(short flags)
        {
            PutByte(FLAGS_FIELD_OFFSET, (byte) flags);

            return this;
        }

        /// <summary>
        /// return header type field
        /// </summary>
        /// <returns> type field value </returns>
        public int HeaderType()
        {
            return GetShort(TYPE_FIELD_OFFSET) & 0xFFFF;
        }

        /// <summary>
        /// set header type field
        /// </summary>
        /// <param name="type"> field value </param>
        /// <returns> flyweight </returns>
        public HeaderFlyweight HeaderType(int type)
        {
            PutShort(TYPE_FIELD_OFFSET, (short) type);

            return this;
        }

        /// <summary>
        /// return frame length field
        /// </summary>
        /// <returns> frame length field </returns>
        public int FrameLength()
        {
            return GetInt(FRAME_LENGTH_FIELD_OFFSET);
        }

        /// <summary>
        /// set frame length field
        /// </summary>
        /// <param name="length"> field value </param>
        /// <returns> flyweight </returns>
        public HeaderFlyweight FrameLength(int length)
        {
            PutInt(FRAME_LENGTH_FIELD_OFFSET, length);

            return this;
        }
    }
}