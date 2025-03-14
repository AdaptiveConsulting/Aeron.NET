/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class CatalogHeaderEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 20;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 10;

    private CatalogHeaderEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public CatalogHeaderEncoder()
    {
        _parentMessage = this;
    }

    public ushort SbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public ushort SbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public ushort SbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public ushort SbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public string SbeSemanticType()
    {
        return "";
    }

    public IMutableDirectBuffer Buffer()
    {
        return _buffer;
    }

    public int Offset()
    {
        return _offset;
    }

    public CatalogHeaderEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public CatalogHeaderEncoder WrapAndApplyHeader(
        IMutableDirectBuffer buffer, int offset, MessageHeaderEncoder headerEncoder)
    {
        headerEncoder
            .Wrap(buffer, offset)
            .BlockLength(BLOCK_LENGTH)
            .TemplateId(TEMPLATE_ID)
            .SchemaId(SCHEMA_ID)
            .Version(SCHEMA_VERSION);

        return Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public int EncodedLength()
    {
        return _limit - _offset;
    }

    public int Limit()
    {
        return _limit;
    }

    public void Limit(int limit)
    {
        this._limit = limit;
    }

    public static int VersionEncodingOffset()
    {
        return 0;
    }

    public static int VersionEncodingLength()
    {
        return 4;
    }

    public static int VersionNullValue()
    {
        return -2147483648;
    }

    public static int VersionMinValue()
    {
        return -2147483647;
    }

    public static int VersionMaxValue()
    {
        return 2147483647;
    }

    public CatalogHeaderEncoder Version(int value)
    {
        _buffer.PutInt(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LengthEncodingOffset()
    {
        return 4;
    }

    public static int LengthEncodingLength()
    {
        return 4;
    }

    public static int LengthNullValue()
    {
        return -2147483648;
    }

    public static int LengthMinValue()
    {
        return -2147483647;
    }

    public static int LengthMaxValue()
    {
        return 2147483647;
    }

    public CatalogHeaderEncoder Length(int value)
    {
        _buffer.PutInt(_offset + 4, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int NextRecordingIdEncodingOffset()
    {
        return 8;
    }

    public static int NextRecordingIdEncodingLength()
    {
        return 8;
    }

    public static long NextRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public CatalogHeaderEncoder NextRecordingId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int AlignmentEncodingOffset()
    {
        return 16;
    }

    public static int AlignmentEncodingLength()
    {
        return 4;
    }

    public static int AlignmentNullValue()
    {
        return -2147483648;
    }

    public static int AlignmentMinValue()
    {
        return -2147483647;
    }

    public static int AlignmentMaxValue()
    {
        return 2147483647;
    }

    public CatalogHeaderEncoder Alignment(int value)
    {
        _buffer.PutInt(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ReservedEncodingOffset()
    {
        return 31;
    }

    public static int ReservedEncodingLength()
    {
        return 1;
    }

    public static sbyte ReservedNullValue()
    {
        return (sbyte)-128;
    }

    public static sbyte ReservedMinValue()
    {
        return (sbyte)-127;
    }

    public static sbyte ReservedMaxValue()
    {
        return (sbyte)127;
    }

    public CatalogHeaderEncoder Reserved(sbyte value)
    {
        _buffer.PutByte(_offset + 31, unchecked((byte)value));
        return this;
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        CatalogHeaderDecoder writer = new CatalogHeaderDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
