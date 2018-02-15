/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class CatalogHeaderEncoder
{
    public const ushort BLOCK_LENGTH = 8;
    public const ushort TEMPLATE_ID = 20;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 0;

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


    public static int EntryLengthEncodingOffset()
    {
        return 4;
    }

    public static int EntryLengthEncodingLength()
    {
        return 4;
    }

    public static int EntryLengthNullValue()
    {
        return -2147483648;
    }

    public static int EntryLengthMinValue()
    {
        return -2147483647;
    }

    public static int EntryLengthMaxValue()
    {
        return 2147483647;
    }

    public CatalogHeaderEncoder EntryLength(int value)
    {
        _buffer.PutInt(_offset + 4, value, ByteOrder.LittleEndian);
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
