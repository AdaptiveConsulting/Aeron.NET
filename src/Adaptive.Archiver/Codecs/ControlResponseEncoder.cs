/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class ControlResponseEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 1;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 8;

    private ControlResponseEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ControlResponseEncoder()
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

    public ControlResponseEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ControlResponseEncoder WrapAndApplyHeader(
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

    public static int ControlSessionIdEncodingOffset()
    {
        return 0;
    }

    public static int ControlSessionIdEncodingLength()
    {
        return 8;
    }

    public static long ControlSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ControlSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ControlSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ControlResponseEncoder ControlSessionId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CorrelationIdEncodingOffset()
    {
        return 8;
    }

    public static int CorrelationIdEncodingLength()
    {
        return 8;
    }

    public static long CorrelationIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CorrelationIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CorrelationIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ControlResponseEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int RelevantIdEncodingOffset()
    {
        return 16;
    }

    public static int RelevantIdEncodingLength()
    {
        return 8;
    }

    public static long RelevantIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long RelevantIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long RelevantIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ControlResponseEncoder RelevantId(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CodeEncodingOffset()
    {
        return 24;
    }

    public static int CodeEncodingLength()
    {
        return 4;
    }

    public ControlResponseEncoder Code(ControlResponseCode value)
    {
        _buffer.PutInt(_offset + 24, (int)value, ByteOrder.LittleEndian);
        return this;
    }

    public static int VersionEncodingOffset()
    {
        return 28;
    }

    public static int VersionEncodingLength()
    {
        return 4;
    }

    public static int VersionNullValue()
    {
        return 0;
    }

    public static int VersionMinValue()
    {
        return 2;
    }

    public static int VersionMaxValue()
    {
        return 16777215;
    }

    public ControlResponseEncoder Version(int value)
    {
        _buffer.PutInt(_offset + 28, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ErrorMessageId()
    {
        return 6;
    }

    public static string ErrorMessageCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ErrorMessageMetaAttribute(MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case MetaAttribute.EPOCH: return "unix";
            case MetaAttribute.TIME_UNIT: return "nanosecond";
            case MetaAttribute.SEMANTIC_TYPE: return "";
            case MetaAttribute.PRESENCE: return "required";
        }

        return "";
    }

    public static int ErrorMessageHeaderLength()
    {
        return 4;
    }

    public ControlResponseEncoder PutErrorMessage(IDirectBuffer src, int srcOffset, int length)
    {
        if (length > 1073741824)
        {
            throw new InvalidOperationException("length > maxValue for type: " + length);
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        _parentMessage.Limit(limit + headerLength + length);
        _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
        _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public ControlResponseEncoder PutErrorMessage(byte[] src, int srcOffset, int length)
    {
        if (length > 1073741824)
        {
            throw new InvalidOperationException("length > maxValue for type: " + length);
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        _parentMessage.Limit(limit + headerLength + length);
        _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
        _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public ControlResponseEncoder ErrorMessage(string value)
    {
        int length = value.Length;
        if (length > 1073741824)
        {
            throw new InvalidOperationException("length > maxValue for type: " + length);
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        _parentMessage.Limit(limit + headerLength + length);
        _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
        _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        ControlResponseDecoder writer = new ControlResponseDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
