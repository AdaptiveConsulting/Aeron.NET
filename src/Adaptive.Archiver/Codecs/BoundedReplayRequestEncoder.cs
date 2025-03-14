/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class BoundedReplayRequestEncoder
{
    public const ushort BLOCK_LENGTH = 60;
    public const ushort TEMPLATE_ID = 18;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 10;

    private BoundedReplayRequestEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public BoundedReplayRequestEncoder()
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

    public BoundedReplayRequestEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public BoundedReplayRequestEncoder WrapAndApplyHeader(
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

    public BoundedReplayRequestEncoder ControlSessionId(long value)
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

    public BoundedReplayRequestEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int RecordingIdEncodingOffset()
    {
        return 16;
    }

    public static int RecordingIdEncodingLength()
    {
        return 8;
    }

    public static long RecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long RecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long RecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public BoundedReplayRequestEncoder RecordingId(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int PositionEncodingOffset()
    {
        return 24;
    }

    public static int PositionEncodingLength()
    {
        return 8;
    }

    public static long PositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long PositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long PositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public BoundedReplayRequestEncoder Position(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LengthEncodingOffset()
    {
        return 32;
    }

    public static int LengthEncodingLength()
    {
        return 8;
    }

    public static long LengthNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LengthMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LengthMaxValue()
    {
        return 9223372036854775807L;
    }

    public BoundedReplayRequestEncoder Length(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LimitCounterIdEncodingOffset()
    {
        return 40;
    }

    public static int LimitCounterIdEncodingLength()
    {
        return 4;
    }

    public static int LimitCounterIdNullValue()
    {
        return -2147483648;
    }

    public static int LimitCounterIdMinValue()
    {
        return -2147483647;
    }

    public static int LimitCounterIdMaxValue()
    {
        return 2147483647;
    }

    public BoundedReplayRequestEncoder LimitCounterId(int value)
    {
        _buffer.PutInt(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ReplayStreamIdEncodingOffset()
    {
        return 44;
    }

    public static int ReplayStreamIdEncodingLength()
    {
        return 4;
    }

    public static int ReplayStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int ReplayStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int ReplayStreamIdMaxValue()
    {
        return 2147483647;
    }

    public BoundedReplayRequestEncoder ReplayStreamId(int value)
    {
        _buffer.PutInt(_offset + 44, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int FileIoMaxLengthEncodingOffset()
    {
        return 48;
    }

    public static int FileIoMaxLengthEncodingLength()
    {
        return 4;
    }

    public static int FileIoMaxLengthNullValue()
    {
        return -2147483648;
    }

    public static int FileIoMaxLengthMinValue()
    {
        return -2147483647;
    }

    public static int FileIoMaxLengthMaxValue()
    {
        return 2147483647;
    }

    public BoundedReplayRequestEncoder FileIoMaxLength(int value)
    {
        _buffer.PutInt(_offset + 48, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ReplayTokenEncodingOffset()
    {
        return 52;
    }

    public static int ReplayTokenEncodingLength()
    {
        return 8;
    }

    public static long ReplayTokenNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ReplayTokenMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ReplayTokenMaxValue()
    {
        return 9223372036854775807L;
    }

    public BoundedReplayRequestEncoder ReplayToken(long value)
    {
        _buffer.PutLong(_offset + 52, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ReplayChannelId()
    {
        return 8;
    }

    public static string ReplayChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ReplayChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ReplayChannelHeaderLength()
    {
        return 4;
    }

    public BoundedReplayRequestEncoder PutReplayChannel(IDirectBuffer src, int srcOffset, int length)
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

    public BoundedReplayRequestEncoder PutReplayChannel(byte[] src, int srcOffset, int length)
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

    public BoundedReplayRequestEncoder ReplayChannel(string value)
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
        BoundedReplayRequestDecoder writer = new BoundedReplayRequestDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
