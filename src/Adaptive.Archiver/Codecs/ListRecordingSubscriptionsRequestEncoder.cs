/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class ListRecordingSubscriptionsRequestEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 17;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 2;

    private ListRecordingSubscriptionsRequestEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ListRecordingSubscriptionsRequestEncoder()
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

    public ListRecordingSubscriptionsRequestEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ListRecordingSubscriptionsRequestEncoder WrapAndApplyHeader(
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

    public ListRecordingSubscriptionsRequestEncoder ControlSessionId(long value)
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

    public ListRecordingSubscriptionsRequestEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int PseudoIndexEncodingOffset()
    {
        return 16;
    }

    public static int PseudoIndexEncodingLength()
    {
        return 4;
    }

    public static int PseudoIndexNullValue()
    {
        return -2147483648;
    }

    public static int PseudoIndexMinValue()
    {
        return -2147483647;
    }

    public static int PseudoIndexMaxValue()
    {
        return 2147483647;
    }

    public ListRecordingSubscriptionsRequestEncoder PseudoIndex(int value)
    {
        _buffer.PutInt(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SubscriptionCountEncodingOffset()
    {
        return 20;
    }

    public static int SubscriptionCountEncodingLength()
    {
        return 4;
    }

    public static int SubscriptionCountNullValue()
    {
        return -2147483648;
    }

    public static int SubscriptionCountMinValue()
    {
        return -2147483647;
    }

    public static int SubscriptionCountMaxValue()
    {
        return 2147483647;
    }

    public ListRecordingSubscriptionsRequestEncoder SubscriptionCount(int value)
    {
        _buffer.PutInt(_offset + 20, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ApplyStreamIdEncodingOffset()
    {
        return 24;
    }

    public static int ApplyStreamIdEncodingLength()
    {
        return 1;
    }

    public ListRecordingSubscriptionsRequestEncoder ApplyStreamId(BooleanType value)
    {
        _buffer.PutByte(_offset + 24, (byte)value);
        return this;
    }

    public static int StreamIdEncodingOffset()
    {
        return 28;
    }

    public static int StreamIdEncodingLength()
    {
        return 4;
    }

    public static int StreamIdNullValue()
    {
        return -2147483648;
    }

    public static int StreamIdMinValue()
    {
        return -2147483647;
    }

    public static int StreamIdMaxValue()
    {
        return 2147483647;
    }

    public ListRecordingSubscriptionsRequestEncoder StreamId(int value)
    {
        _buffer.PutInt(_offset + 28, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ChannelId()
    {
        return 7;
    }

    public static string ChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ChannelHeaderLength()
    {
        return 4;
    }

    public ListRecordingSubscriptionsRequestEncoder PutChannel(IDirectBuffer src, int srcOffset, int length)
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

    public ListRecordingSubscriptionsRequestEncoder PutChannel(byte[] src, int srcOffset, int length)
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

    public ListRecordingSubscriptionsRequestEncoder Channel(string value)
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
        ListRecordingSubscriptionsRequestDecoder writer = new ListRecordingSubscriptionsRequestDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
