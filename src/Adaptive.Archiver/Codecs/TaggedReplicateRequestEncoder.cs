/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class TaggedReplicateRequestEncoder
{
    public const ushort BLOCK_LENGTH = 52;
    public const ushort TEMPLATE_ID = 62;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 6;

    private TaggedReplicateRequestEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public TaggedReplicateRequestEncoder()
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

    public TaggedReplicateRequestEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public TaggedReplicateRequestEncoder WrapAndApplyHeader(
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

    public TaggedReplicateRequestEncoder ControlSessionId(long value)
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

    public TaggedReplicateRequestEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SrcRecordingIdEncodingOffset()
    {
        return 16;
    }

    public static int SrcRecordingIdEncodingLength()
    {
        return 8;
    }

    public static long SrcRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long SrcRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long SrcRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public TaggedReplicateRequestEncoder SrcRecordingId(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int DstRecordingIdEncodingOffset()
    {
        return 24;
    }

    public static int DstRecordingIdEncodingLength()
    {
        return 8;
    }

    public static long DstRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long DstRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long DstRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public TaggedReplicateRequestEncoder DstRecordingId(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ChannelTagIdEncodingOffset()
    {
        return 32;
    }

    public static int ChannelTagIdEncodingLength()
    {
        return 8;
    }

    public static long ChannelTagIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ChannelTagIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ChannelTagIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public TaggedReplicateRequestEncoder ChannelTagId(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SubscriptionTagIdEncodingOffset()
    {
        return 40;
    }

    public static int SubscriptionTagIdEncodingLength()
    {
        return 8;
    }

    public static long SubscriptionTagIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long SubscriptionTagIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long SubscriptionTagIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public TaggedReplicateRequestEncoder SubscriptionTagId(long value)
    {
        _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SrcControlStreamIdEncodingOffset()
    {
        return 48;
    }

    public static int SrcControlStreamIdEncodingLength()
    {
        return 4;
    }

    public static int SrcControlStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int SrcControlStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int SrcControlStreamIdMaxValue()
    {
        return 2147483647;
    }

    public TaggedReplicateRequestEncoder SrcControlStreamId(int value)
    {
        _buffer.PutInt(_offset + 48, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SrcControlChannelId()
    {
        return 8;
    }

    public static string SrcControlChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string SrcControlChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int SrcControlChannelHeaderLength()
    {
        return 4;
    }

    public TaggedReplicateRequestEncoder PutSrcControlChannel(IDirectBuffer src, int srcOffset, int length)
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

    public TaggedReplicateRequestEncoder PutSrcControlChannel(byte[] src, int srcOffset, int length)
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

    public TaggedReplicateRequestEncoder SrcControlChannel(string value)
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

    public static int LiveDestinationId()
    {
        return 9;
    }

    public static string LiveDestinationCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string LiveDestinationMetaAttribute(MetaAttribute metaAttribute)
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

    public static int LiveDestinationHeaderLength()
    {
        return 4;
    }

    public TaggedReplicateRequestEncoder PutLiveDestination(IDirectBuffer src, int srcOffset, int length)
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

    public TaggedReplicateRequestEncoder PutLiveDestination(byte[] src, int srcOffset, int length)
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

    public TaggedReplicateRequestEncoder LiveDestination(string value)
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
        TaggedReplicateRequestDecoder writer = new TaggedReplicateRequestDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
