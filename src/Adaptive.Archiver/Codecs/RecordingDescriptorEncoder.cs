/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class RecordingDescriptorEncoder
{
    public const ushort BLOCK_LENGTH = 80;
    public const ushort TEMPLATE_ID = 22;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 8;

    private RecordingDescriptorEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public RecordingDescriptorEncoder()
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

    public RecordingDescriptorEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RecordingDescriptorEncoder WrapAndApplyHeader(
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

    public RecordingDescriptorEncoder ControlSessionId(long value)
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

    public RecordingDescriptorEncoder CorrelationId(long value)
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

    public RecordingDescriptorEncoder RecordingId(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StartTimestampEncodingOffset()
    {
        return 24;
    }

    public static int StartTimestampEncodingLength()
    {
        return 8;
    }

    public static long StartTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long StartTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long StartTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecordingDescriptorEncoder StartTimestamp(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StopTimestampEncodingOffset()
    {
        return 32;
    }

    public static int StopTimestampEncodingLength()
    {
        return 8;
    }

    public static long StopTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long StopTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long StopTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecordingDescriptorEncoder StopTimestamp(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StartPositionEncodingOffset()
    {
        return 40;
    }

    public static int StartPositionEncodingLength()
    {
        return 8;
    }

    public static long StartPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long StartPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long StartPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecordingDescriptorEncoder StartPosition(long value)
    {
        _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StopPositionEncodingOffset()
    {
        return 48;
    }

    public static int StopPositionEncodingLength()
    {
        return 8;
    }

    public static long StopPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long StopPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long StopPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecordingDescriptorEncoder StopPosition(long value)
    {
        _buffer.PutLong(_offset + 48, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int InitialTermIdEncodingOffset()
    {
        return 56;
    }

    public static int InitialTermIdEncodingLength()
    {
        return 4;
    }

    public static int InitialTermIdNullValue()
    {
        return -2147483648;
    }

    public static int InitialTermIdMinValue()
    {
        return -2147483647;
    }

    public static int InitialTermIdMaxValue()
    {
        return 2147483647;
    }

    public RecordingDescriptorEncoder InitialTermId(int value)
    {
        _buffer.PutInt(_offset + 56, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SegmentFileLengthEncodingOffset()
    {
        return 60;
    }

    public static int SegmentFileLengthEncodingLength()
    {
        return 4;
    }

    public static int SegmentFileLengthNullValue()
    {
        return -2147483648;
    }

    public static int SegmentFileLengthMinValue()
    {
        return -2147483647;
    }

    public static int SegmentFileLengthMaxValue()
    {
        return 2147483647;
    }

    public RecordingDescriptorEncoder SegmentFileLength(int value)
    {
        _buffer.PutInt(_offset + 60, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int TermBufferLengthEncodingOffset()
    {
        return 64;
    }

    public static int TermBufferLengthEncodingLength()
    {
        return 4;
    }

    public static int TermBufferLengthNullValue()
    {
        return -2147483648;
    }

    public static int TermBufferLengthMinValue()
    {
        return -2147483647;
    }

    public static int TermBufferLengthMaxValue()
    {
        return 2147483647;
    }

    public RecordingDescriptorEncoder TermBufferLength(int value)
    {
        _buffer.PutInt(_offset + 64, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int MtuLengthEncodingOffset()
    {
        return 68;
    }

    public static int MtuLengthEncodingLength()
    {
        return 4;
    }

    public static int MtuLengthNullValue()
    {
        return -2147483648;
    }

    public static int MtuLengthMinValue()
    {
        return -2147483647;
    }

    public static int MtuLengthMaxValue()
    {
        return 2147483647;
    }

    public RecordingDescriptorEncoder MtuLength(int value)
    {
        _buffer.PutInt(_offset + 68, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SessionIdEncodingOffset()
    {
        return 72;
    }

    public static int SessionIdEncodingLength()
    {
        return 4;
    }

    public static int SessionIdNullValue()
    {
        return -2147483648;
    }

    public static int SessionIdMinValue()
    {
        return -2147483647;
    }

    public static int SessionIdMaxValue()
    {
        return 2147483647;
    }

    public RecordingDescriptorEncoder SessionId(int value)
    {
        _buffer.PutInt(_offset + 72, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StreamIdEncodingOffset()
    {
        return 76;
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

    public RecordingDescriptorEncoder StreamId(int value)
    {
        _buffer.PutInt(_offset + 76, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StrippedChannelId()
    {
        return 14;
    }

    public static string StrippedChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string StrippedChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int StrippedChannelHeaderLength()
    {
        return 4;
    }

    public RecordingDescriptorEncoder PutStrippedChannel(IDirectBuffer src, int srcOffset, int length)
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

    public RecordingDescriptorEncoder PutStrippedChannel(byte[] src, int srcOffset, int length)
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

    public RecordingDescriptorEncoder StrippedChannel(string value)
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

    public static int OriginalChannelId()
    {
        return 15;
    }

    public static string OriginalChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string OriginalChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int OriginalChannelHeaderLength()
    {
        return 4;
    }

    public RecordingDescriptorEncoder PutOriginalChannel(IDirectBuffer src, int srcOffset, int length)
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

    public RecordingDescriptorEncoder PutOriginalChannel(byte[] src, int srcOffset, int length)
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

    public RecordingDescriptorEncoder OriginalChannel(string value)
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

    public static int SourceIdentityId()
    {
        return 16;
    }

    public static string SourceIdentityCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string SourceIdentityMetaAttribute(MetaAttribute metaAttribute)
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

    public static int SourceIdentityHeaderLength()
    {
        return 4;
    }

    public RecordingDescriptorEncoder PutSourceIdentity(IDirectBuffer src, int srcOffset, int length)
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

    public RecordingDescriptorEncoder PutSourceIdentity(byte[] src, int srcOffset, int length)
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

    public RecordingDescriptorEncoder SourceIdentity(string value)
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
        RecordingDescriptorDecoder writer = new RecordingDescriptorDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
