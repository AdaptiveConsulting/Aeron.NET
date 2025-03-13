/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class ReplicateRequest2Encoder
{
    public const ushort BLOCK_LENGTH = 68;
    public const ushort TEMPLATE_ID = 66;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 8;

    private ReplicateRequest2Encoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ReplicateRequest2Encoder()
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

    public ReplicateRequest2Encoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ReplicateRequest2Encoder WrapAndApplyHeader(
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

    public ReplicateRequest2Encoder ControlSessionId(long value)
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

    public ReplicateRequest2Encoder CorrelationId(long value)
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

    public ReplicateRequest2Encoder SrcRecordingId(long value)
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

    public ReplicateRequest2Encoder DstRecordingId(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StopPositionEncodingOffset()
    {
        return 32;
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

    public ReplicateRequest2Encoder StopPosition(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ChannelTagIdEncodingOffset()
    {
        return 40;
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

    public ReplicateRequest2Encoder ChannelTagId(long value)
    {
        _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SubscriptionTagIdEncodingOffset()
    {
        return 48;
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

    public ReplicateRequest2Encoder SubscriptionTagId(long value)
    {
        _buffer.PutLong(_offset + 48, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SrcControlStreamIdEncodingOffset()
    {
        return 56;
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

    public ReplicateRequest2Encoder SrcControlStreamId(int value)
    {
        _buffer.PutInt(_offset + 56, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int FileIoMaxLengthEncodingOffset()
    {
        return 60;
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

    public ReplicateRequest2Encoder FileIoMaxLength(int value)
    {
        _buffer.PutInt(_offset + 60, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ReplicationSessionIdEncodingOffset()
    {
        return 64;
    }

    public static int ReplicationSessionIdEncodingLength()
    {
        return 4;
    }

    public static int ReplicationSessionIdNullValue()
    {
        return -2147483648;
    }

    public static int ReplicationSessionIdMinValue()
    {
        return -2147483647;
    }

    public static int ReplicationSessionIdMaxValue()
    {
        return 2147483647;
    }

    public ReplicateRequest2Encoder ReplicationSessionId(int value)
    {
        _buffer.PutInt(_offset + 64, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int SrcControlChannelId()
    {
        return 9;
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

    public ReplicateRequest2Encoder PutSrcControlChannel(IDirectBuffer src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder PutSrcControlChannel(byte[] src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder SrcControlChannel(string value)
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
        return 10;
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

    public ReplicateRequest2Encoder PutLiveDestination(IDirectBuffer src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder PutLiveDestination(byte[] src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder LiveDestination(string value)
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

    public static int ReplicationChannelId()
    {
        return 11;
    }

    public static string ReplicationChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ReplicationChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ReplicationChannelHeaderLength()
    {
        return 4;
    }

    public ReplicateRequest2Encoder PutReplicationChannel(IDirectBuffer src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder PutReplicationChannel(byte[] src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder ReplicationChannel(string value)
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

    public static int EncodedCredentialsId()
    {
        return 14;
    }

    public static string EncodedCredentialsMetaAttribute(MetaAttribute metaAttribute)
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

    public static int EncodedCredentialsHeaderLength()
    {
        return 4;
    }

    public ReplicateRequest2Encoder PutEncodedCredentials(IDirectBuffer src, int srcOffset, int length)
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

    public ReplicateRequest2Encoder PutEncodedCredentials(byte[] src, int srcOffset, int length)
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


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        ReplicateRequest2Decoder writer = new ReplicateRequest2Decoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
