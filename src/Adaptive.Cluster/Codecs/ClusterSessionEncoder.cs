/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ClusterSessionEncoder
{
    public const ushort BLOCK_LENGTH = 40;
    public const ushort TEMPLATE_ID = 103;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 8;

    private ClusterSessionEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ClusterSessionEncoder()
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

    public ClusterSessionEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ClusterSessionEncoder WrapAndApplyHeader(
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

    public static int ClusterSessionIdEncodingOffset()
    {
        return 0;
    }

    public static int ClusterSessionIdEncodingLength()
    {
        return 8;
    }

    public static long ClusterSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ClusterSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ClusterSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ClusterSessionEncoder ClusterSessionId(long value)
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

    public ClusterSessionEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int OpenedLogPositionEncodingOffset()
    {
        return 16;
    }

    public static int OpenedLogPositionEncodingLength()
    {
        return 8;
    }

    public static long OpenedLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long OpenedLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long OpenedLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public ClusterSessionEncoder OpenedLogPosition(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int TimeOfLastActivityEncodingOffset()
    {
        return 24;
    }

    public static int TimeOfLastActivityEncodingLength()
    {
        return 8;
    }

    public static long TimeOfLastActivityNullValue()
    {
        return -9223372036854775808L;
    }

    public static long TimeOfLastActivityMinValue()
    {
        return -9223372036854775807L;
    }

    public static long TimeOfLastActivityMaxValue()
    {
        return 9223372036854775807L;
    }

    public ClusterSessionEncoder TimeOfLastActivity(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CloseReasonEncodingOffset()
    {
        return 32;
    }

    public static int CloseReasonEncodingLength()
    {
        return 4;
    }

    public ClusterSessionEncoder CloseReason(CloseReason value)
    {
        _buffer.PutInt(_offset + 32, (int)value, ByteOrder.LittleEndian);
        return this;
    }

    public static int ResponseStreamIdEncodingOffset()
    {
        return 36;
    }

    public static int ResponseStreamIdEncodingLength()
    {
        return 4;
    }

    public static int ResponseStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int ResponseStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int ResponseStreamIdMaxValue()
    {
        return 2147483647;
    }

    public ClusterSessionEncoder ResponseStreamId(int value)
    {
        _buffer.PutInt(_offset + 36, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ResponseChannelId()
    {
        return 7;
    }

    public static string ResponseChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ResponseChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ResponseChannelHeaderLength()
    {
        return 4;
    }

    public ClusterSessionEncoder PutResponseChannel(IDirectBuffer src, int srcOffset, int length)
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

    public ClusterSessionEncoder PutResponseChannel(byte[] src, int srcOffset, int length)
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

    public ClusterSessionEncoder ResponseChannel(string value)
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
        ClusterSessionDecoder writer = new ClusterSessionDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
