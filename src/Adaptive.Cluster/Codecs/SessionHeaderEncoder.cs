/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class SessionHeaderEncoder
{
    public const ushort BLOCK_LENGTH = 24;
    public const ushort TEMPLATE_ID = 20;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private SessionHeaderEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public SessionHeaderEncoder()
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

    public SessionHeaderEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public SessionHeaderEncoder WrapAndApplyHeader(
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

    public static int CorrelationIdEncodingOffset()
    {
        return 0;
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

    public SessionHeaderEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ClusterSessionIdEncodingOffset()
    {
        return 8;
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

    public SessionHeaderEncoder ClusterSessionId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int TimestampEncodingOffset()
    {
        return 16;
    }

    public static int TimestampEncodingLength()
    {
        return 8;
    }

    public static long TimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long TimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long TimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public SessionHeaderEncoder Timestamp(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        SessionHeaderDecoder writer = new SessionHeaderDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
