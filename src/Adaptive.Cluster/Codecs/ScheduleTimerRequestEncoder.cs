/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ScheduleTimerRequestEncoder
{
    public const ushort BLOCK_LENGTH = 16;
    public const ushort TEMPLATE_ID = 30;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private ScheduleTimerRequestEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ScheduleTimerRequestEncoder()
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

    public ScheduleTimerRequestEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ScheduleTimerRequestEncoder WrapAndApplyHeader(
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

    public ScheduleTimerRequestEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int DeadlineEncodingOffset()
    {
        return 8;
    }

    public static int DeadlineEncodingLength()
    {
        return 8;
    }

    public static long DeadlineNullValue()
    {
        return -9223372036854775808L;
    }

    public static long DeadlineMinValue()
    {
        return -9223372036854775807L;
    }

    public static long DeadlineMaxValue()
    {
        return 9223372036854775807L;
    }

    public ScheduleTimerRequestEncoder Deadline(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        ScheduleTimerRequestDecoder writer = new ScheduleTimerRequestDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
