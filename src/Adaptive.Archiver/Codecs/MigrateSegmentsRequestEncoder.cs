/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class MigrateSegmentsRequestEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 57;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 11;

    private MigrateSegmentsRequestEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public MigrateSegmentsRequestEncoder()
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

    public MigrateSegmentsRequestEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public MigrateSegmentsRequestEncoder WrapAndApplyHeader(
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

    public MigrateSegmentsRequestEncoder ControlSessionId(long value)
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

    public MigrateSegmentsRequestEncoder CorrelationId(long value)
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

    public MigrateSegmentsRequestEncoder SrcRecordingId(long value)
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

    public MigrateSegmentsRequestEncoder DstRecordingId(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        MigrateSegmentsRequestDecoder writer = new MigrateSegmentsRequestDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
