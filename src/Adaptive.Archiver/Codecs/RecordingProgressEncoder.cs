/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class RecordingProgressEncoder
{
    public const ushort BLOCK_LENGTH = 24;
    public const ushort TEMPLATE_ID = 102;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 7;

    private RecordingProgressEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public RecordingProgressEncoder()
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

    public RecordingProgressEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RecordingProgressEncoder WrapAndApplyHeader(
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

    public static int RecordingIdEncodingOffset()
    {
        return 0;
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

    public RecordingProgressEncoder RecordingId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StartPositionEncodingOffset()
    {
        return 8;
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

    public RecordingProgressEncoder StartPosition(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int PositionEncodingOffset()
    {
        return 16;
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

    public RecordingProgressEncoder Position(long value)
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
        RecordingProgressDecoder writer = new RecordingProgressDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
