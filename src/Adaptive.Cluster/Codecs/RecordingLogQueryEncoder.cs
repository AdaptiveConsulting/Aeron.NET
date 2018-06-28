/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RecordingLogQueryEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 63;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private RecordingLogQueryEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public RecordingLogQueryEncoder()
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

    public RecordingLogQueryEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RecordingLogQueryEncoder WrapAndApplyHeader(
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

    public RecordingLogQueryEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int RequestMemberIdEncodingOffset()
    {
        return 8;
    }

    public static int RequestMemberIdEncodingLength()
    {
        return 4;
    }

    public static int RequestMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int RequestMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int RequestMemberIdMaxValue()
    {
        return 2147483647;
    }

    public RecordingLogQueryEncoder RequestMemberId(int value)
    {
        _buffer.PutInt(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderMemberIdEncodingOffset()
    {
        return 12;
    }

    public static int LeaderMemberIdEncodingLength()
    {
        return 4;
    }

    public static int LeaderMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int LeaderMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int LeaderMemberIdMaxValue()
    {
        return 2147483647;
    }

    public RecordingLogQueryEncoder LeaderMemberId(int value)
    {
        _buffer.PutInt(_offset + 12, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int FromLeadershipTermIdEncodingOffset()
    {
        return 16;
    }

    public static int FromLeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static long FromLeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long FromLeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long FromLeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecordingLogQueryEncoder FromLeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CountEncodingOffset()
    {
        return 24;
    }

    public static int CountEncodingLength()
    {
        return 4;
    }

    public static int CountNullValue()
    {
        return -2147483648;
    }

    public static int CountMinValue()
    {
        return -2147483647;
    }

    public static int CountMaxValue()
    {
        return 2147483647;
    }

    public RecordingLogQueryEncoder Count(int value)
    {
        _buffer.PutInt(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int IncludeSnapshotsEncodingOffset()
    {
        return 28;
    }

    public static int IncludeSnapshotsEncodingLength()
    {
        return 4;
    }

    public RecordingLogQueryEncoder IncludeSnapshots(BooleanType value)
    {
        _buffer.PutInt(_offset + 28, (int)value, ByteOrder.LittleEndian);
        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        RecordingLogQueryDecoder writer = new RecordingLogQueryDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
