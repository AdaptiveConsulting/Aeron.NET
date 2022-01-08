/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class NewLeadershipTermEncoder
{
    public const ushort BLOCK_LENGTH = 88;
    public const ushort TEMPLATE_ID = 53;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 7;

    private NewLeadershipTermEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public NewLeadershipTermEncoder()
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

    public NewLeadershipTermEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public NewLeadershipTermEncoder WrapAndApplyHeader(
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

    public static int LogLeadershipTermIdEncodingOffset()
    {
        return 0;
    }

    public static int LogLeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static long LogLeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LogLeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LogLeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder LogLeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int NextLeadershipTermIdEncodingOffset()
    {
        return 8;
    }

    public static int NextLeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static long NextLeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextLeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextLeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder NextLeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int NextTermBaseLogPositionEncodingOffset()
    {
        return 16;
    }

    public static int NextTermBaseLogPositionEncodingLength()
    {
        return 8;
    }

    public static long NextTermBaseLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextTermBaseLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextTermBaseLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder NextTermBaseLogPosition(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int NextLogPositionEncodingOffset()
    {
        return 24;
    }

    public static int NextLogPositionEncodingLength()
    {
        return 8;
    }

    public static long NextLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder NextLogPosition(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeadershipTermIdEncodingOffset()
    {
        return 32;
    }

    public static int LeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static long LeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder LeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int TermBaseLogPositionEncodingOffset()
    {
        return 40;
    }

    public static int TermBaseLogPositionEncodingLength()
    {
        return 8;
    }

    public static long TermBaseLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long TermBaseLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long TermBaseLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder TermBaseLogPosition(long value)
    {
        _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogPositionEncodingOffset()
    {
        return 48;
    }

    public static int LogPositionEncodingLength()
    {
        return 8;
    }

    public static long LogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder LogPosition(long value)
    {
        _buffer.PutLong(_offset + 48, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderRecordingIdEncodingOffset()
    {
        return 56;
    }

    public static int LeaderRecordingIdEncodingLength()
    {
        return 8;
    }

    public static long LeaderRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LeaderRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LeaderRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder LeaderRecordingId(long value)
    {
        _buffer.PutLong(_offset + 56, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int TimestampEncodingOffset()
    {
        return 64;
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

    public NewLeadershipTermEncoder Timestamp(long value)
    {
        _buffer.PutLong(_offset + 64, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderMemberIdEncodingOffset()
    {
        return 72;
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

    public NewLeadershipTermEncoder LeaderMemberId(int value)
    {
        _buffer.PutInt(_offset + 72, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogSessionIdEncodingOffset()
    {
        return 76;
    }

    public static int LogSessionIdEncodingLength()
    {
        return 4;
    }

    public static int LogSessionIdNullValue()
    {
        return -2147483648;
    }

    public static int LogSessionIdMinValue()
    {
        return -2147483647;
    }

    public static int LogSessionIdMaxValue()
    {
        return 2147483647;
    }

    public NewLeadershipTermEncoder LogSessionId(int value)
    {
        _buffer.PutInt(_offset + 76, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int AppVersionEncodingOffset()
    {
        return 80;
    }

    public static int AppVersionEncodingLength()
    {
        return 4;
    }

    public static int AppVersionNullValue()
    {
        return 0;
    }

    public static int AppVersionMinValue()
    {
        return 1;
    }

    public static int AppVersionMaxValue()
    {
        return 16777215;
    }

    public NewLeadershipTermEncoder AppVersion(int value)
    {
        _buffer.PutInt(_offset + 80, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int IsStartupEncodingOffset()
    {
        return 84;
    }

    public static int IsStartupEncodingLength()
    {
        return 4;
    }

    public NewLeadershipTermEncoder IsStartup(BooleanType value)
    {
        _buffer.PutInt(_offset + 84, (int)value, ByteOrder.LittleEndian);
        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        NewLeadershipTermDecoder writer = new NewLeadershipTermDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
