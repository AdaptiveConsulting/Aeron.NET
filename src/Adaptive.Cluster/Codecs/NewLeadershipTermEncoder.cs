/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class NewLeadershipTermEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 52;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

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

    public static int LastBaseLogPositionEncodingOffset()
    {
        return 0;
    }

    public static int LastBaseLogPositionEncodingLength()
    {
        return 8;
    }

    public static long LastBaseLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastBaseLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastBaseLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder LastBaseLogPosition(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastTermPositionEncodingOffset()
    {
        return 8;
    }

    public static int LastTermPositionEncodingLength()
    {
        return 8;
    }

    public static long LastTermPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastTermPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastTermPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeadershipTermEncoder LastTermPosition(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeadershipTermIdEncodingOffset()
    {
        return 16;
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
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderMemberIdEncodingOffset()
    {
        return 24;
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
        _buffer.PutInt(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogSessionIdEncodingOffset()
    {
        return 28;
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
        _buffer.PutInt(_offset + 28, value, ByteOrder.LittleEndian);
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
