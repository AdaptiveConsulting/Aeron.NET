/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class VoteEncoder
{
    public const ushort BLOCK_LENGTH = 36;
    public const ushort TEMPLATE_ID = 52;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 8;

    private VoteEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public VoteEncoder()
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

    public VoteEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public VoteEncoder WrapAndApplyHeader(
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

    public static int CandidateTermIdEncodingOffset()
    {
        return 0;
    }

    public static int CandidateTermIdEncodingLength()
    {
        return 8;
    }

    public static long CandidateTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CandidateTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CandidateTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public VoteEncoder CandidateTermId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogLeadershipTermIdEncodingOffset()
    {
        return 8;
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

    public VoteEncoder LogLeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogPositionEncodingOffset()
    {
        return 16;
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

    public VoteEncoder LogPosition(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CandidateMemberIdEncodingOffset()
    {
        return 24;
    }

    public static int CandidateMemberIdEncodingLength()
    {
        return 4;
    }

    public static int CandidateMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int CandidateMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int CandidateMemberIdMaxValue()
    {
        return 2147483647;
    }

    public VoteEncoder CandidateMemberId(int value)
    {
        _buffer.PutInt(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int FollowerMemberIdEncodingOffset()
    {
        return 28;
    }

    public static int FollowerMemberIdEncodingLength()
    {
        return 4;
    }

    public static int FollowerMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int FollowerMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int FollowerMemberIdMaxValue()
    {
        return 2147483647;
    }

    public VoteEncoder FollowerMemberId(int value)
    {
        _buffer.PutInt(_offset + 28, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int VoteEncodingOffset()
    {
        return 32;
    }

    public static int VoteEncodingLength()
    {
        return 4;
    }

    public VoteEncoder Vote(BooleanType value)
    {
        _buffer.PutInt(_offset + 32, (int)value, ByteOrder.LittleEndian);
        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        VoteDecoder writer = new VoteDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
