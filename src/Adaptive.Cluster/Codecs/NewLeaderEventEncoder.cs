/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Io.Aeron.Cluster.Codecs {

public class NewLeaderEventEncoder
{
    public const ushort BLOCK_LENGTH = 44;
    public const ushort TEMPLATE_ID = 6;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 0;

    private NewLeaderEventEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public NewLeaderEventEncoder()
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

    public NewLeaderEventEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public NewLeaderEventEncoder WrapAndApplyHeader(
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

    public NewLeaderEventEncoder ClusterSessionId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastCorrelationIdEncodingOffset()
    {
        return 8;
    }

    public static int LastCorrelationIdEncodingLength()
    {
        return 8;
    }

    public static long LastCorrelationIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastCorrelationIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastCorrelationIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeaderEventEncoder LastCorrelationId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastMessageTimestampEncodingOffset()
    {
        return 16;
    }

    public static int LastMessageTimestampEncodingLength()
    {
        return 8;
    }

    public static long LastMessageTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastMessageTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastMessageTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeaderEventEncoder LastMessageTimestamp(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeadershipTimestampEncodingOffset()
    {
        return 24;
    }

    public static int LeadershipTimestampEncodingLength()
    {
        return 8;
    }

    public static long LeadershipTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LeadershipTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LeadershipTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public NewLeaderEventEncoder LeadershipTimestamp(long value)
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

    public NewLeaderEventEncoder LeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderMemberIdEncodingOffset()
    {
        return 40;
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

    public NewLeaderEventEncoder LeaderMemberId(int value)
    {
        _buffer.PutInt(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int MemberEndpointsId()
    {
        return 7;
    }

    public static string MemberEndpointsCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string MemberEndpointsMetaAttribute(MetaAttribute metaAttribute)
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

    public static int MemberEndpointsHeaderLength()
    {
        return 4;
    }

    public NewLeaderEventEncoder PutMemberEndpoints(IDirectBuffer src, int srcOffset, int length)
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

    public NewLeaderEventEncoder PutMemberEndpoints(byte[] src, int srcOffset, int length)
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

    public NewLeaderEventEncoder MemberEndpoints(string value)
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
        NewLeaderEventDecoder writer = new NewLeaderEventDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
