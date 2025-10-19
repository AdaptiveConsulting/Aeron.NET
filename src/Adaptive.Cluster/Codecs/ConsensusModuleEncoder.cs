/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ConsensusModuleEncoder
{
    public const ushort BLOCK_LENGTH = 28;
    public const ushort TEMPLATE_ID = 105;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 14;

    private ConsensusModuleEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ConsensusModuleEncoder()
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

    public ConsensusModuleEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ConsensusModuleEncoder WrapAndApplyHeader(
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

    public static int NextSessionIdEncodingOffset()
    {
        return 0;
    }

    public static int NextSessionIdEncodingLength()
    {
        return 8;
    }

    public static long NextSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ConsensusModuleEncoder NextSessionId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int NextServiceSessionIdEncodingOffset()
    {
        return 8;
    }

    public static int NextServiceSessionIdEncodingLength()
    {
        return 8;
    }

    public static long NextServiceSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextServiceSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextServiceSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ConsensusModuleEncoder NextServiceSessionId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogServiceSessionIdEncodingOffset()
    {
        return 16;
    }

    public static int LogServiceSessionIdEncodingLength()
    {
        return 8;
    }

    public static long LogServiceSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LogServiceSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LogServiceSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public ConsensusModuleEncoder LogServiceSessionId(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int PendingMessageCapacityEncodingOffset()
    {
        return 24;
    }

    public static int PendingMessageCapacityEncodingLength()
    {
        return 4;
    }

    public static int PendingMessageCapacityNullValue()
    {
        return 0;
    }

    public static int PendingMessageCapacityMinValue()
    {
        return -2147483647;
    }

    public static int PendingMessageCapacityMaxValue()
    {
        return 2147483647;
    }

    public ConsensusModuleEncoder PendingMessageCapacity(int value)
    {
        _buffer.PutInt(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        ConsensusModuleDecoder writer = new ConsensusModuleDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
