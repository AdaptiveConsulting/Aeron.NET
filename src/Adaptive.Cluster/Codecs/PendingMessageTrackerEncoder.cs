/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class PendingMessageTrackerEncoder
{
    public const ushort BLOCK_LENGTH = 24;
    public const ushort TEMPLATE_ID = 107;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 12;

    private PendingMessageTrackerEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public PendingMessageTrackerEncoder()
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

    public PendingMessageTrackerEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public PendingMessageTrackerEncoder WrapAndApplyHeader(
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

    public static int NextServiceSessionIdEncodingOffset()
    {
        return 0;
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

    public PendingMessageTrackerEncoder NextServiceSessionId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LogServiceSessionIdEncodingOffset()
    {
        return 8;
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

    public PendingMessageTrackerEncoder LogServiceSessionId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int PendingMessageCapacityEncodingOffset()
    {
        return 16;
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

    public PendingMessageTrackerEncoder PendingMessageCapacity(int value)
    {
        _buffer.PutInt(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ServiceIdEncodingOffset()
    {
        return 20;
    }

    public static int ServiceIdEncodingLength()
    {
        return 4;
    }

    public static int ServiceIdNullValue()
    {
        return -2147483648;
    }

    public static int ServiceIdMinValue()
    {
        return -2147483647;
    }

    public static int ServiceIdMaxValue()
    {
        return 2147483647;
    }

    public PendingMessageTrackerEncoder ServiceId(int value)
    {
        _buffer.PutInt(_offset + 20, value, ByteOrder.LittleEndian);
        return this;
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        PendingMessageTrackerDecoder writer = new PendingMessageTrackerDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
