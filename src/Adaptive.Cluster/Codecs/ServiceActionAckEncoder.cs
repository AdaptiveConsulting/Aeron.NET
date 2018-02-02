/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ServiceActionAckEncoder
{
    public const ushort BLOCK_LENGTH = 24;
    public const ushort TEMPLATE_ID = 32;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private ServiceActionAckEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ServiceActionAckEncoder()
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

    public ServiceActionAckEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ServiceActionAckEncoder WrapAndApplyHeader(
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

    public static int LogPositionEncodingOffset()
    {
        return 0;
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

    public ServiceActionAckEncoder LogPosition(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeadershipTermIdEncodingOffset()
    {
        return 8;
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

    public ServiceActionAckEncoder LeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ServiceIdEncodingOffset()
    {
        return 16;
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

    public ServiceActionAckEncoder ServiceId(int value)
    {
        _buffer.PutInt(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ActionEncodingOffset()
    {
        return 20;
    }

    public static int ActionEncodingLength()
    {
        return 4;
    }

    public ServiceActionAckEncoder Action(ClusterAction value)
    {
        _buffer.PutInt(_offset + 20, (int)value, ByteOrder.LittleEndian);
        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        ServiceActionAckDecoder writer = new ServiceActionAckDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
