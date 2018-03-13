/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using Adaptive.Agrona;

namespace Adaptive.Cluster.Codecs {
public class GroupSizeEncodingEncoder
{
    public static int ENCODED_LENGTH = 4;
    private int _offset;
    private IMutableDirectBuffer _buffer;

    public GroupSizeEncodingEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;

        return this;
    }

    public IMutableDirectBuffer Buffer()
    {
        return _buffer;
    }

    public int Offset()
    {
        return _offset;
    }

    public int EncodedLength()
    {
        return ENCODED_LENGTH;
    }

    public static int BlockLengthEncodingOffset()
    {
        return 0;
    }

    public static int BlockLengthEncodingLength()
    {
        return 2;
    }

    public static ushort BlockLengthNullValue()
    {
        return 65535;
    }

    public static ushort BlockLengthMinValue()
    {
        return 0;
    }

    public static ushort BlockLengthMaxValue()
    {
        return 65534;
    }

    public GroupSizeEncodingEncoder BlockLength(ushort value)
    {
        _buffer.PutShort(_offset + 0, unchecked((short)value), ByteOrder.LittleEndian);
        return this;
    }


    public static int NumInGroupEncodingOffset()
    {
        return 2;
    }

    public static int NumInGroupEncodingLength()
    {
        return 2;
    }

    public static ushort NumInGroupNullValue()
    {
        return 65535;
    }

    public static ushort NumInGroupMinValue()
    {
        return 0;
    }

    public static ushort NumInGroupMaxValue()
    {
        return 65534;
    }

    public GroupSizeEncodingEncoder NumInGroup(ushort value)
    {
        _buffer.PutShort(_offset + 2, unchecked((short)value), ByteOrder.LittleEndian);
        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        GroupSizeEncodingDecoder writer = new GroupSizeEncodingDecoder();
        writer.Wrap(_buffer, _offset);

        return writer.AppendTo(builder);
    }
}
}
