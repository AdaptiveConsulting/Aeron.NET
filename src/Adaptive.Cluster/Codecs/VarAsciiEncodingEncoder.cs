/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using Adaptive.Agrona;

namespace Io.Aeron.Cluster.Codecs {
public class VarAsciiEncodingEncoder
{
    public static int ENCODED_LENGTH = -1;
    private int _offset;
    private IMutableDirectBuffer _buffer;

    public VarAsciiEncodingEncoder Wrap(IMutableDirectBuffer buffer, int offset)
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

    public static int LengthEncodingOffset()
    {
        return 0;
    }

    public static int LengthEncodingLength()
    {
        return 4;
    }

    public static uint LengthNullValue()
    {
        return 4294967295;
    }

    public static uint LengthMinValue()
    {
        return 0;
    }

    public static uint LengthMaxValue()
    {
        return 1073741824;
    }

    public VarAsciiEncodingEncoder Length(uint value)
    {
        _buffer.PutInt(_offset + 0, unchecked((int)value), ByteOrder.LittleEndian);
        return this;
    }


    public static int VarDataEncodingOffset()
    {
        return 4;
    }

    public static int VarDataEncodingLength()
    {
        return -1;
    }

    public static byte VarDataNullValue()
    {
        return (byte)255;
    }

    public static byte VarDataMinValue()
    {
        return (byte)0;
    }

    public static byte VarDataMaxValue()
    {
        return (byte)254;
    }

    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        VarAsciiEncodingDecoder writer = new VarAsciiEncodingDecoder();
        writer.Wrap(_buffer, _offset);

        return writer.AppendTo(builder);
    }
}
}
