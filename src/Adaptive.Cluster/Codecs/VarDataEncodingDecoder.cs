/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using Adaptive.Agrona;

namespace Adaptive.Cluster.Codecs {
public class VarDataEncodingDecoder
{
    public static int ENCODED_LENGTH = -1;
    private int _offset;
    private IDirectBuffer _buffer;

    public VarDataEncodingDecoder Wrap(IDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;

        return this;
    }

    public IDirectBuffer Buffer()
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

    public uint Length()
    {
        return unchecked((uint)_buffer.GetInt(_offset + 0, ByteOrder.LittleEndian));
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
        builder.Append('(');
        //Token{signal=ENCODING, name='length', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=1073741824, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("Length=");
        builder.Append(Length());
        builder.Append('|');
        //Token{signal=ENCODING, name='varData', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=-1, offset=4, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT8, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append(')');

        return builder;
    }
}
}
