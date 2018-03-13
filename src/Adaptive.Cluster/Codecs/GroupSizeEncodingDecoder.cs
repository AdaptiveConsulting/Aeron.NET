/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using Adaptive.Agrona;

namespace Adaptive.Cluster.Codecs {
public class GroupSizeEncodingDecoder
{
    public static int ENCODED_LENGTH = 4;
    private int _offset;
    private IDirectBuffer _buffer;

    public GroupSizeEncodingDecoder Wrap(IDirectBuffer buffer, int offset)
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

    public ushort BlockLength()
    {
        return unchecked((ushort)_buffer.GetShort(_offset + 0, ByteOrder.LittleEndian));
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

    public ushort NumInGroup()
    {
        return unchecked((ushort)_buffer.GetShort(_offset + 2, ByteOrder.LittleEndian));
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        builder.Append('(');
        //Token{signal=ENCODING, name='blockLength', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=2, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("BlockLength=");
        builder.Append(BlockLength());
        builder.Append('|');
        //Token{signal=ENCODING, name='numInGroup', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=2, offset=2, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("NumInGroup=");
        builder.Append(NumInGroup());
        builder.Append(')');

        return builder;
    }
}
}
