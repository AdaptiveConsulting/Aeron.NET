/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using Adaptive.Agrona;

namespace Io.Aeron.Cluster.Codecs {
public class MessageHeaderDecoder
{
    public static int ENCODED_LENGTH = 8;
    private int _offset;
    private IDirectBuffer _buffer;

    public MessageHeaderDecoder Wrap(IDirectBuffer buffer, int offset)
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


    public static int TemplateIdEncodingOffset()
    {
        return 2;
    }

    public static int TemplateIdEncodingLength()
    {
        return 2;
    }

    public static ushort TemplateIdNullValue()
    {
        return 65535;
    }

    public static ushort TemplateIdMinValue()
    {
        return 0;
    }

    public static ushort TemplateIdMaxValue()
    {
        return 65534;
    }

    public ushort TemplateId()
    {
        return unchecked((ushort)_buffer.GetShort(_offset + 2, ByteOrder.LittleEndian));
    }


    public static int SchemaIdEncodingOffset()
    {
        return 4;
    }

    public static int SchemaIdEncodingLength()
    {
        return 2;
    }

    public static ushort SchemaIdNullValue()
    {
        return 65535;
    }

    public static ushort SchemaIdMinValue()
    {
        return 0;
    }

    public static ushort SchemaIdMaxValue()
    {
        return 65534;
    }

    public ushort SchemaId()
    {
        return unchecked((ushort)_buffer.GetShort(_offset + 4, ByteOrder.LittleEndian));
    }


    public static int VersionEncodingOffset()
    {
        return 6;
    }

    public static int VersionEncodingLength()
    {
        return 2;
    }

    public static ushort VersionNullValue()
    {
        return 65535;
    }

    public static ushort VersionMinValue()
    {
        return 0;
    }

    public static ushort VersionMaxValue()
    {
        return 65534;
    }

    public ushort Version()
    {
        return unchecked((ushort)_buffer.GetShort(_offset + 6, ByteOrder.LittleEndian));
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
        //Token{signal=ENCODING, name='templateId', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=2, offset=2, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("TemplateId=");
        builder.Append(TemplateId());
        builder.Append('|');
        //Token{signal=ENCODING, name='schemaId', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=2, offset=4, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("SchemaId=");
        builder.Append(SchemaId());
        builder.Append('|');
        //Token{signal=ENCODING, name='version', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=2, offset=6, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=UINT16, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("Version=");
        builder.Append(Version());
        builder.Append(')');

        return builder;
    }
}
}
