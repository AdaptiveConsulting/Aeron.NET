/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using Adaptive.Agrona;

namespace Io.Aeron.Archive.Codecs {
public class MessageHeaderEncoder
{
    public static int ENCODED_LENGTH = 8;
    private int _offset;
    private IMutableDirectBuffer _buffer;

    public MessageHeaderEncoder Wrap(IMutableDirectBuffer buffer, int offset)
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

    public MessageHeaderEncoder BlockLength(ushort value)
    {
        _buffer.PutShort(_offset + 0, unchecked((short)value), ByteOrder.LittleEndian);
        return this;
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

    public MessageHeaderEncoder TemplateId(ushort value)
    {
        _buffer.PutShort(_offset + 2, unchecked((short)value), ByteOrder.LittleEndian);
        return this;
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

    public MessageHeaderEncoder SchemaId(ushort value)
    {
        _buffer.PutShort(_offset + 4, unchecked((short)value), ByteOrder.LittleEndian);
        return this;
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

    public MessageHeaderEncoder Version(ushort value)
    {
        _buffer.PutShort(_offset + 6, unchecked((short)value), ByteOrder.LittleEndian);
        return this;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        MessageHeaderDecoder writer = new MessageHeaderDecoder();
        writer.Wrap(_buffer, _offset);

        return writer.AppendTo(builder);
    }
}
}
