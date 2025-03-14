/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs.Mark {

public class MarkFileHeaderEncoder
{
    public const ushort BLOCK_LENGTH = 128;
    public const ushort TEMPLATE_ID = 200;
    public const ushort SCHEMA_ID = 110;
    public const ushort SCHEMA_VERSION = 2;

    private MarkFileHeaderEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public MarkFileHeaderEncoder()
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

    public MarkFileHeaderEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public MarkFileHeaderEncoder WrapAndApplyHeader(
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

    public static int VersionEncodingOffset()
    {
        return 0;
    }

    public static int VersionEncodingLength()
    {
        return 4;
    }

    public static int VersionNullValue()
    {
        return -2147483648;
    }

    public static int VersionMinValue()
    {
        return -2147483647;
    }

    public static int VersionMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder Version(int value)
    {
        _buffer.PutInt(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ComponentTypeEncodingOffset()
    {
        return 4;
    }

    public static int ComponentTypeEncodingLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder ComponentType(ClusterComponentType value)
    {
        _buffer.PutInt(_offset + 4, (int)value, ByteOrder.LittleEndian);
        return this;
    }

    public static int ActivityTimestampEncodingOffset()
    {
        return 8;
    }

    public static int ActivityTimestampEncodingLength()
    {
        return 8;
    }

    public static long ActivityTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ActivityTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ActivityTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarkFileHeaderEncoder ActivityTimestamp(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int StartTimestampEncodingOffset()
    {
        return 16;
    }

    public static int StartTimestampEncodingLength()
    {
        return 8;
    }

    public static long StartTimestampNullValue()
    {
        return -9223372036854775808L;
    }

    public static long StartTimestampMinValue()
    {
        return -9223372036854775807L;
    }

    public static long StartTimestampMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarkFileHeaderEncoder StartTimestamp(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int PidEncodingOffset()
    {
        return 24;
    }

    public static int PidEncodingLength()
    {
        return 8;
    }

    public static long PidNullValue()
    {
        return -9223372036854775808L;
    }

    public static long PidMinValue()
    {
        return -9223372036854775807L;
    }

    public static long PidMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarkFileHeaderEncoder Pid(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CandidateTermIdEncodingOffset()
    {
        return 32;
    }

    public static int CandidateTermIdEncodingLength()
    {
        return 8;
    }

    public static long CandidateTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CandidateTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CandidateTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public MarkFileHeaderEncoder CandidateTermId(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ArchiveStreamIdEncodingOffset()
    {
        return 40;
    }

    public static int ArchiveStreamIdEncodingLength()
    {
        return 4;
    }

    public static int ArchiveStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int ArchiveStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int ArchiveStreamIdMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder ArchiveStreamId(int value)
    {
        _buffer.PutInt(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ServiceStreamIdEncodingOffset()
    {
        return 44;
    }

    public static int ServiceStreamIdEncodingLength()
    {
        return 4;
    }

    public static int ServiceStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int ServiceStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int ServiceStreamIdMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder ServiceStreamId(int value)
    {
        _buffer.PutInt(_offset + 44, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ConsensusModuleStreamIdEncodingOffset()
    {
        return 48;
    }

    public static int ConsensusModuleStreamIdEncodingLength()
    {
        return 4;
    }

    public static int ConsensusModuleStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int ConsensusModuleStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int ConsensusModuleStreamIdMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder ConsensusModuleStreamId(int value)
    {
        _buffer.PutInt(_offset + 48, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int IngressStreamIdEncodingOffset()
    {
        return 52;
    }

    public static int IngressStreamIdEncodingLength()
    {
        return 4;
    }

    public static int IngressStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int IngressStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int IngressStreamIdMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder IngressStreamId(int value)
    {
        _buffer.PutInt(_offset + 52, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int MemberIdEncodingOffset()
    {
        return 56;
    }

    public static int MemberIdEncodingLength()
    {
        return 4;
    }

    public static int MemberIdNullValue()
    {
        return -2147483648;
    }

    public static int MemberIdMinValue()
    {
        return -2147483647;
    }

    public static int MemberIdMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder MemberId(int value)
    {
        _buffer.PutInt(_offset + 56, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ServiceIdEncodingOffset()
    {
        return 60;
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

    public MarkFileHeaderEncoder ServiceId(int value)
    {
        _buffer.PutInt(_offset + 60, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int HeaderLengthEncodingOffset()
    {
        return 64;
    }

    public static int HeaderLengthEncodingLength()
    {
        return 4;
    }

    public static int HeaderLengthNullValue()
    {
        return -2147483648;
    }

    public static int HeaderLengthMinValue()
    {
        return -2147483647;
    }

    public static int HeaderLengthMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder HeaderLength(int value)
    {
        _buffer.PutInt(_offset + 64, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ErrorBufferLengthEncodingOffset()
    {
        return 68;
    }

    public static int ErrorBufferLengthEncodingLength()
    {
        return 4;
    }

    public static int ErrorBufferLengthNullValue()
    {
        return -2147483648;
    }

    public static int ErrorBufferLengthMinValue()
    {
        return -2147483647;
    }

    public static int ErrorBufferLengthMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder ErrorBufferLength(int value)
    {
        _buffer.PutInt(_offset + 68, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ClusterIdEncodingOffset()
    {
        return 72;
    }

    public static int ClusterIdEncodingLength()
    {
        return 4;
    }

    public static int ClusterIdNullValue()
    {
        return -2147483648;
    }

    public static int ClusterIdMinValue()
    {
        return -2147483647;
    }

    public static int ClusterIdMaxValue()
    {
        return 2147483647;
    }

    public MarkFileHeaderEncoder ClusterId(int value)
    {
        _buffer.PutInt(_offset + 72, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int AeronDirectoryId()
    {
        return 16;
    }

    public static string AeronDirectoryCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string AeronDirectoryMetaAttribute(MetaAttribute metaAttribute)
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

    public static int AeronDirectoryHeaderLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder PutAeronDirectory(IDirectBuffer src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder PutAeronDirectory(byte[] src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder AeronDirectory(string value)
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

    public static int ControlChannelId()
    {
        return 17;
    }

    public static string ControlChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ControlChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ControlChannelHeaderLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder PutControlChannel(IDirectBuffer src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder PutControlChannel(byte[] src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder ControlChannel(string value)
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

    public static int IngressChannelId()
    {
        return 18;
    }

    public static string IngressChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string IngressChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int IngressChannelHeaderLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder PutIngressChannel(IDirectBuffer src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder PutIngressChannel(byte[] src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder IngressChannel(string value)
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

    public static int ServiceNameId()
    {
        return 19;
    }

    public static string ServiceNameCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ServiceNameMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ServiceNameHeaderLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder PutServiceName(IDirectBuffer src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder PutServiceName(byte[] src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder ServiceName(string value)
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

    public static int AuthenticatorId()
    {
        return 20;
    }

    public static string AuthenticatorCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string AuthenticatorMetaAttribute(MetaAttribute metaAttribute)
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

    public static int AuthenticatorHeaderLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder PutAuthenticator(IDirectBuffer src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder PutAuthenticator(byte[] src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder Authenticator(string value)
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

    public static int ServicesClusterDirId()
    {
        return 21;
    }

    public static string ServicesClusterDirCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ServicesClusterDirMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ServicesClusterDirHeaderLength()
    {
        return 4;
    }

    public MarkFileHeaderEncoder PutServicesClusterDir(IDirectBuffer src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder PutServicesClusterDir(byte[] src, int srcOffset, int length)
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

    public MarkFileHeaderEncoder ServicesClusterDir(string value)
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
        MarkFileHeaderDecoder writer = new MarkFileHeaderDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
