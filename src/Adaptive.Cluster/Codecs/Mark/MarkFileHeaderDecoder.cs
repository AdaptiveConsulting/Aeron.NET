/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs.Mark {

public class MarkFileHeaderDecoder
{
    public const ushort BLOCK_LENGTH = 128;
    public const ushort TEMPLATE_ID = 200;
    public const ushort SCHEMA_ID = 110;
    public const ushort SCHEMA_VERSION = 0;

    private MarkFileHeaderDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _initialOffset;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public MarkFileHeaderDecoder()
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

    public IDirectBuffer Buffer()
    {
        return _buffer;
    }

    public int Offset()
    {
        return _offset;
    }
    
    public int InitialOffset()
    {
        return _initialOffset;
    }

    public MarkFileHeaderDecoder Wrap(
        IDirectBuffer buffer, int offset, int actingBlockLength, int actingVersion)
    {
        this._buffer = buffer;
        this._initialOffset = offset;
        this._offset = offset;
        this._actingBlockLength = actingBlockLength;
        this._actingVersion = actingVersion;
        Limit(offset + actingBlockLength);

        return this;
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

    public static int VersionId()
    {
        return 1;
    }

    public static int VersionSinceVersion()
    {
        return 0;
    }

    public static int VersionEncodingOffset()
    {
        return 0;
    }

    public static int VersionEncodingLength()
    {
        return 4;
    }

    public static string VersionMetaAttribute(MetaAttribute metaAttribute)
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

    public int Version()
    {
        return _buffer.GetInt(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int ComponentTypeId()
    {
        return 2;
    }

    public static int ComponentTypeSinceVersion()
    {
        return 0;
    }

    public static int ComponentTypeEncodingOffset()
    {
        return 4;
    }

    public static int ComponentTypeEncodingLength()
    {
        return 4;
    }

    public static string ComponentTypeMetaAttribute(MetaAttribute metaAttribute)
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

    public ClusterComponentType ComponentType()
    {
        return (ClusterComponentType)_buffer.GetInt(_offset + 4, ByteOrder.LittleEndian);
    }


    public static int ActivityTimestampId()
    {
        return 3;
    }

    public static int ActivityTimestampSinceVersion()
    {
        return 0;
    }

    public static int ActivityTimestampEncodingOffset()
    {
        return 8;
    }

    public static int ActivityTimestampEncodingLength()
    {
        return 8;
    }

    public static string ActivityTimestampMetaAttribute(MetaAttribute metaAttribute)
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

    public long ActivityTimestamp()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int StartTimestampId()
    {
        return 4;
    }

    public static int StartTimestampSinceVersion()
    {
        return 0;
    }

    public static int StartTimestampEncodingOffset()
    {
        return 16;
    }

    public static int StartTimestampEncodingLength()
    {
        return 8;
    }

    public static string StartTimestampMetaAttribute(MetaAttribute metaAttribute)
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

    public long StartTimestamp()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int PidId()
    {
        return 5;
    }

    public static int PidSinceVersion()
    {
        return 0;
    }

    public static int PidEncodingOffset()
    {
        return 24;
    }

    public static int PidEncodingLength()
    {
        return 8;
    }

    public static string PidMetaAttribute(MetaAttribute metaAttribute)
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

    public long Pid()
    {
        return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
    }


    public static int CandidateTermIdId()
    {
        return 6;
    }

    public static int CandidateTermIdSinceVersion()
    {
        return 0;
    }

    public static int CandidateTermIdEncodingOffset()
    {
        return 32;
    }

    public static int CandidateTermIdEncodingLength()
    {
        return 8;
    }

    public static string CandidateTermIdMetaAttribute(MetaAttribute metaAttribute)
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

    public long CandidateTermId()
    {
        return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
    }


    public static int ArchiveStreamIdId()
    {
        return 7;
    }

    public static int ArchiveStreamIdSinceVersion()
    {
        return 0;
    }

    public static int ArchiveStreamIdEncodingOffset()
    {
        return 40;
    }

    public static int ArchiveStreamIdEncodingLength()
    {
        return 4;
    }

    public static string ArchiveStreamIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int ArchiveStreamId()
    {
        return _buffer.GetInt(_offset + 40, ByteOrder.LittleEndian);
    }


    public static int ServiceStreamIdId()
    {
        return 8;
    }

    public static int ServiceStreamIdSinceVersion()
    {
        return 0;
    }

    public static int ServiceStreamIdEncodingOffset()
    {
        return 44;
    }

    public static int ServiceStreamIdEncodingLength()
    {
        return 4;
    }

    public static string ServiceStreamIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int ServiceStreamId()
    {
        return _buffer.GetInt(_offset + 44, ByteOrder.LittleEndian);
    }


    public static int ConsensusModuleStreamIdId()
    {
        return 9;
    }

    public static int ConsensusModuleStreamIdSinceVersion()
    {
        return 0;
    }

    public static int ConsensusModuleStreamIdEncodingOffset()
    {
        return 48;
    }

    public static int ConsensusModuleStreamIdEncodingLength()
    {
        return 4;
    }

    public static string ConsensusModuleStreamIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int ConsensusModuleStreamId()
    {
        return _buffer.GetInt(_offset + 48, ByteOrder.LittleEndian);
    }


    public static int IngressStreamIdId()
    {
        return 10;
    }

    public static int IngressStreamIdSinceVersion()
    {
        return 0;
    }

    public static int IngressStreamIdEncodingOffset()
    {
        return 52;
    }

    public static int IngressStreamIdEncodingLength()
    {
        return 4;
    }

    public static string IngressStreamIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int IngressStreamId()
    {
        return _buffer.GetInt(_offset + 52, ByteOrder.LittleEndian);
    }


    public static int MemberIdId()
    {
        return 11;
    }

    public static int MemberIdSinceVersion()
    {
        return 0;
    }

    public static int MemberIdEncodingOffset()
    {
        return 56;
    }

    public static int MemberIdEncodingLength()
    {
        return 4;
    }

    public static string MemberIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int MemberId()
    {
        return _buffer.GetInt(_offset + 56, ByteOrder.LittleEndian);
    }


    public static int ServiceIdId()
    {
        return 12;
    }

    public static int ServiceIdSinceVersion()
    {
        return 0;
    }

    public static int ServiceIdEncodingOffset()
    {
        return 60;
    }

    public static int ServiceIdEncodingLength()
    {
        return 4;
    }

    public static string ServiceIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int ServiceId()
    {
        return _buffer.GetInt(_offset + 60, ByteOrder.LittleEndian);
    }


    public static int HeaderLengthId()
    {
        return 13;
    }

    public static int HeaderLengthSinceVersion()
    {
        return 0;
    }

    public static int HeaderLengthEncodingOffset()
    {
        return 64;
    }

    public static int HeaderLengthEncodingLength()
    {
        return 4;
    }

    public static string HeaderLengthMetaAttribute(MetaAttribute metaAttribute)
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

    public int HeaderLength()
    {
        return _buffer.GetInt(_offset + 64, ByteOrder.LittleEndian);
    }


    public static int ErrorBufferLengthId()
    {
        return 14;
    }

    public static int ErrorBufferLengthSinceVersion()
    {
        return 0;
    }

    public static int ErrorBufferLengthEncodingOffset()
    {
        return 68;
    }

    public static int ErrorBufferLengthEncodingLength()
    {
        return 4;
    }

    public static string ErrorBufferLengthMetaAttribute(MetaAttribute metaAttribute)
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

    public int ErrorBufferLength()
    {
        return _buffer.GetInt(_offset + 68, ByteOrder.LittleEndian);
    }


    public static int ClusterIdId()
    {
        return 15;
    }

    public static int ClusterIdSinceVersion()
    {
        return 0;
    }

    public static int ClusterIdEncodingOffset()
    {
        return 72;
    }

    public static int ClusterIdEncodingLength()
    {
        return 4;
    }

    public static string ClusterIdMetaAttribute(MetaAttribute metaAttribute)
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

    public int ClusterId()
    {
        return _buffer.GetInt(_offset + 72, ByteOrder.LittleEndian);
    }


    public static int AeronDirectoryId()
    {
        return 16;
    }

    public static int AeronDirectorySinceVersion()
    {
        return 0;
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

    public int AeronDirectoryLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetAeronDirectory(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetAeronDirectory(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string AeronDirectory()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int ControlChannelId()
    {
        return 17;
    }

    public static int ControlChannelSinceVersion()
    {
        return 0;
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

    public int ControlChannelLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetControlChannel(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetControlChannel(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string ControlChannel()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int IngressChannelId()
    {
        return 18;
    }

    public static int IngressChannelSinceVersion()
    {
        return 0;
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

    public int IngressChannelLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetIngressChannel(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetIngressChannel(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string IngressChannel()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int ServiceNameId()
    {
        return 19;
    }

    public static int ServiceNameSinceVersion()
    {
        return 0;
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

    public int ServiceNameLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetServiceName(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetServiceName(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string ServiceName()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int AuthenticatorId()
    {
        return 20;
    }

    public static int AuthenticatorSinceVersion()
    {
        return 0;
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

    public int AuthenticatorLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetAuthenticator(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetAuthenticator(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string Authenticator()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[MarkFileHeader](sbeTemplateId=");
        builder.Append(TEMPLATE_ID);
        builder.Append("|sbeSchemaId=");
        builder.Append(SCHEMA_ID);
        builder.Append("|sbeSchemaVersion=");
        if (_parentMessage._actingVersion != SCHEMA_VERSION)
        {
            builder.Append(_parentMessage._actingVersion);
            builder.Append('/');
        }
        builder.Append(SCHEMA_VERSION);
        builder.Append("|sbeBlockLength=");
        if (_actingBlockLength != BLOCK_LENGTH)
        {
            builder.Append(_actingBlockLength);
            builder.Append('/');
        }
        builder.Append(BLOCK_LENGTH);
        builder.Append("):");
        //Token{signal=BEGIN_FIELD, name='version', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Version=");
        builder.Append(Version());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='componentType', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=4, componentTokenCount=8, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=BEGIN_ENUM, name='ClusterComponentType', referencedName='null', description='Type of Cluster Component', id=-1, version=0, deprecated=0, encodedLength=4, offset=4, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("ComponentType=");
        builder.Append(ComponentType());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='activityTimestamp', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time in milliseconds since 1 Jan 1970 UTC.', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ActivityTimestamp=");
        builder.Append(ActivityTimestamp());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='startTimestamp', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time in milliseconds since 1 Jan 1970 UTC.', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("StartTimestamp=");
        builder.Append(StartTimestamp());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='pid', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Pid=");
        builder.Append(Pid());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='candidateTermId', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CandidateTermId=");
        builder.Append(CandidateTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='archiveStreamId', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ArchiveStreamId=");
        builder.Append(ArchiveStreamId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='serviceStreamId', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=0, offset=44, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=44, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ServiceStreamId=");
        builder.Append(ServiceStreamId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='consensusModuleStreamId', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=48, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=48, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ConsensusModuleStreamId=");
        builder.Append(ConsensusModuleStreamId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='ingressStreamId', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=52, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=52, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("IngressStreamId=");
        builder.Append(IngressStreamId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='memberId', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=56, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=56, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("MemberId=");
        builder.Append(MemberId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='serviceId', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=60, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=60, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ServiceId=");
        builder.Append(ServiceId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='headerLength', referencedName='null', description='null', id=13, version=0, deprecated=0, encodedLength=0, offset=64, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=64, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("HeaderLength=");
        builder.Append(HeaderLength());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='errorBufferLength', referencedName='null', description='null', id=14, version=0, deprecated=0, encodedLength=0, offset=68, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=68, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ErrorBufferLength=");
        builder.Append(ErrorBufferLength());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='clusterId', referencedName='null', description='null', id=15, version=0, deprecated=0, encodedLength=0, offset=72, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=72, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ClusterId=");
        builder.Append(ClusterId());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='aeronDirectory', referencedName='null', description='null', id=16, version=0, deprecated=0, encodedLength=0, offset=128, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("AeronDirectory=");
        builder.Append(AeronDirectory());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='controlChannel', referencedName='null', description='null', id=17, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ControlChannel=");
        builder.Append(ControlChannel());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='ingressChannel', referencedName='null', description='null', id=18, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("IngressChannel=");
        builder.Append(IngressChannel());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='serviceName', referencedName='null', description='null', id=19, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ServiceName=");
        builder.Append(ServiceName());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='authenticator', referencedName='null', description='null', id=20, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Authenticator=");
        builder.Append(Authenticator());

        Limit(originalLimit);

        return builder;
    }
}
}
