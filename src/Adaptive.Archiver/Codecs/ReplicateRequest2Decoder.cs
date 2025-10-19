/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class ReplicateRequest2Decoder
{
    public const ushort BLOCK_LENGTH = 68;
    public const ushort TEMPLATE_ID = 66;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 13;

    private ReplicateRequest2Decoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public ReplicateRequest2Decoder()
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

    public ReplicateRequest2Decoder Wrap(
        IDirectBuffer buffer, int offset, int actingBlockLength, int actingVersion)
    {
        this._buffer = buffer;
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

    public static int ControlSessionIdId()
    {
        return 1;
    }

    public static int ControlSessionIdSinceVersion()
    {
        return 0;
    }

    public static int ControlSessionIdEncodingOffset()
    {
        return 0;
    }

    public static int ControlSessionIdEncodingLength()
    {
        return 8;
    }

    public static string ControlSessionIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long ControlSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ControlSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ControlSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long ControlSessionId()
    {
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int CorrelationIdId()
    {
        return 2;
    }

    public static int CorrelationIdSinceVersion()
    {
        return 0;
    }

    public static int CorrelationIdEncodingOffset()
    {
        return 8;
    }

    public static int CorrelationIdEncodingLength()
    {
        return 8;
    }

    public static string CorrelationIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long CorrelationIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CorrelationIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CorrelationIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long CorrelationId()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int SrcRecordingIdId()
    {
        return 3;
    }

    public static int SrcRecordingIdSinceVersion()
    {
        return 0;
    }

    public static int SrcRecordingIdEncodingOffset()
    {
        return 16;
    }

    public static int SrcRecordingIdEncodingLength()
    {
        return 8;
    }

    public static string SrcRecordingIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long SrcRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long SrcRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long SrcRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long SrcRecordingId()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int DstRecordingIdId()
    {
        return 4;
    }

    public static int DstRecordingIdSinceVersion()
    {
        return 0;
    }

    public static int DstRecordingIdEncodingOffset()
    {
        return 24;
    }

    public static int DstRecordingIdEncodingLength()
    {
        return 8;
    }

    public static string DstRecordingIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long DstRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long DstRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long DstRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long DstRecordingId()
    {
        return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
    }


    public static int StopPositionId()
    {
        return 5;
    }

    public static int StopPositionSinceVersion()
    {
        return 0;
    }

    public static int StopPositionEncodingOffset()
    {
        return 32;
    }

    public static int StopPositionEncodingLength()
    {
        return 8;
    }

    public static string StopPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long StopPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long StopPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long StopPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long StopPosition()
    {
        return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
    }


    public static int ChannelTagIdId()
    {
        return 6;
    }

    public static int ChannelTagIdSinceVersion()
    {
        return 0;
    }

    public static int ChannelTagIdEncodingOffset()
    {
        return 40;
    }

    public static int ChannelTagIdEncodingLength()
    {
        return 8;
    }

    public static string ChannelTagIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long ChannelTagIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long ChannelTagIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long ChannelTagIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long ChannelTagId()
    {
        return _buffer.GetLong(_offset + 40, ByteOrder.LittleEndian);
    }


    public static int SubscriptionTagIdId()
    {
        return 7;
    }

    public static int SubscriptionTagIdSinceVersion()
    {
        return 0;
    }

    public static int SubscriptionTagIdEncodingOffset()
    {
        return 48;
    }

    public static int SubscriptionTagIdEncodingLength()
    {
        return 8;
    }

    public static string SubscriptionTagIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long SubscriptionTagIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long SubscriptionTagIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long SubscriptionTagIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long SubscriptionTagId()
    {
        return _buffer.GetLong(_offset + 48, ByteOrder.LittleEndian);
    }


    public static int SrcControlStreamIdId()
    {
        return 8;
    }

    public static int SrcControlStreamIdSinceVersion()
    {
        return 0;
    }

    public static int SrcControlStreamIdEncodingOffset()
    {
        return 56;
    }

    public static int SrcControlStreamIdEncodingLength()
    {
        return 4;
    }

    public static string SrcControlStreamIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int SrcControlStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int SrcControlStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int SrcControlStreamIdMaxValue()
    {
        return 2147483647;
    }

    public int SrcControlStreamId()
    {
        return _buffer.GetInt(_offset + 56, ByteOrder.LittleEndian);
    }


    public static int FileIoMaxLengthId()
    {
        return 12;
    }

    public static int FileIoMaxLengthSinceVersion()
    {
        return 7;
    }

    public static int FileIoMaxLengthEncodingOffset()
    {
        return 60;
    }

    public static int FileIoMaxLengthEncodingLength()
    {
        return 4;
    }

    public static string FileIoMaxLengthMetaAttribute(MetaAttribute metaAttribute)
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

    public static int FileIoMaxLengthNullValue()
    {
        return -2147483648;
    }

    public static int FileIoMaxLengthMinValue()
    {
        return -2147483647;
    }

    public static int FileIoMaxLengthMaxValue()
    {
        return 2147483647;
    }

    public int FileIoMaxLength()
    {
        return _buffer.GetInt(_offset + 60, ByteOrder.LittleEndian);
    }


    public static int ReplicationSessionIdId()
    {
        return 13;
    }

    public static int ReplicationSessionIdSinceVersion()
    {
        return 8;
    }

    public static int ReplicationSessionIdEncodingOffset()
    {
        return 64;
    }

    public static int ReplicationSessionIdEncodingLength()
    {
        return 4;
    }

    public static string ReplicationSessionIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ReplicationSessionIdNullValue()
    {
        return -2147483648;
    }

    public static int ReplicationSessionIdMinValue()
    {
        return -2147483647;
    }

    public static int ReplicationSessionIdMaxValue()
    {
        return 2147483647;
    }

    public int ReplicationSessionId()
    {
        return _buffer.GetInt(_offset + 64, ByteOrder.LittleEndian);
    }


    public static int SrcControlChannelId()
    {
        return 9;
    }

    public static int SrcControlChannelSinceVersion()
    {
        return 0;
    }

    public static string SrcControlChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string SrcControlChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int SrcControlChannelHeaderLength()
    {
        return 4;
    }

    public int SrcControlChannelLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetSrcControlChannel(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetSrcControlChannel(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string SrcControlChannel()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int LiveDestinationId()
    {
        return 10;
    }

    public static int LiveDestinationSinceVersion()
    {
        return 0;
    }

    public static string LiveDestinationCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string LiveDestinationMetaAttribute(MetaAttribute metaAttribute)
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

    public static int LiveDestinationHeaderLength()
    {
        return 4;
    }

    public int LiveDestinationLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetLiveDestination(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetLiveDestination(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string LiveDestination()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int ReplicationChannelId()
    {
        return 11;
    }

    public static int ReplicationChannelSinceVersion()
    {
        return 0;
    }

    public static string ReplicationChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ReplicationChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ReplicationChannelHeaderLength()
    {
        return 4;
    }

    public int ReplicationChannelLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetReplicationChannel(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetReplicationChannel(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string ReplicationChannel()
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        _parentMessage.Limit(limit + headerLength + dataLength);
        byte[] tmp = new byte[dataLength];
        _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

        return Encoding.ASCII.GetString(tmp);
    }

    public static int EncodedCredentialsId()
    {
        return 14;
    }

    public static int EncodedCredentialsSinceVersion()
    {
        return 8;
    }

    public static string EncodedCredentialsMetaAttribute(MetaAttribute metaAttribute)
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

    public static int EncodedCredentialsHeaderLength()
    {
        return 4;
    }

    public int EncodedCredentialsLength()
    {
        if (_parentMessage._actingVersion < 8)
        {
            return 0;
        }

        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetEncodedCredentials(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        if (_parentMessage._actingVersion < 8)
        {
            return 0;
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetEncodedCredentials(byte[] dst, int dstOffset, int length)
    {
        if (_parentMessage._actingVersion < 8)
        {
            return 0;
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public static int SrcResponseChannelId()
    {
        return 15;
    }

    public static int SrcResponseChannelSinceVersion()
    {
        return 10;
    }

    public static string SrcResponseChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string SrcResponseChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int SrcResponseChannelHeaderLength()
    {
        return 4;
    }

    public int SrcResponseChannelLength()
    {
        if (_parentMessage._actingVersion < 10)
        {
            return 0;
        }

        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetSrcResponseChannel(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        if (_parentMessage._actingVersion < 10)
        {
            return 0;
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetSrcResponseChannel(byte[] dst, int dstOffset, int length)
    {
        if (_parentMessage._actingVersion < 10)
        {
            return 0;
        }

        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string SrcResponseChannel()
    {
        if (_parentMessage._actingVersion < 10)
        {
            return "";
        }

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
        builder.Append("[ReplicateRequest2](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='controlSessionId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ControlSessionId=");
        builder.Append(ControlSessionId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='correlationId', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CorrelationId=");
        builder.Append(CorrelationId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='srcRecordingId', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("SrcRecordingId=");
        builder.Append(SrcRecordingId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='dstRecordingId', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("DstRecordingId=");
        builder.Append(DstRecordingId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='stopPosition', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("StopPosition=");
        builder.Append(StopPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='channelTagId', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ChannelTagId=");
        builder.Append(ChannelTagId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='subscriptionTagId', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=48, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=48, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("SubscriptionTagId=");
        builder.Append(SubscriptionTagId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='srcControlStreamId', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=0, offset=56, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=56, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("SrcControlStreamId=");
        builder.Append(SrcControlStreamId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='fileIoMaxLength', referencedName='null', description='null', id=12, version=7, deprecated=0, encodedLength=0, offset=60, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=60, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("FileIoMaxLength=");
        builder.Append(FileIoMaxLength());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='replicationSessionId', referencedName='null', description='null', id=13, version=8, deprecated=0, encodedLength=0, offset=64, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=64, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ReplicationSessionId=");
        builder.Append(ReplicationSessionId());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='srcControlChannel', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=68, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("SrcControlChannel=");
        builder.Append(SrcControlChannel());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='liveDestination', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LiveDestination=");
        builder.Append(LiveDestination());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='replicationChannel', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ReplicationChannel=");
        builder.Append(ReplicationChannel());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='encodedCredentials', referencedName='null', description='null', id=14, version=8, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("EncodedCredentials=");
        builder.Append(EncodedCredentialsLength() + " raw bytes");
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='srcResponseChannel', referencedName='null', description='null', id=15, version=10, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("SrcResponseChannel=");
        builder.Append(SrcResponseChannel());

        Limit(originalLimit);

        return builder;
    }
}
}
