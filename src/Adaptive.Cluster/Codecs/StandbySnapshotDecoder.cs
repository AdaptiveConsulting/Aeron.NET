/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class StandbySnapshotDecoder
{
    public const ushort BLOCK_LENGTH = 16;
    public const ushort TEMPLATE_ID = 81;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 13;

    private StandbySnapshotDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public StandbySnapshotDecoder()
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

    public StandbySnapshotDecoder Wrap(
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

    public static int CorrelationIdId()
    {
        return 1;
    }

    public static int CorrelationIdSinceVersion()
    {
        return 0;
    }

    public static int CorrelationIdEncodingOffset()
    {
        return 0;
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
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int VersionId()
    {
        return 2;
    }

    public static int VersionSinceVersion()
    {
        return 0;
    }

    public static int VersionEncodingOffset()
    {
        return 8;
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
        return _buffer.GetInt(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int ResponseStreamIdId()
    {
        return 3;
    }

    public static int ResponseStreamIdSinceVersion()
    {
        return 0;
    }

    public static int ResponseStreamIdEncodingOffset()
    {
        return 12;
    }

    public static int ResponseStreamIdEncodingLength()
    {
        return 4;
    }

    public static string ResponseStreamIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ResponseStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int ResponseStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int ResponseStreamIdMaxValue()
    {
        return 2147483647;
    }

    public int ResponseStreamId()
    {
        return _buffer.GetInt(_offset + 12, ByteOrder.LittleEndian);
    }


    private SnapshotsDecoder _Snapshots = new SnapshotsDecoder();

    public static long SnapshotsDecoderId()
    {
        return 4;
    }

    public static int SnapshotsDecoderSinceVersion()
    {
        return 0;
    }

    public SnapshotsDecoder Snapshots()
    {
        _Snapshots.Wrap(_parentMessage, _buffer);
        return _Snapshots;
    }

    public class SnapshotsDecoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingDecoder _dimensions = new GroupSizeEncodingDecoder();
        private StandbySnapshotDecoder _parentMessage;
        private IDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;
        private int _blockLength;

        public void Wrap(
            StandbySnapshotDecoder parentMessage, IDirectBuffer buffer)
        {
            this._parentMessage = parentMessage;
            this._buffer = buffer;
            _dimensions.Wrap(buffer, parentMessage.Limit());
            _blockLength = _dimensions.BlockLength();
            _count = _dimensions.NumInGroup();
            _index = -1;
            parentMessage.Limit(parentMessage.Limit() + HEADER_SIZE);
        }

        public static int SbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int SbeBlockLength()
        {
            return 44;
        }

        public int ActingBlockLength()
        {
            return _blockLength;
        }

        public int Count()
        {
            return _count;
        }

        public bool HasNext()
        {
            return (_index + 1) < _count;
        }

        public SnapshotsDecoder Next()
        {
            if (_index + 1 >= _count)
            {
                throw new IndexOutOfRangeException();
            }

            _offset = _parentMessage.Limit();
            _parentMessage.Limit(_offset + _blockLength);
            ++_index;

            return this;
        }

        public static int RecordingIdId()
        {
            return 5;
        }

        public static int RecordingIdSinceVersion()
        {
            return 0;
        }

        public static int RecordingIdEncodingOffset()
        {
            return 0;
        }

        public static int RecordingIdEncodingLength()
        {
            return 8;
        }

        public static string RecordingIdMetaAttribute(MetaAttribute metaAttribute)
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

        public static long RecordingIdNullValue()
        {
            return -9223372036854775808L;
        }

        public static long RecordingIdMinValue()
        {
            return -9223372036854775807L;
        }

        public static long RecordingIdMaxValue()
        {
            return 9223372036854775807L;
        }

        public long RecordingId()
        {
            return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
        }


        public static int LeadershipTermIdId()
        {
            return 6;
        }

        public static int LeadershipTermIdSinceVersion()
        {
            return 0;
        }

        public static int LeadershipTermIdEncodingOffset()
        {
            return 8;
        }

        public static int LeadershipTermIdEncodingLength()
        {
            return 8;
        }

        public static string LeadershipTermIdMetaAttribute(MetaAttribute metaAttribute)
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

        public long LeadershipTermId()
        {
            return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
        }


        public static int TermBaseLogPositionId()
        {
            return 7;
        }

        public static int TermBaseLogPositionSinceVersion()
        {
            return 0;
        }

        public static int TermBaseLogPositionEncodingOffset()
        {
            return 16;
        }

        public static int TermBaseLogPositionEncodingLength()
        {
            return 8;
        }

        public static string TermBaseLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

        public static long TermBaseLogPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long TermBaseLogPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long TermBaseLogPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public long TermBaseLogPosition()
        {
            return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
        }


        public static int LogPositionId()
        {
            return 8;
        }

        public static int LogPositionSinceVersion()
        {
            return 0;
        }

        public static int LogPositionEncodingOffset()
        {
            return 24;
        }

        public static int LogPositionEncodingLength()
        {
            return 8;
        }

        public static string LogPositionMetaAttribute(MetaAttribute metaAttribute)
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

        public long LogPosition()
        {
            return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
        }


        public static int TimestampId()
        {
            return 9;
        }

        public static int TimestampSinceVersion()
        {
            return 0;
        }

        public static int TimestampEncodingOffset()
        {
            return 32;
        }

        public static int TimestampEncodingLength()
        {
            return 8;
        }

        public static string TimestampMetaAttribute(MetaAttribute metaAttribute)
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

        public static long TimestampNullValue()
        {
            return -9223372036854775808L;
        }

        public static long TimestampMinValue()
        {
            return -9223372036854775807L;
        }

        public static long TimestampMaxValue()
        {
            return 9223372036854775807L;
        }

        public long Timestamp()
        {
            return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
        }


        public static int ServiceIdId()
        {
            return 10;
        }

        public static int ServiceIdSinceVersion()
        {
            return 0;
        }

        public static int ServiceIdEncodingOffset()
        {
            return 40;
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
            return _buffer.GetInt(_offset + 40, ByteOrder.LittleEndian);
        }


        public static int ArchiveEndpointId()
        {
            return 11;
        }

        public static int ArchiveEndpointSinceVersion()
        {
            return 0;
        }

        public static string ArchiveEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string ArchiveEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int ArchiveEndpointHeaderLength()
        {
            return 4;
        }

        public int ArchiveEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetArchiveEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetArchiveEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string ArchiveEndpoint()
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
            builder.Append('(');
            //Token{signal=BEGIN_FIELD, name='recordingId', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("RecordingId=");
            builder.Append(RecordingId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LeadershipTermId=");
            builder.Append(LeadershipTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='termBaseLogPosition', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TermBaseLogPosition=");
            builder.Append(TermBaseLogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogPosition=");
            builder.Append(LogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='timestamp', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("Timestamp=");
            builder.Append(Timestamp());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='serviceId', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ServiceId=");
            builder.Append(ServiceId());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='archiveEndpoint', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=44, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ArchiveEndpoint=");
            builder.Append(ArchiveEndpoint());
            builder.Append(')');
            return builder;
        }
    }

    public static int ResponseChannelId()
    {
        return 12;
    }

    public static int ResponseChannelSinceVersion()
    {
        return 0;
    }

    public static string ResponseChannelCharacterEncoding()
    {
        return "US-ASCII";
    }

    public static string ResponseChannelMetaAttribute(MetaAttribute metaAttribute)
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

    public static int ResponseChannelHeaderLength()
    {
        return 4;
    }

    public int ResponseChannelLength()
    {
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetResponseChannel(IMutableDirectBuffer dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int GetResponseChannel(byte[] dst, int dstOffset, int length)
    {
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public string ResponseChannel()
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
        return 13;
    }

    public static int EncodedCredentialsSinceVersion()
    {
        return 0;
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
        int limit = _parentMessage.Limit();
        return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
    }

    public int GetEncodedCredentials(IMutableDirectBuffer dst, int dstOffset, int length)
    {
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
        int headerLength = 4;
        int limit = _parentMessage.Limit();
        int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        int bytesCopied = Math.Min(length, dataLength);
        _parentMessage.Limit(limit + headerLength + dataLength);
        _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[StandbySnapshot](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='correlationId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CorrelationId=");
        builder.Append(CorrelationId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='version', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Version=");
        builder.Append(Version());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='responseStreamId', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=12, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=12, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ResponseStreamId=");
        builder.Append(ResponseStreamId());
        builder.Append('|');
        //Token{signal=BEGIN_GROUP, name='snapshots', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=44, offset=16, componentTokenCount=30, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("Snapshots=[");
        SnapshotsDecoder Snapshots = this.Snapshots();
        if (Snapshots.Count() > 0)
        {
            while (Snapshots.HasNext())
            {
                Snapshots.Next().AppendTo(builder);
                builder.Append(',');
            }
            builder.Length = builder.Length - 1;
        }
        builder.Append(']');
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='responseChannel', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("ResponseChannel=");
        builder.Append(ResponseChannel());
        builder.Append('|');
        //Token{signal=BEGIN_VAR_DATA, name='encodedCredentials', referencedName='null', description='null', id=13, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("EncodedCredentials=");
        builder.Append(EncodedCredentialsLength() + " raw bytes");

        Limit(originalLimit);

        return builder;
    }
}
}
