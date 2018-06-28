/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RecoveryPlanDecoder
{
    public const ushort BLOCK_LENGTH = 48;
    public const ushort TEMPLATE_ID = 62;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private RecoveryPlanDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public RecoveryPlanDecoder()
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

    public RecoveryPlanDecoder Wrap(
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


    public static int RequestMemberIdId()
    {
        return 2;
    }

    public static int RequestMemberIdSinceVersion()
    {
        return 0;
    }

    public static int RequestMemberIdEncodingOffset()
    {
        return 8;
    }

    public static int RequestMemberIdEncodingLength()
    {
        return 4;
    }

    public static string RequestMemberIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int RequestMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int RequestMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int RequestMemberIdMaxValue()
    {
        return 2147483647;
    }

    public int RequestMemberId()
    {
        return _buffer.GetInt(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int LeaderMemberIdId()
    {
        return 3;
    }

    public static int LeaderMemberIdSinceVersion()
    {
        return 0;
    }

    public static int LeaderMemberIdEncodingOffset()
    {
        return 12;
    }

    public static int LeaderMemberIdEncodingLength()
    {
        return 4;
    }

    public static string LeaderMemberIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int LeaderMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int LeaderMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int LeaderMemberIdMaxValue()
    {
        return 2147483647;
    }

    public int LeaderMemberId()
    {
        return _buffer.GetInt(_offset + 12, ByteOrder.LittleEndian);
    }


    public static int LastLeadershipTermIdId()
    {
        return 4;
    }

    public static int LastLeadershipTermIdSinceVersion()
    {
        return 0;
    }

    public static int LastLeadershipTermIdEncodingOffset()
    {
        return 16;
    }

    public static int LastLeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static string LastLeadershipTermIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LastLeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastLeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastLeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LastLeadershipTermId()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int LastTermBaseLogPositionId()
    {
        return 5;
    }

    public static int LastTermBaseLogPositionSinceVersion()
    {
        return 0;
    }

    public static int LastTermBaseLogPositionEncodingOffset()
    {
        return 24;
    }

    public static int LastTermBaseLogPositionEncodingLength()
    {
        return 8;
    }

    public static string LastTermBaseLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LastTermBaseLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastTermBaseLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastTermBaseLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LastTermBaseLogPosition()
    {
        return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
    }


    public static int AppendedLogPositionId()
    {
        return 6;
    }

    public static int AppendedLogPositionSinceVersion()
    {
        return 0;
    }

    public static int AppendedLogPositionEncodingOffset()
    {
        return 32;
    }

    public static int AppendedLogPositionEncodingLength()
    {
        return 8;
    }

    public static string AppendedLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long AppendedLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long AppendedLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long AppendedLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long AppendedLogPosition()
    {
        return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
    }


    public static int CommittedLogPositionId()
    {
        return 7;
    }

    public static int CommittedLogPositionSinceVersion()
    {
        return 0;
    }

    public static int CommittedLogPositionEncodingOffset()
    {
        return 40;
    }

    public static int CommittedLogPositionEncodingLength()
    {
        return 8;
    }

    public static string CommittedLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long CommittedLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CommittedLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CommittedLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long CommittedLogPosition()
    {
        return _buffer.GetLong(_offset + 40, ByteOrder.LittleEndian);
    }


    private SnapshotsDecoder _Snapshots = new SnapshotsDecoder();

    public static long SnapshotsDecoderId()
    {
        return 8;
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
        private RecoveryPlanDecoder _parentMessage;
        private IDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;
        private int _blockLength;

        public void Wrap(
            RecoveryPlanDecoder parentMessage, IDirectBuffer buffer)
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
            return 9;
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
            return 10;
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
            return 11;
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
            return 12;
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
            return 13;
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
            return 14;
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



        public override string ToString()
        {
            return AppendTo(new StringBuilder(100)).ToString();
        }

        public StringBuilder AppendTo(StringBuilder builder)
        {
            builder.Append('(');
            //Token{signal=BEGIN_FIELD, name='recordingId', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("RecordingId=");
            builder.Append(RecordingId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LeadershipTermId=");
            builder.Append(LeadershipTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='termBaseLogPosition', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TermBaseLogPosition=");
            builder.Append(TermBaseLogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogPosition=");
            builder.Append(LogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='timestamp', referencedName='null', description='null', id=13, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time in milliseconds since 1 Jan 1970 UTC', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("Timestamp=");
            builder.Append(Timestamp());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='serviceId', referencedName='null', description='null', id=14, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ServiceId=");
            builder.Append(ServiceId());
            builder.Append(')');
            return builder;
        }
    }

    private LogsDecoder _Logs = new LogsDecoder();

    public static long LogsDecoderId()
    {
        return 15;
    }

    public static int LogsDecoderSinceVersion()
    {
        return 0;
    }

    public LogsDecoder Logs()
    {
        _Logs.Wrap(_parentMessage, _buffer);
        return _Logs;
    }

    public class LogsDecoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingDecoder _dimensions = new GroupSizeEncodingDecoder();
        private RecoveryPlanDecoder _parentMessage;
        private IDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;
        private int _blockLength;

        public void Wrap(
            RecoveryPlanDecoder parentMessage, IDirectBuffer buffer)
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
            return 64;
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

        public LogsDecoder Next()
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
            return 16;
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
            return 17;
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
            return 18;
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
            return 19;
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


        public static int StartPositionId()
        {
            return 12;
        }

        public static int StartPositionSinceVersion()
        {
            return 0;
        }

        public static int StartPositionEncodingOffset()
        {
            return 32;
        }

        public static int StartPositionEncodingLength()
        {
            return 8;
        }

        public static string StartPositionMetaAttribute(MetaAttribute metaAttribute)
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

        public static long StartPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long StartPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long StartPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public long StartPosition()
        {
            return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
        }


        public static int StopPositionId()
        {
            return 21;
        }

        public static int StopPositionSinceVersion()
        {
            return 0;
        }

        public static int StopPositionEncodingOffset()
        {
            return 40;
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
            return _buffer.GetLong(_offset + 40, ByteOrder.LittleEndian);
        }


        public static int InitialTermIdId()
        {
            return 22;
        }

        public static int InitialTermIdSinceVersion()
        {
            return 0;
        }

        public static int InitialTermIdEncodingOffset()
        {
            return 48;
        }

        public static int InitialTermIdEncodingLength()
        {
            return 4;
        }

        public static string InitialTermIdMetaAttribute(MetaAttribute metaAttribute)
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

        public static int InitialTermIdNullValue()
        {
            return -2147483648;
        }

        public static int InitialTermIdMinValue()
        {
            return -2147483647;
        }

        public static int InitialTermIdMaxValue()
        {
            return 2147483647;
        }

        public int InitialTermId()
        {
            return _buffer.GetInt(_offset + 48, ByteOrder.LittleEndian);
        }


        public static int TermBufferLengthId()
        {
            return 23;
        }

        public static int TermBufferLengthSinceVersion()
        {
            return 0;
        }

        public static int TermBufferLengthEncodingOffset()
        {
            return 52;
        }

        public static int TermBufferLengthEncodingLength()
        {
            return 4;
        }

        public static string TermBufferLengthMetaAttribute(MetaAttribute metaAttribute)
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

        public static int TermBufferLengthNullValue()
        {
            return -2147483648;
        }

        public static int TermBufferLengthMinValue()
        {
            return -2147483647;
        }

        public static int TermBufferLengthMaxValue()
        {
            return 2147483647;
        }

        public int TermBufferLength()
        {
            return _buffer.GetInt(_offset + 52, ByteOrder.LittleEndian);
        }


        public static int MtuLengthId()
        {
            return 24;
        }

        public static int MtuLengthSinceVersion()
        {
            return 0;
        }

        public static int MtuLengthEncodingOffset()
        {
            return 56;
        }

        public static int MtuLengthEncodingLength()
        {
            return 4;
        }

        public static string MtuLengthMetaAttribute(MetaAttribute metaAttribute)
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

        public static int MtuLengthNullValue()
        {
            return -2147483648;
        }

        public static int MtuLengthMinValue()
        {
            return -2147483647;
        }

        public static int MtuLengthMaxValue()
        {
            return 2147483647;
        }

        public int MtuLength()
        {
            return _buffer.GetInt(_offset + 56, ByteOrder.LittleEndian);
        }


        public static int SessionIdId()
        {
            return 25;
        }

        public static int SessionIdSinceVersion()
        {
            return 0;
        }

        public static int SessionIdEncodingOffset()
        {
            return 60;
        }

        public static int SessionIdEncodingLength()
        {
            return 4;
        }

        public static string SessionIdMetaAttribute(MetaAttribute metaAttribute)
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

        public static int SessionIdNullValue()
        {
            return -2147483648;
        }

        public static int SessionIdMinValue()
        {
            return -2147483647;
        }

        public static int SessionIdMaxValue()
        {
            return 2147483647;
        }

        public int SessionId()
        {
            return _buffer.GetInt(_offset + 60, ByteOrder.LittleEndian);
        }



        public override string ToString()
        {
            return AppendTo(new StringBuilder(100)).ToString();
        }

        public StringBuilder AppendTo(StringBuilder builder)
        {
            builder.Append('(');
            //Token{signal=BEGIN_FIELD, name='recordingId', referencedName='null', description='null', id=16, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("RecordingId=");
            builder.Append(RecordingId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=17, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LeadershipTermId=");
            builder.Append(LeadershipTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='termBaseLogPosition', referencedName='null', description='null', id=18, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TermBaseLogPosition=");
            builder.Append(TermBaseLogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=19, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogPosition=");
            builder.Append(LogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='startPosition', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("StartPosition=");
            builder.Append(StartPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='stopPosition', referencedName='null', description='null', id=21, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("StopPosition=");
            builder.Append(StopPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='initialTermId', referencedName='null', description='null', id=22, version=0, deprecated=0, encodedLength=0, offset=48, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=48, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("InitialTermId=");
            builder.Append(InitialTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='termBufferLength', referencedName='null', description='null', id=23, version=0, deprecated=0, encodedLength=0, offset=52, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=52, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TermBufferLength=");
            builder.Append(TermBufferLength());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='mtuLength', referencedName='null', description='null', id=24, version=0, deprecated=0, encodedLength=0, offset=56, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=56, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("MtuLength=");
            builder.Append(MtuLength());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='sessionId', referencedName='null', description='null', id=25, version=0, deprecated=0, encodedLength=0, offset=60, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=60, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("SessionId=");
            builder.Append(SessionId());
            builder.Append(')');
            return builder;
        }
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[RecoveryPlan](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='requestMemberId', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("RequestMemberId=");
        builder.Append(RequestMemberId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='leaderMemberId', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=12, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=12, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LeaderMemberId=");
        builder.Append(LeaderMemberId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastLeadershipTermId', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastLeadershipTermId=");
        builder.Append(LastLeadershipTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastTermBaseLogPosition', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastTermBaseLogPosition=");
        builder.Append(LastTermBaseLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='appendedLogPosition', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("AppendedLogPosition=");
        builder.Append(AppendedLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='committedLogPosition', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CommittedLogPosition=");
        builder.Append(CommittedLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_GROUP, name='snapshots', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=44, offset=48, componentTokenCount=24, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
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
        //Token{signal=BEGIN_GROUP, name='logs', referencedName='null', description='null', id=15, version=0, deprecated=0, encodedLength=64, offset=-1, componentTokenCount=36, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("Logs=[");
        LogsDecoder Logs = this.Logs();
        if (Logs.Count() > 0)
        {
            while (Logs.HasNext())
            {
                Logs.Next().AppendTo(builder);
                builder.Append(',');
            }
            builder.Length = builder.Length - 1;
        }
        builder.Append(']');

        Limit(originalLimit);

        return builder;
    }
}
}
