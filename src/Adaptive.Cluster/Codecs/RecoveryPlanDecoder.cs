/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RecoveryPlanDecoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 110;
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

    public static int LastLeadershipTermIdId()
    {
        return 1;
    }

    public static int LastLeadershipTermIdSinceVersion()
    {
        return 0;
    }

    public static int LastLeadershipTermIdEncodingOffset()
    {
        return 0;
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
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int LastTermBaseLogPositionId()
    {
        return 2;
    }

    public static int LastTermBaseLogPositionSinceVersion()
    {
        return 0;
    }

    public static int LastTermBaseLogPositionEncodingOffset()
    {
        return 8;
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
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int LastTermPositionCommittedId()
    {
        return 3;
    }

    public static int LastTermPositionCommittedSinceVersion()
    {
        return 0;
    }

    public static int LastTermPositionCommittedEncodingOffset()
    {
        return 16;
    }

    public static int LastTermPositionCommittedEncodingLength()
    {
        return 8;
    }

    public static string LastTermPositionCommittedMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LastTermPositionCommittedNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastTermPositionCommittedMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastTermPositionCommittedMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LastTermPositionCommitted()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int LastTermPositionAppendedId()
    {
        return 4;
    }

    public static int LastTermPositionAppendedSinceVersion()
    {
        return 0;
    }

    public static int LastTermPositionAppendedEncodingOffset()
    {
        return 24;
    }

    public static int LastTermPositionAppendedEncodingLength()
    {
        return 8;
    }

    public static string LastTermPositionAppendedMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LastTermPositionAppendedNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastTermPositionAppendedMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastTermPositionAppendedMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LastTermPositionAppended()
    {
        return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
    }


    private StepsDecoder _Steps = new StepsDecoder();

    public static long StepsDecoderId()
    {
        return 5;
    }

    public static int StepsDecoderSinceVersion()
    {
        return 0;
    }

    public StepsDecoder Steps()
    {
        _Steps.Wrap(_parentMessage, _buffer);
        return _Steps;
    }

    public class StepsDecoder
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
            return 68;
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

        public StepsDecoder Next()
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

        public static int RecordingStartPositionId()
        {
            return 6;
        }

        public static int RecordingStartPositionSinceVersion()
        {
            return 0;
        }

        public static int RecordingStartPositionEncodingOffset()
        {
            return 0;
        }

        public static int RecordingStartPositionEncodingLength()
        {
            return 8;
        }

        public static string RecordingStartPositionMetaAttribute(MetaAttribute metaAttribute)
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

        public static long RecordingStartPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long RecordingStartPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long RecordingStartPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public long RecordingStartPosition()
        {
            return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
        }


        public static int RecordingStopPositionId()
        {
            return 7;
        }

        public static int RecordingStopPositionSinceVersion()
        {
            return 0;
        }

        public static int RecordingStopPositionEncodingOffset()
        {
            return 8;
        }

        public static int RecordingStopPositionEncodingLength()
        {
            return 8;
        }

        public static string RecordingStopPositionMetaAttribute(MetaAttribute metaAttribute)
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

        public static long RecordingStopPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long RecordingStopPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long RecordingStopPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public long RecordingStopPosition()
        {
            return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
        }


        public static int RecordingIdId()
        {
            return 8;
        }

        public static int RecordingIdSinceVersion()
        {
            return 0;
        }

        public static int RecordingIdEncodingOffset()
        {
            return 16;
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
            return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
        }


        public static int LeadershipTermIdId()
        {
            return 9;
        }

        public static int LeadershipTermIdSinceVersion()
        {
            return 0;
        }

        public static int LeadershipTermIdEncodingOffset()
        {
            return 24;
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
            return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
        }


        public static int TermBaseLogPositionId()
        {
            return 10;
        }

        public static int TermBaseLogPositionSinceVersion()
        {
            return 0;
        }

        public static int TermBaseLogPositionEncodingOffset()
        {
            return 32;
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
            return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
        }


        public static int TermPositionId()
        {
            return 11;
        }

        public static int TermPositionSinceVersion()
        {
            return 0;
        }

        public static int TermPositionEncodingOffset()
        {
            return 40;
        }

        public static int TermPositionEncodingLength()
        {
            return 8;
        }

        public static string TermPositionMetaAttribute(MetaAttribute metaAttribute)
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

        public static long TermPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long TermPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long TermPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public long TermPosition()
        {
            return _buffer.GetLong(_offset + 40, ByteOrder.LittleEndian);
        }


        public static int TimestampId()
        {
            return 12;
        }

        public static int TimestampSinceVersion()
        {
            return 0;
        }

        public static int TimestampEncodingOffset()
        {
            return 48;
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
            return _buffer.GetLong(_offset + 48, ByteOrder.LittleEndian);
        }


        public static int VotedForMemberIdId()
        {
            return 13;
        }

        public static int VotedForMemberIdSinceVersion()
        {
            return 0;
        }

        public static int VotedForMemberIdEncodingOffset()
        {
            return 56;
        }

        public static int VotedForMemberIdEncodingLength()
        {
            return 4;
        }

        public static string VotedForMemberIdMetaAttribute(MetaAttribute metaAttribute)
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

        public static int VotedForMemberIdNullValue()
        {
            return -2147483648;
        }

        public static int VotedForMemberIdMinValue()
        {
            return -2147483647;
        }

        public static int VotedForMemberIdMaxValue()
        {
            return 2147483647;
        }

        public int VotedForMemberId()
        {
            return _buffer.GetInt(_offset + 56, ByteOrder.LittleEndian);
        }


        public static int EntryTypeId()
        {
            return 14;
        }

        public static int EntryTypeSinceVersion()
        {
            return 0;
        }

        public static int EntryTypeEncodingOffset()
        {
            return 60;
        }

        public static int EntryTypeEncodingLength()
        {
            return 4;
        }

        public static string EntryTypeMetaAttribute(MetaAttribute metaAttribute)
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

        public static int EntryTypeNullValue()
        {
            return -2147483648;
        }

        public static int EntryTypeMinValue()
        {
            return -2147483647;
        }

        public static int EntryTypeMaxValue()
        {
            return 2147483647;
        }

        public int EntryType()
        {
            return _buffer.GetInt(_offset + 60, ByteOrder.LittleEndian);
        }


        public static int EntryIndexId()
        {
            return 15;
        }

        public static int EntryIndexSinceVersion()
        {
            return 0;
        }

        public static int EntryIndexEncodingOffset()
        {
            return 64;
        }

        public static int EntryIndexEncodingLength()
        {
            return 4;
        }

        public static string EntryIndexMetaAttribute(MetaAttribute metaAttribute)
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

        public static int EntryIndexNullValue()
        {
            return -2147483648;
        }

        public static int EntryIndexMinValue()
        {
            return -2147483647;
        }

        public static int EntryIndexMaxValue()
        {
            return 2147483647;
        }

        public int EntryIndex()
        {
            return _buffer.GetInt(_offset + 64, ByteOrder.LittleEndian);
        }



        public override string ToString()
        {
            return AppendTo(new StringBuilder(100)).ToString();
        }

        public StringBuilder AppendTo(StringBuilder builder)
        {
            builder.Append('(');
            //Token{signal=BEGIN_FIELD, name='recordingStartPosition', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("RecordingStartPosition=");
            builder.Append(RecordingStartPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='recordingStopPosition', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("RecordingStopPosition=");
            builder.Append(RecordingStopPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='recordingId', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("RecordingId=");
            builder.Append(RecordingId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LeadershipTermId=");
            builder.Append(LeadershipTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='termBaseLogPosition', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TermBaseLogPosition=");
            builder.Append(TermBaseLogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='termPosition', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TermPosition=");
            builder.Append(TermPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='timestamp', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=48, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time in milliseconds since 1 Jan 1970 UTC', id=-1, version=0, deprecated=0, encodedLength=8, offset=48, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("Timestamp=");
            builder.Append(Timestamp());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='votedForMemberId', referencedName='null', description='null', id=13, version=0, deprecated=0, encodedLength=0, offset=56, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=56, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("VotedForMemberId=");
            builder.Append(VotedForMemberId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='entryType', referencedName='null', description='null', id=14, version=0, deprecated=0, encodedLength=0, offset=60, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=60, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("EntryType=");
            builder.Append(EntryType());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='entryIndex', referencedName='null', description='null', id=15, version=0, deprecated=0, encodedLength=0, offset=64, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=64, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("EntryIndex=");
            builder.Append(EntryIndex());
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
        //Token{signal=BEGIN_FIELD, name='lastLeadershipTermId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastLeadershipTermId=");
        builder.Append(LastLeadershipTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastTermBaseLogPosition', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastTermBaseLogPosition=");
        builder.Append(LastTermBaseLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastTermPositionCommitted', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastTermPositionCommitted=");
        builder.Append(LastTermPositionCommitted());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastTermPositionAppended', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastTermPositionAppended=");
        builder.Append(LastTermPositionAppended());
        builder.Append('|');
        //Token{signal=BEGIN_GROUP, name='steps', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=68, offset=32, componentTokenCount=36, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("Steps=[");
        StepsDecoder Steps = this.Steps();
        if (Steps.Count() > 0)
        {
            while (Steps.HasNext())
            {
                Steps.Next().AppendTo(builder);
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
