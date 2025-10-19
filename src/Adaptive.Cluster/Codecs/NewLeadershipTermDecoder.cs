/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class NewLeadershipTermDecoder
{
    public const ushort BLOCK_LENGTH = 88;
    public const ushort TEMPLATE_ID = 53;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 14;

    private NewLeadershipTermDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public NewLeadershipTermDecoder()
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

    public NewLeadershipTermDecoder Wrap(
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

    public static int LogLeadershipTermIdId()
    {
        return 1;
    }

    public static int LogLeadershipTermIdSinceVersion()
    {
        return 0;
    }

    public static int LogLeadershipTermIdEncodingOffset()
    {
        return 0;
    }

    public static int LogLeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static string LogLeadershipTermIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LogLeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LogLeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LogLeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LogLeadershipTermId()
    {
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int NextLeadershipTermIdId()
    {
        return 2;
    }

    public static int NextLeadershipTermIdSinceVersion()
    {
        return 0;
    }

    public static int NextLeadershipTermIdEncodingOffset()
    {
        return 8;
    }

    public static int NextLeadershipTermIdEncodingLength()
    {
        return 8;
    }

    public static string NextLeadershipTermIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long NextLeadershipTermIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextLeadershipTermIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextLeadershipTermIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long NextLeadershipTermId()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int NextTermBaseLogPositionId()
    {
        return 3;
    }

    public static int NextTermBaseLogPositionSinceVersion()
    {
        return 0;
    }

    public static int NextTermBaseLogPositionEncodingOffset()
    {
        return 16;
    }

    public static int NextTermBaseLogPositionEncodingLength()
    {
        return 8;
    }

    public static string NextTermBaseLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long NextTermBaseLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextTermBaseLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextTermBaseLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long NextTermBaseLogPosition()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int NextLogPositionId()
    {
        return 4;
    }

    public static int NextLogPositionSinceVersion()
    {
        return 0;
    }

    public static int NextLogPositionEncodingOffset()
    {
        return 24;
    }

    public static int NextLogPositionEncodingLength()
    {
        return 8;
    }

    public static string NextLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long NextLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long NextLogPosition()
    {
        return _buffer.GetLong(_offset + 24, ByteOrder.LittleEndian);
    }


    public static int LeadershipTermIdId()
    {
        return 5;
    }

    public static int LeadershipTermIdSinceVersion()
    {
        return 0;
    }

    public static int LeadershipTermIdEncodingOffset()
    {
        return 32;
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
        return _buffer.GetLong(_offset + 32, ByteOrder.LittleEndian);
    }


    public static int TermBaseLogPositionId()
    {
        return 6;
    }

    public static int TermBaseLogPositionSinceVersion()
    {
        return 0;
    }

    public static int TermBaseLogPositionEncodingOffset()
    {
        return 40;
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
        return _buffer.GetLong(_offset + 40, ByteOrder.LittleEndian);
    }


    public static int LogPositionId()
    {
        return 7;
    }

    public static int LogPositionSinceVersion()
    {
        return 0;
    }

    public static int LogPositionEncodingOffset()
    {
        return 48;
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
        return _buffer.GetLong(_offset + 48, ByteOrder.LittleEndian);
    }


    public static int LeaderRecordingIdId()
    {
        return 8;
    }

    public static int LeaderRecordingIdSinceVersion()
    {
        return 0;
    }

    public static int LeaderRecordingIdEncodingOffset()
    {
        return 56;
    }

    public static int LeaderRecordingIdEncodingLength()
    {
        return 8;
    }

    public static string LeaderRecordingIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LeaderRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LeaderRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LeaderRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LeaderRecordingId()
    {
        return _buffer.GetLong(_offset + 56, ByteOrder.LittleEndian);
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
        return 64;
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
        return _buffer.GetLong(_offset + 64, ByteOrder.LittleEndian);
    }


    public static int LeaderMemberIdId()
    {
        return 10;
    }

    public static int LeaderMemberIdSinceVersion()
    {
        return 0;
    }

    public static int LeaderMemberIdEncodingOffset()
    {
        return 72;
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
        return _buffer.GetInt(_offset + 72, ByteOrder.LittleEndian);
    }


    public static int LogSessionIdId()
    {
        return 11;
    }

    public static int LogSessionIdSinceVersion()
    {
        return 0;
    }

    public static int LogSessionIdEncodingOffset()
    {
        return 76;
    }

    public static int LogSessionIdEncodingLength()
    {
        return 4;
    }

    public static string LogSessionIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int LogSessionIdNullValue()
    {
        return -2147483648;
    }

    public static int LogSessionIdMinValue()
    {
        return -2147483647;
    }

    public static int LogSessionIdMaxValue()
    {
        return 2147483647;
    }

    public int LogSessionId()
    {
        return _buffer.GetInt(_offset + 76, ByteOrder.LittleEndian);
    }


    public static int AppVersionId()
    {
        return 12;
    }

    public static int AppVersionSinceVersion()
    {
        return 0;
    }

    public static int AppVersionEncodingOffset()
    {
        return 80;
    }

    public static int AppVersionEncodingLength()
    {
        return 4;
    }

    public static string AppVersionMetaAttribute(MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case MetaAttribute.EPOCH: return "unix";
            case MetaAttribute.TIME_UNIT: return "nanosecond";
            case MetaAttribute.SEMANTIC_TYPE: return "";
            case MetaAttribute.PRESENCE: return "optional";
        }

        return "";
    }

    public static int AppVersionNullValue()
    {
        return 0;
    }

    public static int AppVersionMinValue()
    {
        return 1;
    }

    public static int AppVersionMaxValue()
    {
        return 16777215;
    }

    public int AppVersion()
    {
        return _buffer.GetInt(_offset + 80, ByteOrder.LittleEndian);
    }


    public static int IsStartupId()
    {
        return 13;
    }

    public static int IsStartupSinceVersion()
    {
        return 0;
    }

    public static int IsStartupEncodingOffset()
    {
        return 84;
    }

    public static int IsStartupEncodingLength()
    {
        return 4;
    }

    public static string IsStartupMetaAttribute(MetaAttribute metaAttribute)
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

    public BooleanType IsStartup()
    {
        return (BooleanType)_buffer.GetInt(_offset + 84, ByteOrder.LittleEndian);
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[NewLeadershipTerm](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='logLeadershipTermId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LogLeadershipTermId=");
        builder.Append(LogLeadershipTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='nextLeadershipTermId', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("NextLeadershipTermId=");
        builder.Append(NextLeadershipTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='nextTermBaseLogPosition', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("NextTermBaseLogPosition=");
        builder.Append(NextTermBaseLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='nextLogPosition', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("NextLogPosition=");
        builder.Append(NextLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=32, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=32, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LeadershipTermId=");
        builder.Append(LeadershipTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='termBaseLogPosition', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=40, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=40, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("TermBaseLogPosition=");
        builder.Append(TermBaseLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=48, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=48, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LogPosition=");
        builder.Append(LogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='leaderRecordingId', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=0, offset=56, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=56, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LeaderRecordingId=");
        builder.Append(LeaderRecordingId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='timestamp', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=64, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time since 1 Jan 1970 UTC.', id=-1, version=0, deprecated=0, encodedLength=8, offset=64, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Timestamp=");
        builder.Append(Timestamp());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='leaderMemberId', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=72, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=72, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LeaderMemberId=");
        builder.Append(LeaderMemberId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='logSessionId', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=76, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=76, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LogSessionId=");
        builder.Append(LogSessionId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='appVersion', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=80, componentTokenCount=3, encoding=Encoding{presence=OPTIONAL, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='version_t', referencedName='null', description='Protocol or application suite version.', id=-1, version=0, deprecated=0, encodedLength=4, offset=80, componentTokenCount=1, encoding=Encoding{presence=OPTIONAL, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=1, maxValue=16777215, nullValue=0, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("AppVersion=");
        builder.Append(AppVersion());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='isStartup', referencedName='null', description='null', id=13, version=0, deprecated=0, encodedLength=0, offset=84, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=BEGIN_ENUM, name='BooleanType', referencedName='null', description='Language independent boolean type.', id=-1, version=0, deprecated=0, encodedLength=4, offset=84, componentTokenCount=4, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("IsStartup=");
        builder.Append(IsStartup());

        Limit(originalLimit);

        return builder;
    }
}
}
