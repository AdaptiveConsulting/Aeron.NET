/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class SnapshotMarkerDecoder
{
    public const ushort BLOCK_LENGTH = 40;
    public const ushort TEMPLATE_ID = 100;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 12;

    private SnapshotMarkerDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public SnapshotMarkerDecoder()
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

    public SnapshotMarkerDecoder Wrap(
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

    public static int TypeIdId()
    {
        return 1;
    }

    public static int TypeIdSinceVersion()
    {
        return 0;
    }

    public static int TypeIdEncodingOffset()
    {
        return 0;
    }

    public static int TypeIdEncodingLength()
    {
        return 8;
    }

    public static string TypeIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long TypeIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long TypeIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long TypeIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long TypeId()
    {
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int LogPositionId()
    {
        return 2;
    }

    public static int LogPositionSinceVersion()
    {
        return 0;
    }

    public static int LogPositionEncodingOffset()
    {
        return 8;
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
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int LeadershipTermIdId()
    {
        return 3;
    }

    public static int LeadershipTermIdSinceVersion()
    {
        return 0;
    }

    public static int LeadershipTermIdEncodingOffset()
    {
        return 16;
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
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int IndexId()
    {
        return 4;
    }

    public static int IndexSinceVersion()
    {
        return 0;
    }

    public static int IndexEncodingOffset()
    {
        return 24;
    }

    public static int IndexEncodingLength()
    {
        return 4;
    }

    public static string IndexMetaAttribute(MetaAttribute metaAttribute)
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

    public static int IndexNullValue()
    {
        return -2147483648;
    }

    public static int IndexMinValue()
    {
        return -2147483647;
    }

    public static int IndexMaxValue()
    {
        return 2147483647;
    }

    public int Index()
    {
        return _buffer.GetInt(_offset + 24, ByteOrder.LittleEndian);
    }


    public static int MarkId()
    {
        return 5;
    }

    public static int MarkSinceVersion()
    {
        return 0;
    }

    public static int MarkEncodingOffset()
    {
        return 28;
    }

    public static int MarkEncodingLength()
    {
        return 4;
    }

    public static string MarkMetaAttribute(MetaAttribute metaAttribute)
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

    public SnapshotMark Mark()
    {
        return (SnapshotMark)_buffer.GetInt(_offset + 28, ByteOrder.LittleEndian);
    }


    public static int TimeUnitId()
    {
        return 6;
    }

    public static int TimeUnitSinceVersion()
    {
        return 4;
    }

    public static int TimeUnitEncodingOffset()
    {
        return 32;
    }

    public static int TimeUnitEncodingLength()
    {
        return 4;
    }

    public static string TimeUnitMetaAttribute(MetaAttribute metaAttribute)
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

    public ClusterTimeUnit TimeUnit()
    {
        if (_actingVersion < 4) return ClusterTimeUnit.NULL_VALUE;

        return (ClusterTimeUnit)_buffer.GetInt(_offset + 32, ByteOrder.LittleEndian);
    }


    public static int AppVersionId()
    {
        return 7;
    }

    public static int AppVersionSinceVersion()
    {
        return 4;
    }

    public static int AppVersionEncodingOffset()
    {
        return 36;
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
        return _buffer.GetInt(_offset + 36, ByteOrder.LittleEndian);
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[SnapshotMarker](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='typeId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("TypeId=");
        builder.Append(TypeId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LogPosition=");
        builder.Append(LogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LeadershipTermId=");
        builder.Append(LeadershipTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='index', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Index=");
        builder.Append(Index());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='mark', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=28, componentTokenCount=7, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=BEGIN_ENUM, name='SnapshotMark', referencedName='null', description='Mark within a snapshot.', id=-1, version=0, deprecated=0, encodedLength=4, offset=28, componentTokenCount=5, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("Mark=");
        builder.Append(Mark());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='timeUnit', referencedName='null', description='null', id=6, version=4, deprecated=0, encodedLength=0, offset=32, componentTokenCount=7, encoding=Encoding{presence=OPTIONAL, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=BEGIN_ENUM, name='ClusterTimeUnit', referencedName='null', description='Type the time unit used for timestamps.', id=-1, version=4, deprecated=0, encodedLength=4, offset=32, componentTokenCount=5, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("TimeUnit=");
        builder.Append(TimeUnit());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='appVersion', referencedName='null', description='null', id=7, version=4, deprecated=0, encodedLength=0, offset=36, componentTokenCount=3, encoding=Encoding{presence=OPTIONAL, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='version_t', referencedName='null', description='Protocol or application suite version.', id=-1, version=0, deprecated=0, encodedLength=4, offset=36, componentTokenCount=1, encoding=Encoding{presence=OPTIONAL, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=1, maxValue=16777215, nullValue=0, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("AppVersion=");
        builder.Append(AppVersion());

        Limit(originalLimit);

        return builder;
    }
}
}
