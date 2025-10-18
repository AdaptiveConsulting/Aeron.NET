/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ConsensusModuleDecoder
{
    public const ushort BLOCK_LENGTH = 28;
    public const ushort TEMPLATE_ID = 105;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 13;

    private ConsensusModuleDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public ConsensusModuleDecoder()
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

    public ConsensusModuleDecoder Wrap(
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

    public static int NextSessionIdId()
    {
        return 1;
    }

    public static int NextSessionIdSinceVersion()
    {
        return 0;
    }

    public static int NextSessionIdEncodingOffset()
    {
        return 0;
    }

    public static int NextSessionIdEncodingLength()
    {
        return 8;
    }

    public static string NextSessionIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long NextSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long NextSessionId()
    {
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int NextServiceSessionIdId()
    {
        return 2;
    }

    public static int NextServiceSessionIdSinceVersion()
    {
        return 3;
    }

    public static int NextServiceSessionIdEncodingOffset()
    {
        return 8;
    }

    public static int NextServiceSessionIdEncodingLength()
    {
        return 8;
    }

    public static string NextServiceSessionIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long NextServiceSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextServiceSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextServiceSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long NextServiceSessionId()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int LogServiceSessionIdId()
    {
        return 3;
    }

    public static int LogServiceSessionIdSinceVersion()
    {
        return 3;
    }

    public static int LogServiceSessionIdEncodingOffset()
    {
        return 16;
    }

    public static int LogServiceSessionIdEncodingLength()
    {
        return 8;
    }

    public static string LogServiceSessionIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LogServiceSessionIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LogServiceSessionIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LogServiceSessionIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LogServiceSessionId()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int PendingMessageCapacityId()
    {
        return 4;
    }

    public static int PendingMessageCapacitySinceVersion()
    {
        return 3;
    }

    public static int PendingMessageCapacityEncodingOffset()
    {
        return 24;
    }

    public static int PendingMessageCapacityEncodingLength()
    {
        return 4;
    }

    public static string PendingMessageCapacityMetaAttribute(MetaAttribute metaAttribute)
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

    public static int PendingMessageCapacityNullValue()
    {
        return 0;
    }

    public static int PendingMessageCapacityMinValue()
    {
        return -2147483647;
    }

    public static int PendingMessageCapacityMaxValue()
    {
        return 2147483647;
    }

    public int PendingMessageCapacity()
    {
        return _buffer.GetInt(_offset + 24, ByteOrder.LittleEndian);
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[ConsensusModule](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='nextSessionId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("NextSessionId=");
        builder.Append(NextSessionId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='nextServiceSessionId', referencedName='null', description='null', id=2, version=3, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=OPTIONAL, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=OPTIONAL, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("NextServiceSessionId=");
        builder.Append(NextServiceSessionId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='logServiceSessionId', referencedName='null', description='null', id=3, version=3, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=OPTIONAL, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=OPTIONAL, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LogServiceSessionId=");
        builder.Append(LogServiceSessionId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='pendingMessageCapacity', referencedName='null', description='null', id=4, version=3, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=OPTIONAL, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='capacity_t', referencedName='null', description='Capacity of a container.', id=-1, version=0, deprecated=0, encodedLength=4, offset=24, componentTokenCount=1, encoding=Encoding{presence=OPTIONAL, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=0, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("PendingMessageCapacity=");
        builder.Append(PendingMessageCapacity());

        Limit(originalLimit);

        return builder;
    }
}
}
