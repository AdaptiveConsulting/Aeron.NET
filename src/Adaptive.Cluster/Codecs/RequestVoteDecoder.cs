/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RequestVoteDecoder
{
    public const ushort BLOCK_LENGTH = 28;
    public const ushort TEMPLATE_ID = 50;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private RequestVoteDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public RequestVoteDecoder()
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

    public RequestVoteDecoder Wrap(
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

    public static int CandidateTermIdId()
    {
        return 1;
    }

    public static int CandidateTermIdSinceVersion()
    {
        return 0;
    }

    public static int CandidateTermIdEncodingOffset()
    {
        return 0;
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
        return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
    }


    public static int LastBaseLogPositionId()
    {
        return 2;
    }

    public static int LastBaseLogPositionSinceVersion()
    {
        return 0;
    }

    public static int LastBaseLogPositionEncodingOffset()
    {
        return 8;
    }

    public static int LastBaseLogPositionEncodingLength()
    {
        return 8;
    }

    public static string LastBaseLogPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LastBaseLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastBaseLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastBaseLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LastBaseLogPosition()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int LastTermPositionId()
    {
        return 3;
    }

    public static int LastTermPositionSinceVersion()
    {
        return 0;
    }

    public static int LastTermPositionEncodingOffset()
    {
        return 16;
    }

    public static int LastTermPositionEncodingLength()
    {
        return 8;
    }

    public static string LastTermPositionMetaAttribute(MetaAttribute metaAttribute)
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

    public static long LastTermPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long LastTermPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long LastTermPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public long LastTermPosition()
    {
        return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int CandidateMemberIdId()
    {
        return 4;
    }

    public static int CandidateMemberIdSinceVersion()
    {
        return 0;
    }

    public static int CandidateMemberIdEncodingOffset()
    {
        return 24;
    }

    public static int CandidateMemberIdEncodingLength()
    {
        return 4;
    }

    public static string CandidateMemberIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static int CandidateMemberIdNullValue()
    {
        return -2147483648;
    }

    public static int CandidateMemberIdMinValue()
    {
        return -2147483647;
    }

    public static int CandidateMemberIdMaxValue()
    {
        return 2147483647;
    }

    public int CandidateMemberId()
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
        builder.Append("[RequestVote](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='candidateTermId', referencedName='null', description='null', id=1, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CandidateTermId=");
        builder.Append(CandidateTermId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastBaseLogPosition', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastBaseLogPosition=");
        builder.Append(LastBaseLogPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='lastTermPosition', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LastTermPosition=");
        builder.Append(LastTermPosition());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='candidateMemberId', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CandidateMemberId=");
        builder.Append(CandidateMemberId());

        Limit(originalLimit);

        return builder;
    }
}
}
