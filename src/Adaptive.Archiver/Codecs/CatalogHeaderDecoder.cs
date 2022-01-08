/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Archiver.Codecs {

public class CatalogHeaderDecoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 20;
    public const ushort SCHEMA_ID = 101;
    public const ushort SCHEMA_VERSION = 6;

    private CatalogHeaderDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public CatalogHeaderDecoder()
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

    public CatalogHeaderDecoder Wrap(
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


    public static int LengthId()
    {
        return 2;
    }

    public static int LengthSinceVersion()
    {
        return 0;
    }

    public static int LengthEncodingOffset()
    {
        return 4;
    }

    public static int LengthEncodingLength()
    {
        return 4;
    }

    public static string LengthMetaAttribute(MetaAttribute metaAttribute)
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

    public static int LengthNullValue()
    {
        return -2147483648;
    }

    public static int LengthMinValue()
    {
        return -2147483647;
    }

    public static int LengthMaxValue()
    {
        return 2147483647;
    }

    public int Length()
    {
        return _buffer.GetInt(_offset + 4, ByteOrder.LittleEndian);
    }


    public static int NextRecordingIdId()
    {
        return 3;
    }

    public static int NextRecordingIdSinceVersion()
    {
        return 0;
    }

    public static int NextRecordingIdEncodingOffset()
    {
        return 8;
    }

    public static int NextRecordingIdEncodingLength()
    {
        return 8;
    }

    public static string NextRecordingIdMetaAttribute(MetaAttribute metaAttribute)
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

    public static long NextRecordingIdNullValue()
    {
        return -9223372036854775808L;
    }

    public static long NextRecordingIdMinValue()
    {
        return -9223372036854775807L;
    }

    public static long NextRecordingIdMaxValue()
    {
        return 9223372036854775807L;
    }

    public long NextRecordingId()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
    }


    public static int AlignmentId()
    {
        return 4;
    }

    public static int AlignmentSinceVersion()
    {
        return 0;
    }

    public static int AlignmentEncodingOffset()
    {
        return 16;
    }

    public static int AlignmentEncodingLength()
    {
        return 4;
    }

    public static string AlignmentMetaAttribute(MetaAttribute metaAttribute)
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

    public static int AlignmentNullValue()
    {
        return -2147483648;
    }

    public static int AlignmentMinValue()
    {
        return -2147483647;
    }

    public static int AlignmentMaxValue()
    {
        return 2147483647;
    }

    public int Alignment()
    {
        return _buffer.GetInt(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int ReservedId()
    {
        return 5;
    }

    public static int ReservedSinceVersion()
    {
        return 0;
    }

    public static int ReservedEncodingOffset()
    {
        return 31;
    }

    public static int ReservedEncodingLength()
    {
        return 1;
    }

    public static string ReservedMetaAttribute(MetaAttribute metaAttribute)
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

    public static sbyte ReservedNullValue()
    {
        return (sbyte)-128;
    }

    public static sbyte ReservedMinValue()
    {
        return (sbyte)-127;
    }

    public static sbyte ReservedMaxValue()
    {
        return (sbyte)127;
    }

    public sbyte Reserved()
    {
        return unchecked((sbyte)_buffer.GetByte(_offset + 31));
    }



    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        int originalLimit = Limit();
        Limit(_offset + _actingBlockLength);
        builder.Append("[CatalogHeader](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='length', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=4, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=4, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Length=");
        builder.Append(Length());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='nextRecordingId', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("NextRecordingId=");
        builder.Append(NextRecordingId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='alignment', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Alignment=");
        builder.Append(Alignment());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='reserved', referencedName='null', description='null', id=5, version=0, deprecated=0, encodedLength=0, offset=31, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int8', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=1, offset=31, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT8, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("Reserved=");
        builder.Append(Reserved());

        Limit(originalLimit);

        return builder;
    }
}
}
