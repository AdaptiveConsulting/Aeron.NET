/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ClusterMembersExtendedResponseDecoder
{
    public const ushort BLOCK_LENGTH = 24;
    public const ushort TEMPLATE_ID = 43;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 9;

    private ClusterMembersExtendedResponseDecoder _parentMessage;
    private IDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;
    protected int _actingBlockLength;
    protected int _actingVersion;

    public ClusterMembersExtendedResponseDecoder()
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

    public ClusterMembersExtendedResponseDecoder Wrap(
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


    public static int CurrentTimeNsId()
    {
        return 2;
    }

    public static int CurrentTimeNsSinceVersion()
    {
        return 0;
    }

    public static int CurrentTimeNsEncodingOffset()
    {
        return 8;
    }

    public static int CurrentTimeNsEncodingLength()
    {
        return 8;
    }

    public static string CurrentTimeNsMetaAttribute(MetaAttribute metaAttribute)
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

    public static long CurrentTimeNsNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CurrentTimeNsMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CurrentTimeNsMaxValue()
    {
        return 9223372036854775807L;
    }

    public long CurrentTimeNs()
    {
        return _buffer.GetLong(_offset + 8, ByteOrder.LittleEndian);
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
        return 16;
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
        return _buffer.GetInt(_offset + 16, ByteOrder.LittleEndian);
    }


    public static int MemberIdId()
    {
        return 4;
    }

    public static int MemberIdSinceVersion()
    {
        return 0;
    }

    public static int MemberIdEncodingOffset()
    {
        return 20;
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
        return _buffer.GetInt(_offset + 20, ByteOrder.LittleEndian);
    }


    private ActiveMembersDecoder _ActiveMembers = new ActiveMembersDecoder();

    public static long ActiveMembersDecoderId()
    {
        return 5;
    }

    public static int ActiveMembersDecoderSinceVersion()
    {
        return 0;
    }

    public ActiveMembersDecoder ActiveMembers()
    {
        _ActiveMembers.Wrap(_parentMessage, _buffer);
        return _ActiveMembers;
    }

    public class ActiveMembersDecoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingDecoder _dimensions = new GroupSizeEncodingDecoder();
        private ClusterMembersExtendedResponseDecoder _parentMessage;
        private IDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;
        private int _blockLength;

        public void Wrap(
            ClusterMembersExtendedResponseDecoder parentMessage, IDirectBuffer buffer)
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
            return 28;
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

        public ActiveMembersDecoder Next()
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
            return 0;
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
            return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
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


        public static int TimeOfLastAppendNsId()
        {
            return 8;
        }

        public static int TimeOfLastAppendNsSinceVersion()
        {
            return 0;
        }

        public static int TimeOfLastAppendNsEncodingOffset()
        {
            return 16;
        }

        public static int TimeOfLastAppendNsEncodingLength()
        {
            return 8;
        }

        public static string TimeOfLastAppendNsMetaAttribute(MetaAttribute metaAttribute)
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

        public static long TimeOfLastAppendNsNullValue()
        {
            return -9223372036854775808L;
        }

        public static long TimeOfLastAppendNsMinValue()
        {
            return -9223372036854775807L;
        }

        public static long TimeOfLastAppendNsMaxValue()
        {
            return 9223372036854775807L;
        }

        public long TimeOfLastAppendNs()
        {
            return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
        }


        public static int MemberIdId()
        {
            return 9;
        }

        public static int MemberIdSinceVersion()
        {
            return 0;
        }

        public static int MemberIdEncodingOffset()
        {
            return 24;
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
            return _buffer.GetInt(_offset + 24, ByteOrder.LittleEndian);
        }


        public static int IngressEndpointId()
        {
            return 10;
        }

        public static int IngressEndpointSinceVersion()
        {
            return 0;
        }

        public static string IngressEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string IngressEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int IngressEndpointHeaderLength()
        {
            return 4;
        }

        public int IngressEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetIngressEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetIngressEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string IngressEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int ConsensusEndpointId()
        {
            return 11;
        }

        public static int ConsensusEndpointSinceVersion()
        {
            return 0;
        }

        public static string ConsensusEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string ConsensusEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int ConsensusEndpointHeaderLength()
        {
            return 4;
        }

        public int ConsensusEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetConsensusEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetConsensusEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string ConsensusEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int LogEndpointId()
        {
            return 12;
        }

        public static int LogEndpointSinceVersion()
        {
            return 0;
        }

        public static string LogEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string LogEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int LogEndpointHeaderLength()
        {
            return 4;
        }

        public int LogEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetLogEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetLogEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string LogEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int CatchupEndpointId()
        {
            return 13;
        }

        public static int CatchupEndpointSinceVersion()
        {
            return 0;
        }

        public static string CatchupEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string CatchupEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int CatchupEndpointHeaderLength()
        {
            return 4;
        }

        public int CatchupEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetCatchupEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetCatchupEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string CatchupEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int ArchiveEndpointId()
        {
            return 14;
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
            //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=6, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LeadershipTermId=");
            builder.Append(LeadershipTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=7, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogPosition=");
            builder.Append(LogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='timeOfLastAppendNs', referencedName='null', description='null', id=8, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time since 1 Jan 1970 UTC.', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TimeOfLastAppendNs=");
            builder.Append(TimeOfLastAppendNs());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='memberId', referencedName='null', description='null', id=9, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("MemberId=");
            builder.Append(MemberId());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='ingressEndpoint', referencedName='null', description='null', id=10, version=0, deprecated=0, encodedLength=0, offset=28, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("IngressEndpoint=");
            builder.Append(IngressEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='consensusEndpoint', referencedName='null', description='null', id=11, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ConsensusEndpoint=");
            builder.Append(ConsensusEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='logEndpoint', referencedName='null', description='null', id=12, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogEndpoint=");
            builder.Append(LogEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='catchupEndpoint', referencedName='null', description='null', id=13, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("CatchupEndpoint=");
            builder.Append(CatchupEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='archiveEndpoint', referencedName='null', description='null', id=14, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ArchiveEndpoint=");
            builder.Append(ArchiveEndpoint());
            builder.Append(')');
            return builder;
        }
    }

    private PassiveMembersDecoder _PassiveMembers = new PassiveMembersDecoder();

    public static long PassiveMembersDecoderId()
    {
        return 15;
    }

    public static int PassiveMembersDecoderSinceVersion()
    {
        return 0;
    }

    public PassiveMembersDecoder PassiveMembers()
    {
        _PassiveMembers.Wrap(_parentMessage, _buffer);
        return _PassiveMembers;
    }

    public class PassiveMembersDecoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingDecoder _dimensions = new GroupSizeEncodingDecoder();
        private ClusterMembersExtendedResponseDecoder _parentMessage;
        private IDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;
        private int _blockLength;

        public void Wrap(
            ClusterMembersExtendedResponseDecoder parentMessage, IDirectBuffer buffer)
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
            return 28;
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

        public PassiveMembersDecoder Next()
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

        public static int LeadershipTermIdId()
        {
            return 16;
        }

        public static int LeadershipTermIdSinceVersion()
        {
            return 0;
        }

        public static int LeadershipTermIdEncodingOffset()
        {
            return 0;
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
            return _buffer.GetLong(_offset + 0, ByteOrder.LittleEndian);
        }


        public static int LogPositionId()
        {
            return 17;
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


        public static int TimeOfLastAppendNsId()
        {
            return 18;
        }

        public static int TimeOfLastAppendNsSinceVersion()
        {
            return 0;
        }

        public static int TimeOfLastAppendNsEncodingOffset()
        {
            return 16;
        }

        public static int TimeOfLastAppendNsEncodingLength()
        {
            return 8;
        }

        public static string TimeOfLastAppendNsMetaAttribute(MetaAttribute metaAttribute)
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

        public static long TimeOfLastAppendNsNullValue()
        {
            return -9223372036854775808L;
        }

        public static long TimeOfLastAppendNsMinValue()
        {
            return -9223372036854775807L;
        }

        public static long TimeOfLastAppendNsMaxValue()
        {
            return 9223372036854775807L;
        }

        public long TimeOfLastAppendNs()
        {
            return _buffer.GetLong(_offset + 16, ByteOrder.LittleEndian);
        }


        public static int MemberIdId()
        {
            return 19;
        }

        public static int MemberIdSinceVersion()
        {
            return 0;
        }

        public static int MemberIdEncodingOffset()
        {
            return 24;
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
            return _buffer.GetInt(_offset + 24, ByteOrder.LittleEndian);
        }


        public static int IngressEndpointId()
        {
            return 20;
        }

        public static int IngressEndpointSinceVersion()
        {
            return 0;
        }

        public static string IngressEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string IngressEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int IngressEndpointHeaderLength()
        {
            return 4;
        }

        public int IngressEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetIngressEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetIngressEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string IngressEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int ConsensusEndpointId()
        {
            return 21;
        }

        public static int ConsensusEndpointSinceVersion()
        {
            return 0;
        }

        public static string ConsensusEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string ConsensusEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int ConsensusEndpointHeaderLength()
        {
            return 4;
        }

        public int ConsensusEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetConsensusEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetConsensusEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string ConsensusEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int LogEndpointId()
        {
            return 22;
        }

        public static int LogEndpointSinceVersion()
        {
            return 0;
        }

        public static string LogEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string LogEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int LogEndpointHeaderLength()
        {
            return 4;
        }

        public int LogEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetLogEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetLogEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string LogEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int CatchupEndpointId()
        {
            return 23;
        }

        public static int CatchupEndpointSinceVersion()
        {
            return 0;
        }

        public static string CatchupEndpointCharacterEncoding()
        {
            return "US-ASCII";
        }

        public static string CatchupEndpointMetaAttribute(MetaAttribute metaAttribute)
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

        public static int CatchupEndpointHeaderLength()
        {
            return 4;
        }

        public int CatchupEndpointLength()
        {
            int limit = _parentMessage.Limit();
            return (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
        }

        public int GetCatchupEndpoint(IMutableDirectBuffer dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int GetCatchupEndpoint(byte[] dst, int dstOffset, int length)
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            int bytesCopied = Math.Min(length, dataLength);
            _parentMessage.Limit(limit + headerLength + dataLength);
            _buffer.GetBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public string CatchupEndpoint()
        {
            int headerLength = 4;
            int limit = _parentMessage.Limit();
            int dataLength = (int)unchecked((uint)_buffer.GetInt(limit, ByteOrder.LittleEndian));
            _parentMessage.Limit(limit + headerLength + dataLength);
            byte[] tmp = new byte[dataLength];
            _buffer.GetBytes(limit + headerLength, tmp, 0, dataLength);

            return Encoding.ASCII.GetString(tmp);
        }

        public static int ArchiveEndpointId()
        {
            return 24;
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
            //Token{signal=BEGIN_FIELD, name='leadershipTermId', referencedName='null', description='null', id=16, version=0, deprecated=0, encodedLength=0, offset=0, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=0, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LeadershipTermId=");
            builder.Append(LeadershipTermId());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='logPosition', referencedName='null', description='null', id=17, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int64', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogPosition=");
            builder.Append(LogPosition());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='timeOfLastAppendNs', referencedName='null', description='null', id=18, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time since 1 Jan 1970 UTC.', id=-1, version=0, deprecated=0, encodedLength=8, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("TimeOfLastAppendNs=");
            builder.Append(TimeOfLastAppendNs());
            builder.Append('|');
            //Token{signal=BEGIN_FIELD, name='memberId', referencedName='null', description='null', id=19, version=0, deprecated=0, encodedLength=0, offset=24, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=24, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("MemberId=");
            builder.Append(MemberId());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='ingressEndpoint', referencedName='null', description='null', id=20, version=0, deprecated=0, encodedLength=0, offset=28, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("IngressEndpoint=");
            builder.Append(IngressEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='consensusEndpoint', referencedName='null', description='null', id=21, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ConsensusEndpoint=");
            builder.Append(ConsensusEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='logEndpoint', referencedName='null', description='null', id=22, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("LogEndpoint=");
            builder.Append(LogEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='catchupEndpoint', referencedName='null', description='null', id=23, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("CatchupEndpoint=");
            builder.Append(CatchupEndpoint());
            builder.Append('|');
            //Token{signal=BEGIN_VAR_DATA, name='archiveEndpoint', referencedName='null', description='null', id=24, version=0, deprecated=0, encodedLength=0, offset=-1, componentTokenCount=6, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
            builder.Append("ArchiveEndpoint=");
            builder.Append(ArchiveEndpoint());
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
        builder.Append("[ClusterMembersExtendedResponse](sbeTemplateId=");
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
        //Token{signal=BEGIN_FIELD, name='currentTimeNs', referencedName='null', description='null', id=2, version=0, deprecated=0, encodedLength=0, offset=8, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='time_t', referencedName='null', description='Epoch time since 1 Jan 1970 UTC.', id=-1, version=0, deprecated=0, encodedLength=8, offset=8, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT64, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("CurrentTimeNs=");
        builder.Append(CurrentTimeNs());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='leaderMemberId', referencedName='null', description='null', id=3, version=0, deprecated=0, encodedLength=0, offset=16, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=16, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("LeaderMemberId=");
        builder.Append(LeaderMemberId());
        builder.Append('|');
        //Token{signal=BEGIN_FIELD, name='memberId', referencedName='null', description='null', id=4, version=0, deprecated=0, encodedLength=0, offset=20, componentTokenCount=3, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        //Token{signal=ENCODING, name='int32', referencedName='null', description='null', id=-1, version=0, deprecated=0, encodedLength=4, offset=20, componentTokenCount=1, encoding=Encoding{presence=REQUIRED, primitiveType=INT32, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='unix', timeUnit=nanosecond, semanticType='null'}}
        builder.Append("MemberId=");
        builder.Append(MemberId());
        builder.Append('|');
        //Token{signal=BEGIN_GROUP, name='activeMembers', referencedName='null', description='Members of the cluster which have voting rights.', id=5, version=0, deprecated=0, encodedLength=28, offset=24, componentTokenCount=48, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("ActiveMembers=[");
        ActiveMembersDecoder ActiveMembers = this.ActiveMembers();
        if (ActiveMembers.Count() > 0)
        {
            while (ActiveMembers.HasNext())
            {
                ActiveMembers.Next().AppendTo(builder);
                builder.Append(',');
            }
            builder.Length = builder.Length - 1;
        }
        builder.Append(']');
        builder.Append('|');
        //Token{signal=BEGIN_GROUP, name='passiveMembers', referencedName='null', description='Members of the cluster which do not have voting rights but could become active members.', id=15, version=0, deprecated=0, encodedLength=28, offset=-1, componentTokenCount=48, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        builder.Append("PassiveMembers=[");
        PassiveMembersDecoder PassiveMembers = this.PassiveMembers();
        if (PassiveMembers.Count() > 0)
        {
            while (PassiveMembers.HasNext())
            {
                PassiveMembers.Next().AppendTo(builder);
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
