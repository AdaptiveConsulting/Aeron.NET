/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class ClusterMembersExtendedResponseEncoder
{
    public const ushort BLOCK_LENGTH = 24;
    public const ushort TEMPLATE_ID = 43;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 14;

    private ClusterMembersExtendedResponseEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public ClusterMembersExtendedResponseEncoder()
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

    public IMutableDirectBuffer Buffer()
    {
        return _buffer;
    }

    public int Offset()
    {
        return _offset;
    }

    public ClusterMembersExtendedResponseEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public ClusterMembersExtendedResponseEncoder WrapAndApplyHeader(
        IMutableDirectBuffer buffer, int offset, MessageHeaderEncoder headerEncoder)
    {
        headerEncoder
            .Wrap(buffer, offset)
            .BlockLength(BLOCK_LENGTH)
            .TemplateId(TEMPLATE_ID)
            .SchemaId(SCHEMA_ID)
            .Version(SCHEMA_VERSION);

        return Wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
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

    public static int CorrelationIdEncodingOffset()
    {
        return 0;
    }

    public static int CorrelationIdEncodingLength()
    {
        return 8;
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

    public ClusterMembersExtendedResponseEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CurrentTimeNsEncodingOffset()
    {
        return 8;
    }

    public static int CurrentTimeNsEncodingLength()
    {
        return 8;
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

    public ClusterMembersExtendedResponseEncoder CurrentTimeNs(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderMemberIdEncodingOffset()
    {
        return 16;
    }

    public static int LeaderMemberIdEncodingLength()
    {
        return 4;
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

    public ClusterMembersExtendedResponseEncoder LeaderMemberId(int value)
    {
        _buffer.PutInt(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int MemberIdEncodingOffset()
    {
        return 20;
    }

    public static int MemberIdEncodingLength()
    {
        return 4;
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

    public ClusterMembersExtendedResponseEncoder MemberId(int value)
    {
        _buffer.PutInt(_offset + 20, value, ByteOrder.LittleEndian);
        return this;
    }


    private ActiveMembersEncoder _ActiveMembers = new ActiveMembersEncoder();

    public static long ActiveMembersId()
    {
        return 5;
    }

    public ActiveMembersEncoder ActiveMembersCount(int count)
    {
        _ActiveMembers.Wrap(_parentMessage, _buffer, count);
        return _ActiveMembers;
    }

    public class ActiveMembersEncoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingEncoder _dimensions = new GroupSizeEncodingEncoder();
        private ClusterMembersExtendedResponseEncoder _parentMessage;
        private IMutableDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;

        public void Wrap(
            ClusterMembersExtendedResponseEncoder parentMessage, IMutableDirectBuffer buffer, int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new ArgumentException("count outside allowed range: count=" + count);
            }

            this._parentMessage = parentMessage;
            this._buffer = buffer;
            _dimensions.Wrap(buffer, parentMessage.Limit());
            _dimensions.BlockLength((ushort)28);
            _dimensions.NumInGroup((ushort)count);
            _index = -1;
            this._count = count;
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

        public ActiveMembersEncoder Next()
        {
            if (_index + 1 >= _count)
            {
                throw new IndexOutOfRangeException();
            }

            _offset = _parentMessage.Limit();
            _parentMessage.Limit(_offset + SbeBlockLength());
            ++_index;

            return this;
        }

        public static int LeadershipTermIdEncodingOffset()
        {
            return 0;
        }

        public static int LeadershipTermIdEncodingLength()
        {
            return 8;
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

        public ActiveMembersEncoder LeadershipTermId(long value)
        {
            _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int LogPositionEncodingOffset()
        {
            return 8;
        }

        public static int LogPositionEncodingLength()
        {
            return 8;
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

        public ActiveMembersEncoder LogPosition(long value)
        {
            _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TimeOfLastAppendNsEncodingOffset()
        {
            return 16;
        }

        public static int TimeOfLastAppendNsEncodingLength()
        {
            return 8;
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

        public ActiveMembersEncoder TimeOfLastAppendNs(long value)
        {
            _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int MemberIdEncodingOffset()
        {
            return 24;
        }

        public static int MemberIdEncodingLength()
        {
            return 4;
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

        public ActiveMembersEncoder MemberId(int value)
        {
            _buffer.PutInt(_offset + 24, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int IngressEndpointId()
        {
            return 10;
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

        public ActiveMembersEncoder PutIngressEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder PutIngressEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder IngressEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int ConsensusEndpointId()
        {
            return 11;
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

        public ActiveMembersEncoder PutConsensusEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder PutConsensusEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder ConsensusEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int LogEndpointId()
        {
            return 12;
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

        public ActiveMembersEncoder PutLogEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder PutLogEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder LogEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int CatchupEndpointId()
        {
            return 13;
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

        public ActiveMembersEncoder PutCatchupEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder PutCatchupEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder CatchupEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int ArchiveEndpointId()
        {
            return 14;
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

        public ActiveMembersEncoder PutArchiveEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder PutArchiveEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public ActiveMembersEncoder ArchiveEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }
    }

    private PassiveMembersEncoder _PassiveMembers = new PassiveMembersEncoder();

    public static long PassiveMembersId()
    {
        return 15;
    }

    public PassiveMembersEncoder PassiveMembersCount(int count)
    {
        _PassiveMembers.Wrap(_parentMessage, _buffer, count);
        return _PassiveMembers;
    }

    public class PassiveMembersEncoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingEncoder _dimensions = new GroupSizeEncodingEncoder();
        private ClusterMembersExtendedResponseEncoder _parentMessage;
        private IMutableDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;

        public void Wrap(
            ClusterMembersExtendedResponseEncoder parentMessage, IMutableDirectBuffer buffer, int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new ArgumentException("count outside allowed range: count=" + count);
            }

            this._parentMessage = parentMessage;
            this._buffer = buffer;
            _dimensions.Wrap(buffer, parentMessage.Limit());
            _dimensions.BlockLength((ushort)28);
            _dimensions.NumInGroup((ushort)count);
            _index = -1;
            this._count = count;
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

        public PassiveMembersEncoder Next()
        {
            if (_index + 1 >= _count)
            {
                throw new IndexOutOfRangeException();
            }

            _offset = _parentMessage.Limit();
            _parentMessage.Limit(_offset + SbeBlockLength());
            ++_index;

            return this;
        }

        public static int LeadershipTermIdEncodingOffset()
        {
            return 0;
        }

        public static int LeadershipTermIdEncodingLength()
        {
            return 8;
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

        public PassiveMembersEncoder LeadershipTermId(long value)
        {
            _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int LogPositionEncodingOffset()
        {
            return 8;
        }

        public static int LogPositionEncodingLength()
        {
            return 8;
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

        public PassiveMembersEncoder LogPosition(long value)
        {
            _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TimeOfLastAppendNsEncodingOffset()
        {
            return 16;
        }

        public static int TimeOfLastAppendNsEncodingLength()
        {
            return 8;
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

        public PassiveMembersEncoder TimeOfLastAppendNs(long value)
        {
            _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int MemberIdEncodingOffset()
        {
            return 24;
        }

        public static int MemberIdEncodingLength()
        {
            return 4;
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

        public PassiveMembersEncoder MemberId(int value)
        {
            _buffer.PutInt(_offset + 24, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int IngressEndpointId()
        {
            return 20;
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

        public PassiveMembersEncoder PutIngressEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder PutIngressEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder IngressEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int ConsensusEndpointId()
        {
            return 21;
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

        public PassiveMembersEncoder PutConsensusEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder PutConsensusEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder ConsensusEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int LogEndpointId()
        {
            return 22;
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

        public PassiveMembersEncoder PutLogEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder PutLogEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder LogEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int CatchupEndpointId()
        {
            return 23;
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

        public PassiveMembersEncoder PutCatchupEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder PutCatchupEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder CatchupEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }

        public static int ArchiveEndpointId()
        {
            return 24;
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

        public PassiveMembersEncoder PutArchiveEndpoint(IDirectBuffer src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder PutArchiveEndpoint(byte[] src, int srcOffset, int length)
        {
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutBytes(limit + headerLength, src, srcOffset, length);

            return this;
        }

        public PassiveMembersEncoder ArchiveEndpoint(string value)
        {
            int length = value.Length;
            if (length > 1073741824)
            {
                throw new InvalidOperationException("length > maxValue for type: " + length);
            }

            int headerLength = 4;
            int limit = _parentMessage.Limit();
            _parentMessage.Limit(limit + headerLength + length);
            _buffer.PutInt(limit, unchecked((int)length), ByteOrder.LittleEndian);
            _buffer.PutStringWithoutLengthAscii(limit + headerLength, value);

            return this;
        }
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        ClusterMembersExtendedResponseDecoder writer = new ClusterMembersExtendedResponseDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
