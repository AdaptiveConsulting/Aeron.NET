/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class StandbySnapshotEncoder
{
    public const ushort BLOCK_LENGTH = 16;
    public const ushort TEMPLATE_ID = 81;
    public const ushort SCHEMA_ID = 111;
    public const ushort SCHEMA_VERSION = 14;

    private StandbySnapshotEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public StandbySnapshotEncoder()
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

    public StandbySnapshotEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public StandbySnapshotEncoder WrapAndApplyHeader(
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

    public StandbySnapshotEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int VersionEncodingOffset()
    {
        return 8;
    }

    public static int VersionEncodingLength()
    {
        return 4;
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

    public StandbySnapshotEncoder Version(int value)
    {
        _buffer.PutInt(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int ResponseStreamIdEncodingOffset()
    {
        return 12;
    }

    public static int ResponseStreamIdEncodingLength()
    {
        return 4;
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

    public StandbySnapshotEncoder ResponseStreamId(int value)
    {
        _buffer.PutInt(_offset + 12, value, ByteOrder.LittleEndian);
        return this;
    }


    private SnapshotsEncoder _Snapshots = new SnapshotsEncoder();

    public static long SnapshotsId()
    {
        return 4;
    }

    public SnapshotsEncoder SnapshotsCount(int count)
    {
        _Snapshots.Wrap(_parentMessage, _buffer, count);
        return _Snapshots;
    }

    public class SnapshotsEncoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingEncoder _dimensions = new GroupSizeEncodingEncoder();
        private StandbySnapshotEncoder _parentMessage;
        private IMutableDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;

        public void Wrap(
            StandbySnapshotEncoder parentMessage, IMutableDirectBuffer buffer, int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new ArgumentException("count outside allowed range: count=" + count);
            }

            this._parentMessage = parentMessage;
            this._buffer = buffer;
            _dimensions.Wrap(buffer, parentMessage.Limit());
            _dimensions.BlockLength((ushort)44);
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
            return 44;
        }

        public SnapshotsEncoder Next()
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

        public static int RecordingIdEncodingOffset()
        {
            return 0;
        }

        public static int RecordingIdEncodingLength()
        {
            return 8;
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

        public SnapshotsEncoder RecordingId(long value)
        {
            _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int LeadershipTermIdEncodingOffset()
        {
            return 8;
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

        public SnapshotsEncoder LeadershipTermId(long value)
        {
            _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TermBaseLogPositionEncodingOffset()
        {
            return 16;
        }

        public static int TermBaseLogPositionEncodingLength()
        {
            return 8;
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

        public SnapshotsEncoder TermBaseLogPosition(long value)
        {
            _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int LogPositionEncodingOffset()
        {
            return 24;
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

        public SnapshotsEncoder LogPosition(long value)
        {
            _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TimestampEncodingOffset()
        {
            return 32;
        }

        public static int TimestampEncodingLength()
        {
            return 8;
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

        public SnapshotsEncoder Timestamp(long value)
        {
            _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int ServiceIdEncodingOffset()
        {
            return 40;
        }

        public static int ServiceIdEncodingLength()
        {
            return 4;
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

        public SnapshotsEncoder ServiceId(int value)
        {
            _buffer.PutInt(_offset + 40, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int ArchiveEndpointId()
        {
            return 11;
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

        public SnapshotsEncoder PutArchiveEndpoint(IDirectBuffer src, int srcOffset, int length)
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

        public SnapshotsEncoder PutArchiveEndpoint(byte[] src, int srcOffset, int length)
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

        public SnapshotsEncoder ArchiveEndpoint(string value)
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

    public static int ResponseChannelId()
    {
        return 12;
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

    public StandbySnapshotEncoder PutResponseChannel(IDirectBuffer src, int srcOffset, int length)
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

    public StandbySnapshotEncoder PutResponseChannel(byte[] src, int srcOffset, int length)
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

    public StandbySnapshotEncoder ResponseChannel(string value)
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

    public static int EncodedCredentialsId()
    {
        return 13;
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

    public StandbySnapshotEncoder PutEncodedCredentials(IDirectBuffer src, int srcOffset, int length)
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

    public StandbySnapshotEncoder PutEncodedCredentials(byte[] src, int srcOffset, int length)
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


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        StandbySnapshotDecoder writer = new StandbySnapshotDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
