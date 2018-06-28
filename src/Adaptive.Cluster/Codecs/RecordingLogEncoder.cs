/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RecordingLogEncoder
{
    public const ushort BLOCK_LENGTH = 20;
    public const ushort TEMPLATE_ID = 64;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private RecordingLogEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public RecordingLogEncoder()
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

    public RecordingLogEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RecordingLogEncoder WrapAndApplyHeader(
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

    public RecordingLogEncoder CorrelationId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int RequestMemberIdEncodingOffset()
    {
        return 8;
    }

    public static int RequestMemberIdEncodingLength()
    {
        return 4;
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

    public RecordingLogEncoder RequestMemberId(int value)
    {
        _buffer.PutInt(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LeaderMemberIdEncodingOffset()
    {
        return 12;
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

    public RecordingLogEncoder LeaderMemberId(int value)
    {
        _buffer.PutInt(_offset + 12, value, ByteOrder.LittleEndian);
        return this;
    }


    private EntriesEncoder _Entries = new EntriesEncoder();

    public static long EntriesId()
    {
        return 4;
    }

    public EntriesEncoder EntriesCount(int count)
    {
        _Entries.Wrap(_parentMessage, _buffer, count);
        return _Entries;
    }

    public class EntriesEncoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingEncoder _dimensions = new GroupSizeEncodingEncoder();
        private RecordingLogEncoder _parentMessage;
        private IMutableDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;

        public void Wrap(
            RecordingLogEncoder parentMessage, IMutableDirectBuffer buffer, int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new ArgumentException("count outside allowed range: count=" + count);
            }

            this._parentMessage = parentMessage;
            this._buffer = buffer;
            _dimensions.Wrap(buffer, parentMessage.Limit());
            _dimensions.BlockLength((ushort)64);
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
            return 64;
        }

        public EntriesEncoder Next()
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

        public EntriesEncoder RecordingId(long value)
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

        public EntriesEncoder LeadershipTermId(long value)
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

        public EntriesEncoder TermBaseLogPosition(long value)
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

        public EntriesEncoder LogPosition(long value)
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

        public EntriesEncoder Timestamp(long value)
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

        public EntriesEncoder ServiceId(int value)
        {
            _buffer.PutInt(_offset + 40, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int EntryTypeEncodingOffset()
        {
            return 44;
        }

        public static int EntryTypeEncodingLength()
        {
            return 4;
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

        public EntriesEncoder EntryType(int value)
        {
            _buffer.PutInt(_offset + 44, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int IsCurrentEncodingOffset()
        {
            return 48;
        }

        public static int IsCurrentEncodingLength()
        {
            return 4;
        }

        public EntriesEncoder IsCurrent(BooleanType value)
        {
            _buffer.PutInt(_offset + 48, (int)value, ByteOrder.LittleEndian);
            return this;
        }
    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        RecordingLogDecoder writer = new RecordingLogDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
