/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RecoveryPlanEncoder
{
    public const ushort BLOCK_LENGTH = 32;
    public const ushort TEMPLATE_ID = 110;
    public const ushort SCHEMA_ID = 1;
    public const ushort SCHEMA_VERSION = 1;

    private RecoveryPlanEncoder _parentMessage;
    private IMutableDirectBuffer _buffer;
    protected int _offset;
    protected int _limit;

    public RecoveryPlanEncoder()
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

    public RecoveryPlanEncoder Wrap(IMutableDirectBuffer buffer, int offset)
    {
        this._buffer = buffer;
        this._offset = offset;
        Limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RecoveryPlanEncoder WrapAndApplyHeader(
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

    public static int LastLeadershipTermIdEncodingOffset()
    {
        return 0;
    }

    public static int LastLeadershipTermIdEncodingLength()
    {
        return 8;
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

    public RecoveryPlanEncoder LastLeadershipTermId(long value)
    {
        _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastTermBaseLogPositionEncodingOffset()
    {
        return 8;
    }

    public static int LastTermBaseLogPositionEncodingLength()
    {
        return 8;
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

    public RecoveryPlanEncoder LastTermBaseLogPosition(long value)
    {
        _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastTermPositionCommittedEncodingOffset()
    {
        return 16;
    }

    public static int LastTermPositionCommittedEncodingLength()
    {
        return 8;
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

    public RecoveryPlanEncoder LastTermPositionCommitted(long value)
    {
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastTermPositionAppendedEncodingOffset()
    {
        return 24;
    }

    public static int LastTermPositionAppendedEncodingLength()
    {
        return 8;
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

    public RecoveryPlanEncoder LastTermPositionAppended(long value)
    {
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    private StepsEncoder _Steps = new StepsEncoder();

    public static long StepsId()
    {
        return 5;
    }

    public StepsEncoder StepsCount(int count)
    {
        _Steps.Wrap(_parentMessage, _buffer, count);
        return _Steps;
    }

    public class StepsEncoder
    {
        private static int HEADER_SIZE = 4;
        private GroupSizeEncodingEncoder _dimensions = new GroupSizeEncodingEncoder();
        private RecoveryPlanEncoder _parentMessage;
        private IMutableDirectBuffer _buffer;
        private int _count;
        private int _index;
        private int _offset;

        public void Wrap(
            RecoveryPlanEncoder parentMessage, IMutableDirectBuffer buffer, int count)
        {
            if (count < 0 || count > 65534)
            {
                throw new ArgumentException("count outside allowed range: count=" + count);
            }

            this._parentMessage = parentMessage;
            this._buffer = buffer;
            _dimensions.Wrap(buffer, parentMessage.Limit());
            _dimensions.BlockLength((ushort)68);
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
            return 68;
        }

        public StepsEncoder Next()
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

        public static int RecordingStartPositionEncodingOffset()
        {
            return 0;
        }

        public static int RecordingStartPositionEncodingLength()
        {
            return 8;
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

        public StepsEncoder RecordingStartPosition(long value)
        {
            _buffer.PutLong(_offset + 0, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int RecordingStopPositionEncodingOffset()
        {
            return 8;
        }

        public static int RecordingStopPositionEncodingLength()
        {
            return 8;
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

        public StepsEncoder RecordingStopPosition(long value)
        {
            _buffer.PutLong(_offset + 8, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int RecordingIdEncodingOffset()
        {
            return 16;
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

        public StepsEncoder RecordingId(long value)
        {
            _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int LeadershipTermIdEncodingOffset()
        {
            return 24;
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

        public StepsEncoder LeadershipTermId(long value)
        {
            _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TermBaseLogPositionEncodingOffset()
        {
            return 32;
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

        public StepsEncoder TermBaseLogPosition(long value)
        {
            _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TermPositionEncodingOffset()
        {
            return 40;
        }

        public static int TermPositionEncodingLength()
        {
            return 8;
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

        public StepsEncoder TermPosition(long value)
        {
            _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TimestampEncodingOffset()
        {
            return 48;
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

        public StepsEncoder Timestamp(long value)
        {
            _buffer.PutLong(_offset + 48, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int VotedForMemberIdEncodingOffset()
        {
            return 56;
        }

        public static int VotedForMemberIdEncodingLength()
        {
            return 4;
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

        public StepsEncoder VotedForMemberId(int value)
        {
            _buffer.PutInt(_offset + 56, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int EntryTypeEncodingOffset()
        {
            return 60;
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

        public StepsEncoder EntryType(int value)
        {
            _buffer.PutInt(_offset + 60, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int EntryIndexEncodingOffset()
        {
            return 64;
        }

        public static int EntryIndexEncodingLength()
        {
            return 4;
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

        public StepsEncoder EntryIndex(int value)
        {
            _buffer.PutInt(_offset + 64, value, ByteOrder.LittleEndian);
            return this;
        }

    }


    public override string ToString()
    {
        return AppendTo(new StringBuilder(100)).ToString();
    }

    public StringBuilder AppendTo(StringBuilder builder)
    {
        RecoveryPlanDecoder writer = new RecoveryPlanDecoder();
        writer.Wrap(_buffer, _offset, BLOCK_LENGTH, SCHEMA_VERSION);

        return writer.AppendTo(builder);
    }
}
}
