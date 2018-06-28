/* Generated SBE (Simple Binary Encoding) message codec */
using System;
using System.Text;
using System.Collections.Generic;
using Adaptive.Agrona;


namespace Adaptive.Cluster.Codecs {

public class RecoveryPlanEncoder
{
    public const ushort BLOCK_LENGTH = 48;
    public const ushort TEMPLATE_ID = 62;
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

    public RecoveryPlanEncoder CorrelationId(long value)
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

    public RecoveryPlanEncoder RequestMemberId(int value)
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

    public RecoveryPlanEncoder LeaderMemberId(int value)
    {
        _buffer.PutInt(_offset + 12, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastLeadershipTermIdEncodingOffset()
    {
        return 16;
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
        _buffer.PutLong(_offset + 16, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int LastTermBaseLogPositionEncodingOffset()
    {
        return 24;
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
        _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int AppendedLogPositionEncodingOffset()
    {
        return 32;
    }

    public static int AppendedLogPositionEncodingLength()
    {
        return 8;
    }

    public static long AppendedLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long AppendedLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long AppendedLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecoveryPlanEncoder AppendedLogPosition(long value)
    {
        _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
        return this;
    }


    public static int CommittedLogPositionEncodingOffset()
    {
        return 40;
    }

    public static int CommittedLogPositionEncodingLength()
    {
        return 8;
    }

    public static long CommittedLogPositionNullValue()
    {
        return -9223372036854775808L;
    }

    public static long CommittedLogPositionMinValue()
    {
        return -9223372036854775807L;
    }

    public static long CommittedLogPositionMaxValue()
    {
        return 9223372036854775807L;
    }

    public RecoveryPlanEncoder CommittedLogPosition(long value)
    {
        _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
        return this;
    }


    private SnapshotsEncoder _Snapshots = new SnapshotsEncoder();

    public static long SnapshotsId()
    {
        return 8;
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

    }

    private LogsEncoder _Logs = new LogsEncoder();

    public static long LogsId()
    {
        return 15;
    }

    public LogsEncoder LogsCount(int count)
    {
        _Logs.Wrap(_parentMessage, _buffer, count);
        return _Logs;
    }

    public class LogsEncoder
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

        public LogsEncoder Next()
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

        public LogsEncoder RecordingId(long value)
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

        public LogsEncoder LeadershipTermId(long value)
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

        public LogsEncoder TermBaseLogPosition(long value)
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

        public LogsEncoder LogPosition(long value)
        {
            _buffer.PutLong(_offset + 24, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int StartPositionEncodingOffset()
        {
            return 32;
        }

        public static int StartPositionEncodingLength()
        {
            return 8;
        }

        public static long StartPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long StartPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long StartPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public LogsEncoder StartPosition(long value)
        {
            _buffer.PutLong(_offset + 32, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int StopPositionEncodingOffset()
        {
            return 40;
        }

        public static int StopPositionEncodingLength()
        {
            return 8;
        }

        public static long StopPositionNullValue()
        {
            return -9223372036854775808L;
        }

        public static long StopPositionMinValue()
        {
            return -9223372036854775807L;
        }

        public static long StopPositionMaxValue()
        {
            return 9223372036854775807L;
        }

        public LogsEncoder StopPosition(long value)
        {
            _buffer.PutLong(_offset + 40, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int InitialTermIdEncodingOffset()
        {
            return 48;
        }

        public static int InitialTermIdEncodingLength()
        {
            return 4;
        }

        public static int InitialTermIdNullValue()
        {
            return -2147483648;
        }

        public static int InitialTermIdMinValue()
        {
            return -2147483647;
        }

        public static int InitialTermIdMaxValue()
        {
            return 2147483647;
        }

        public LogsEncoder InitialTermId(int value)
        {
            _buffer.PutInt(_offset + 48, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int TermBufferLengthEncodingOffset()
        {
            return 52;
        }

        public static int TermBufferLengthEncodingLength()
        {
            return 4;
        }

        public static int TermBufferLengthNullValue()
        {
            return -2147483648;
        }

        public static int TermBufferLengthMinValue()
        {
            return -2147483647;
        }

        public static int TermBufferLengthMaxValue()
        {
            return 2147483647;
        }

        public LogsEncoder TermBufferLength(int value)
        {
            _buffer.PutInt(_offset + 52, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int MtuLengthEncodingOffset()
        {
            return 56;
        }

        public static int MtuLengthEncodingLength()
        {
            return 4;
        }

        public static int MtuLengthNullValue()
        {
            return -2147483648;
        }

        public static int MtuLengthMinValue()
        {
            return -2147483647;
        }

        public static int MtuLengthMaxValue()
        {
            return 2147483647;
        }

        public LogsEncoder MtuLength(int value)
        {
            _buffer.PutInt(_offset + 56, value, ByteOrder.LittleEndian);
            return this;
        }


        public static int SessionIdEncodingOffset()
        {
            return 60;
        }

        public static int SessionIdEncodingLength()
        {
            return 4;
        }

        public static int SessionIdNullValue()
        {
            return -2147483648;
        }

        public static int SessionIdMinValue()
        {
            return -2147483647;
        }

        public static int SessionIdMaxValue()
        {
            return 2147483647;
        }

        public LogsEncoder SessionId(int value)
        {
            _buffer.PutInt(_offset + 60, value, ByteOrder.LittleEndian);
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
