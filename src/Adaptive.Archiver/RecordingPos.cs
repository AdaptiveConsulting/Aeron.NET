using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Archiver
{
    /// <summary>
    /// The position a recording has reached when being archived.
    ///
    /// Key has the following layout:
    ///
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                        Recording ID                           |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     Control Session ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                       Correlation ID                          |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                         Session ID                            |
    ///  +---------------------------------------------------------------+
    ///  |                         Stream ID                             |
    ///  +---------------------------------------------------------------+
    /// 
    /// </summary>
    public class RecordingPos
    {
        /// <summary>
        /// Type id of a recording position counter.
        /// </summary>
        public const int RECORDING_POSITION_TYPE_ID = 100;

        /// <summary>
        /// Represents a null recording id when not found.
        /// </summary>
        public const long NULL_RECORDING_ID = -1L;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "rec-pos";

        public const int RECORDING_ID_OFFSET = 0;
        public static readonly int CONTROL_SESSION_ID_OFFSET = RECORDING_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int CORRELATION_ID_OFFSET = CONTROL_SESSION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int STREAM_ID_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        public static readonly int KEY_LENGTH = STREAM_ID_OFFSET + BitUtil.SIZE_OF_INT;

        public static Counter Allocate(Aeron.Aeron aeron, UnsafeBuffer tempBuffer, long recordingId,
            long controlSessionId,
            long correlationId, int sessionId, int streamId, string strippedChannel)
        {
            tempBuffer.PutLong(RECORDING_ID_OFFSET, recordingId);
            tempBuffer.PutLong(CONTROL_SESSION_ID_OFFSET, controlSessionId);
            tempBuffer.PutLong(CORRELATION_ID_OFFSET, correlationId);
            tempBuffer.PutInt(SESSION_ID_OFFSET, sessionId);
            tempBuffer.PutInt(STREAM_ID_OFFSET, streamId);

            int labelLength = 0;
            labelLength += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH, NAME + ": ");
            labelLength += tempBuffer.PutLongAscii(KEY_LENGTH + labelLength, recordingId);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelLength, " ");
            labelLength += tempBuffer.PutIntAscii(KEY_LENGTH + labelLength, sessionId);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelLength, " ");
            labelLength += tempBuffer.PutIntAscii(KEY_LENGTH + labelLength, streamId);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelLength, " ");
            labelLength += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelLength, strippedChannel, 0,
                CountersReader.MAX_LABEL_LENGTH - labelLength);

            return aeron.AddCounter(RECORDING_POSITION_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH,
                labelLength);
        }

        /// <summary>
        /// Find the active counter id for a stream based on the recording id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="recordingId">    for the active recording. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterIdByRecording(CountersReader countersReader, long recordingId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int i = 0, size = countersReader.MaxCounterId; i < size; i++)
            {
                if (countersReader.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                        buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId)
                    {
                        return i;
                    }
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }

        /// <summary>
        /// Count the number of counters for a given session. It is possible for different recording to exist on the
        /// same session if there are images under subscriptions with different channel and stream id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="sessionId">      to search for. </param>
        /// <returns> the count of recordings matching a session id. </returns>
        public static int CountBySession(CountersReader countersReader, int sessionId)
        {
            int count = 0;
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int i = 0, size = countersReader.MaxCounterId; i < size; i++)
            {
                if (countersReader.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                        buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
                    {
                        ++count;
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// Find the active counter id for a stream based on the session id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="sessionId">      for the active recording. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterIdBySession(CountersReader countersReader, int sessionId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int i = 0, size = countersReader.MaxCounterId; i < size; i++)
            {
                if (countersReader.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                        buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
                    {
                        return i;
                    }
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }

        /// <summary>
        /// Get the recording id for a given counter id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="counterId">      for the active recording. </param>
        /// <returns> the counter id if found otherwise <seealso cref="NULL_RECORDING_ID"/>. </returns>
        public static long GetRecordingId(CountersReader countersReader, int counterId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            if (countersReader.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID)
                {
                    return buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + RECORDING_ID_OFFSET);
                }
            }

            return NULL_RECORDING_ID;
        }

        /// <summary>
        /// Is the recording counter still active.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="counterId">      to search for. </param>
        /// <param name="recordingId">    to confirm it is still the same value. </param>
        /// <returns> true if the counter is still active otherwise false. </returns>
        public static bool IsActive(CountersReader countersReader, int counterId, long recordingId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            if (countersReader.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                return buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                       buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId;
            }

            return false;
        }
    }
}