using System;
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
    ///  |                         Session ID                            |
    ///  +---------------------------------------------------------------+
    ///  |                Source Identity for the Image                  |
    ///  |                                                              ...
    /// ...                                                              |
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
        public const long NULL_RECORDING_ID = Aeron.Aeron.NULL_VALUE;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "rec-pos";

        public const int RECORDING_ID_OFFSET = 0;
        public static readonly int SESSION_ID_OFFSET = RECORDING_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        public static readonly int SOURCE_IDENTITY_LENGTH_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        public static readonly int SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;


        public static Counter Allocate(Aeron.Aeron aeron, UnsafeBuffer tempBuffer, long recordingId,
            int sessionId, int streamId, string strippedChannel, string sourceIdentity)
        {
            tempBuffer.PutLong(RECORDING_ID_OFFSET, recordingId);
            tempBuffer.PutInt(SESSION_ID_OFFSET, sessionId);

            var sourceIdentityLength = Math.Min(sourceIdentity.Length, CountersReader.MAX_KEY_LENGTH - SOURCE_IDENTITY_OFFSET);
            tempBuffer.PutStringAscii(SOURCE_IDENTITY_LENGTH_OFFSET, sourceIdentity);
            var keyLength = SOURCE_IDENTITY_OFFSET + sourceIdentityLength;

            int labelOffset = BitUtil.Align(keyLength, BitUtil.SIZE_OF_INT);
            int labelLength = 0;
            labelLength += tempBuffer.PutStringWithoutLengthAscii(labelOffset, NAME + ": ");
            labelLength += tempBuffer.PutLongAscii(labelOffset + labelLength, recordingId);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(labelOffset + labelLength, " ");
            labelLength += tempBuffer.PutIntAscii(labelOffset + labelLength, sessionId);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(labelOffset + labelLength, " ");
            labelLength += tempBuffer.PutIntAscii(labelOffset + labelLength, streamId);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(labelOffset + labelLength, " ");
            labelLength += tempBuffer.PutStringWithoutLengthAscii(labelOffset + labelLength, strippedChannel, 0, CountersReader.MAX_LABEL_LENGTH - labelLength);

            return aeron.AddCounter(RECORDING_POSITION_TYPE_ID, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength);
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
        /// Get the <seealso cref="Image.SourceIdentity()"/> for the recording.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="counterId">      for the active recording. </param>
        /// <returns> <seealso cref="Image.SourceIdentity()"/> for the recording or null if not found. </returns>
        public static string GetSourceIdentity(CountersReader countersReader, int counterId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            if (countersReader.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID)
                {
                    return buffer.GetStringAscii(recordOffset + CountersReader.KEY_OFFSET + SOURCE_IDENTITY_LENGTH_OFFSET);
                }
            }

            return null;
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