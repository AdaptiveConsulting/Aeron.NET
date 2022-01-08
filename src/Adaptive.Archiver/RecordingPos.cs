using System;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using static Adaptive.Agrona.Concurrent.Status.CountersReader;

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
        public const int RECORDING_POSITION_TYPE_ID = AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID;

        /// <summary>
        /// Represents a null recording id when not found.
        /// </summary>
        public const long NULL_RECORDING_ID = Aeron.Aeron.NULL_VALUE;

        /// <summary>
        /// Human-readable name for the counter.
        /// </summary>
        public const string NAME = "rec-pos";

        private const int RECORDING_ID_OFFSET = 0;
        private static readonly int SESSION_ID_OFFSET = RECORDING_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int SOURCE_IDENTITY_LENGTH_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

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
                var counterState = countersReader.GetCounterState(i);
                if (counterState == RECORD_ALLOCATED &&
                    countersReader.GetCounterTypeId(i) == RECORDING_POSITION_TYPE_ID)
                {
                    if (buffer.GetLong(MetaDataOffset(i) + KEY_OFFSET +
                                       RECORDING_ID_OFFSET) == recordingId)
                    {
                        return i;
                    }
                }
                else if (RECORD_UNUSED == counterState)
                {
                    break;
                }
            }

            return NULL_COUNTER_ID;
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
                int counterState = countersReader.GetCounterState(i);
                if (counterState == RECORD_ALLOCATED &&
                    countersReader.GetCounterTypeId(i) == RECORDING_POSITION_TYPE_ID)
                {
                    if (buffer.GetInt(MetaDataOffset(i) + KEY_OFFSET +
                                      SESSION_ID_OFFSET) == sessionId)
                    {
                        return i;
                    }
                }
                else if (RECORD_UNUSED == counterState)
                {
                    break;
                }
            }

            return NULL_COUNTER_ID;
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

            if (countersReader.GetCounterState(counterId) == RECORD_ALLOCATED &&
                countersReader.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
            {
                return buffer.GetLong(MetaDataOffset(counterId) + KEY_OFFSET +
                                      RECORDING_ID_OFFSET);
            }

            return NULL_RECORDING_ID;
        }

        /// <summary>
        /// Get the <seealso cref="Image.SourceIdentity()"/> for the recording.
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <param name="counterId"> for the active recording. </param>
        /// <returns> <seealso cref="Image.SourceIdentity()"/> for the recording or null if not found. </returns>
        public static string GetSourceIdentity(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == RECORD_ALLOCATED &&
                counters.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
            {
                int recordOffset = MetaDataOffset(counterId);
                return buffer.GetStringAscii(recordOffset + KEY_OFFSET + SOURCE_IDENTITY_LENGTH_OFFSET);
            }

            return null;
        }

        /// <summary>
        /// Is the recording counter still active.
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <param name="counterId">   to search for. </param>
        /// <param name="recordingId"> to confirm it is still the same value. </param>
        /// <returns> true if the counter is still active otherwise false. </returns>
        public static bool IsActive(CountersReader counters, int counterId, long recordingId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;
            int recordOffset = MetaDataOffset(counterId);

            return counters.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID &&
                   buffer.GetLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId &&
                   counters.GetCounterState(counterId) == RECORD_ALLOCATED;
        }
    }
}