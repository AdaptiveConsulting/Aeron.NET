/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;
using static Adaptive.Agrona.Concurrent.Status.CountersReader;

namespace Adaptive.Archiver
{
    /// <summary>
    /// The position a recording has reached when being archived.
    /// <para>
    /// Key has the following layout:
    /// <pre>
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
    ///  |                         Archive ID                            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </para>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
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

        internal const int RecordingIdOffset = 0;
        internal static readonly int SessionIdOffset = RecordingIdOffset + BitUtil.SIZE_OF_LONG;
        internal static readonly int SourceIdentityLengthOffset = SessionIdOffset + BitUtil.SIZE_OF_INT;
        internal static readonly int SourceIdentityOffset = SourceIdentityLengthOffset + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Find the active counter id for a stream based on the recording id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="recordingId">    for the active recording. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        [Obsolete("Use FindCounterIdByRecording(CountersReader, long, long).")]
        public static int FindCounterIdByRecording(CountersReader countersReader, long recordingId)
        {
            return FindCounterIdByRecording(countersReader, recordingId, Aeron.Aeron.NULL_VALUE);
        }

        /// <summary>
        /// Find the active counter id for a stream based on the recording id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="recordingId">    for the active recording. </param>
        /// <param name="archiveId"> to target specific Archive. Use <see cref="Aeron.Aeron.NULL_VALUE"/> to emulate old
        /// behaviour.</param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterIdByRecording(CountersReader countersReader, long recordingId, long archiveId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int counterId = 0, maxId = countersReader.MaxCounterId; counterId < maxId; counterId++)
            {
                var counterState = countersReader.GetCounterState(counterId);
                if (RECORD_ALLOCATED == counterState)
                {
                    if (countersReader.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
                    {
                        int keyOffset = MetaDataOffset(counterId) + KEY_OFFSET;
                        if (buffer.GetLong(keyOffset + RecordingIdOffset) == recordingId)
                        {
                            int sourceIdentityLength = buffer.GetInt(keyOffset + SourceIdentityLengthOffset);
                            int archiveIdOffset = keyOffset + SourceIdentityOffset + sourceIdentityLength;

                            if (Aeron.Aeron.NULL_VALUE == archiveId || buffer.GetLong(archiveIdOffset) == archiveId)
                            {
                                return counterId;
                            }
                        }

                        return counterId;
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
        [Obsolete("Use FindCounterIdBySession(CountersReader, int, long) instead.")]
        public static int FindCounterIdBySession(CountersReader countersReader, int sessionId)
        {
            return FindCounterIdBySession(countersReader, sessionId, Aeron.Aeron.NULL_VALUE);
        }

        /// <summary>
        /// Finds the active counter ID for a stream based on the session ID and archive ID.
        /// </summary>
        /// <param name="countersReader">The reader to search within.</param>
        /// <param name="sessionId">The session ID for the active recording.</param>
        /// <param name="archiveId">
        /// The archive ID to target a specific archive. Use <see cref="Aeron.Aeron.NULL_VALUE"/> to emulate the old
        /// behavior.
        /// </param>
        /// <returns>
        /// The counter ID if found; otherwise <see cref="CountersReader.NULL_COUNTER_ID"/>.
        /// </returns>
        /// <remarks>Since 1.44.0</remarks>
        public static int FindCounterIdBySession(CountersReader countersReader, int sessionId, long archiveId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int counterId = 0, maxId = countersReader.MaxCounterId; counterId < maxId; counterId++)
            {
                var counterState = countersReader.GetCounterState(counterId);
                if (RECORD_ALLOCATED == counterState)
                {
                    if (countersReader.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
                    {
                        int keyOffset = MetaDataOffset(counterId) + KEY_OFFSET;
                        if (buffer.GetInt(keyOffset + SessionIdOffset) == sessionId)
                        {
                            int sourceIdentityLength = buffer.GetInt(keyOffset + SourceIdentityLengthOffset);
                            int archiveIdOffset = keyOffset + SourceIdentityOffset + sourceIdentityLength;

                            if (Aeron.Aeron.NULL_VALUE == archiveId || buffer.GetLong(archiveIdOffset) == archiveId)
                            {
                                return counterId;
                            }
                        }
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
            if (
                countersReader.GetCounterState(counterId) == RECORD_ALLOCATED
                && countersReader.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID
            )
            {
                return countersReader.MetaDataBuffer.GetLong(
                    MetaDataOffset(counterId) + KEY_OFFSET + RecordingIdOffset
                );
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
            if (
                counters.GetCounterState(counterId) == RECORD_ALLOCATED
                && counters.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID
            )
            {
                int recordOffset = MetaDataOffset(counterId);
                return counters.MetaDataBuffer.GetStringAscii(
                    recordOffset + KEY_OFFSET + SourceIdentityLengthOffset
                );
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
            int recordingIdOffset = MetaDataOffset(counterId) + +KEY_OFFSET + RecordingIdOffset;

            return counters.GetCounterState(counterId) == RECORD_ALLOCATED
                && counters.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID
                && counters.MetaDataBuffer.GetLong(recordingIdOffset) == recordingId;
        }
    }
}
