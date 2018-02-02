using System;
using System.Collections.Generic;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// A log of recordings that make up the history of a Raft log. Entries are in order.
    /// 
    /// The log is made up of entries of log terms or snapshots to roll up state as of a log position and leadership term.
    /// 
    /// The latest state is made up of a the latest snapshot followed by any term logs which follow. It is possible that
    /// the a snapshot is taken mid term and therefore the latest state is the snapshot plus the log of messages which
    /// begin before the snapshot but continues after it.
    /// 
    /// Record layout as follows:
    /// 
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                        Recording ID                           |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     Leadership Term ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |             Log Position at beginning of term                 |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                    Term Position/Length                       |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |   Timestamp at beginning of term or when snapshot was taken   |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                       Member ID vote                          |
    ///  +---------------------------------------------------------------+
    ///  |                 Entry Type (Log or Snapshot)                  |
    ///  +---------------------------------------------------------------+
    ///  |                                                               |
    ///  |                                                              ...
    /// ...                Repeats to the end of the log                 |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// 
    /// </summary>
    public class RecordingLog
    {
        /// <summary>
        /// A copy of the entry in the log.
        /// </summary>
        public sealed class Entry
        {
            public readonly long recordingId;
            public readonly long leadershipTermId;
            public readonly long logPosition;
            public readonly long termPosition;
            public readonly long timestamp;
            public readonly int memberIdVote;
            public readonly int type;
            public readonly int entryIndex;

            /// <summary>
            /// A new entry in the recording log.
            /// </summary>
            /// <param name="recordingId">      of the entry in an archive. </param>
            /// <param name="leadershipTermId"> of this entry. </param>
            /// <param name="logPosition">      accumulated position of the log over leadership terms for the beginning of the term. </param>
            /// <param name="termPosition">     position reached within the current leadership term, same at leadership term length. </param>
            /// <param name="timestamp">        of this entry. </param>
            /// <param name="memberIdVote">     which member this node voted for in the election. </param>
            /// <param name="type">             of the entry as a log of a term or a snapshot. </param>
            /// <param name="entryIndex">       of the entry on disk. </param>
            public Entry(
                long recordingId,
                long leadershipTermId,
                long logPosition,
                long termPosition,
                long timestamp,
                int memberIdVote,
                int type,
                int entryIndex)
            {
                this.recordingId = recordingId;
                this.leadershipTermId = leadershipTermId;
                this.logPosition = logPosition;
                this.termPosition = termPosition;
                this.timestamp = timestamp;
                this.memberIdVote = memberIdVote;
                this.type = type;
                this.entryIndex = entryIndex;
            }

            public override string ToString()
            {
                return "Entry{" + "recordingId=" + recordingId + ", leadershipTermId=" + leadershipTermId + ", logPosition=" + logPosition + ", termPosition=" + termPosition + ", timestamp=" + timestamp + ", memberIdVote=" + memberIdVote + ", type=" + type + ", entryIndex=" + entryIndex + '}';
            }
        }

        /// <summary>
        /// Steps in a recovery plan.
        /// </summary>
        public class ReplayStep
        {
            public readonly long recordingStartPosition;
            public readonly long recordingStopPosition;
            public readonly Entry entry;

            public ReplayStep(long recordingStartPosition, long recordingStopPosition, Entry entry)
            {
                this.recordingStartPosition = recordingStartPosition;
                this.recordingStopPosition = recordingStopPosition;
                this.entry = entry;
            }

            public override string ToString()
            {
                return "ReplayStep{recordingStartPosition=" + recordingStartPosition +
                       ", recordingStopPosition=" + recordingStopPosition + ", entry=" + entry + '}';
            }
        }

        /// <summary>
        /// The snapshot and steps to recover the state of a cluster.
        /// </summary>
        public class RecoveryPlan
        {
            public readonly long lastLeadershipTermId;
            public readonly long lastLogPosition;
            public readonly long lastTermPositionCommitted;
            public readonly long lastTermPositionAppended;
            public readonly ReplayStep snapshotStep;
            public readonly List<ReplayStep> termSteps;

            public RecoveryPlan(
                long lastLeadershipTermId,
                long lastLogPosition,
                long lastTermPositionCommitted,
                long lastTermPositionAppended,
                ReplayStep snapshotStep,
                List<ReplayStep> termSteps)
            {
                this.lastLeadershipTermId = lastLeadershipTermId;
                this.lastLogPosition = lastLogPosition;
                this.lastTermPositionCommitted = lastTermPositionCommitted;
                this.lastTermPositionAppended = lastTermPositionAppended;
                this.snapshotStep = snapshotStep;
                this.termSteps = termSteps;
            }

            public override string ToString()
            {
                return "RecoveryPlan{" + "lastLeadershipTermId=" + lastLeadershipTermId + ", lastLogPosition=" + lastLogPosition + ", lastTermPositionCommitted=" + lastTermPositionCommitted + ", lastTermPositionAppended=" + lastTermPositionAppended + ", snapshotStep=" + snapshotStep + ", termSteps=" + termSteps + '}';
            }
        }

        /// <summary>
        /// Filename for the recording index for the history of log terms and snapshots.
        /// </summary>
        public const string RECORDING_INDEX_FILE_NAME = "recording-index.log";

        /// <summary>
        /// Represents a value that is not set or invalid.
        /// </summary>
        public const int NULL_VALUE = -1;

        /// <summary>
        /// The index entry is for a recording of messages within a term to the consensus log.
        /// </summary>
        public const int ENTRY_TYPE_TERM = 0;

        /// <summary>
        /// The index entry is for a recording of a snapshot of state taken as of a position in the log.
        /// </summary>
        public const int ENTRY_TYPE_SNAPSHOT = 1;

        /// <summary>
        /// The offset at which the recording id for the entry is stored.
        /// </summary>
        public const int RECORDING_ID_OFFSET = 0;

        /// <summary>
        /// The offset at which the leadership term id for the entry is stored.
        /// </summary>
        public static readonly int LEADERSHIP_TERM_ID_OFFSET = RECORDING_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the absolute log position for the entry is stored.
        /// </summary>
        public static readonly int LOG_POSITION_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the term position is stored.
        /// </summary>
        public static readonly int TERM_POSITION_OFFSET = LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the timestamp for the entry is stored.
        /// </summary>
        public static readonly int TIMESTAMP_OFFSET = TERM_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the voted for member id is recorded.
        /// </summary>
        public static readonly int MEMBER_ID_VOTE_OFFSET = TIMESTAMP_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the type of the entry is stored.
        /// </summary>
        public static readonly int ENTRY_TYPE_OFFSET = MEMBER_ID_VOTE_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// The length of each entry.
        /// </summary>
        private static readonly int ENTRY_LENGTH =
            BitUtil.Align(ENTRY_TYPE_OFFSET + BitUtil.SIZE_OF_INT, BitUtil.CACHE_LINE_LENGTH);

        private int nextEntryIndex;
        private readonly DirectoryInfo parentDir;
        private readonly FileInfo indexFile;
        private readonly byte[] byteBuffer = new byte[4096];
        private UnsafeBuffer buffer;
        private readonly List<Entry> entries = new List<Entry>();

        /// <summary>
        /// Create an index that appends to an existing index or creates a new one.
        /// </summary>
        /// <param name="parentDir"> in which the index will be created. </param>
        public RecordingLog(DirectoryInfo parentDir)
        {
            buffer = new UnsafeBuffer(byteBuffer);

            this.parentDir = parentDir;
            indexFile = new FileInfo(Path.Combine(parentDir.FullName, RECORDING_INDEX_FILE_NAME));

            Reload();
        }

        /// <summary>
        /// List of currently loaded entries.
        /// </summary>
        /// <returns> the list of currently loaded entries. </returns>
        public IList<Entry> Entries()
        {
            return entries;
        }

        /// <summary>
        /// Get the next index to be used when appending an entry to the log.
        /// </summary>
        /// <returns> the next index to be used when appending an entry to the log. </returns>
        public virtual int NextEntryIndex()
        {
            return nextEntryIndex;
        }

        /// <summary>
        /// Reload the index from disk.
        /// </summary>
        public void Reload()
        {
            entries.Clear();

            FileStream fileChannel = null;
            try
            {
                bool newFile = !indexFile.Exists;

                fileChannel = new FileStream(indexFile.FullName, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                    FileShare.ReadWrite, 4096, FileOptions.WriteThrough);

                if (newFile)
                {
                    SyncDirectory(parentDir);
                    return;
                }

                // buffer clear
                nextEntryIndex = 0;
                while (true)
                {
                    int length = fileChannel.Read(byteBuffer, 0, byteBuffer.Length);
                    if (length > 0)
                    {
                        CaptureEntriesFromBuffer(length, buffer, entries);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            finally
            {
                fileChannel?.Dispose();
            }
        }

        /// <summary>
        /// Get the latest snapshot <seealso cref="Entry"/> in the index.
        /// </summary>
        /// <returns> the latest snapshot <seealso cref="Entry"/> in the index or null if no snapshot exists. </returns>
        public Entry GetLatestSnapshot()
        {
            for (int i = entries.Count - 1; i >= 0; i--)
            {
                Entry entry = entries[i];
                if (ENTRY_TYPE_SNAPSHOT == entry.type)
                {
                    return entry;
                }
            }

            return null;
        }

        /// <summary>
        /// Create a recovery plan for the cluster that when the steps are replayed will bring the cluster back to the
        /// latest stable state.
        /// </summary>
        /// <param name="archive"> to lookup recording descriptors. </param>
        /// <returns> a new <seealso cref="RecoveryPlan"/> for the cluster. </returns>
        public virtual RecoveryPlan CreateRecoveryPlan(AeronArchive archive)
        {
            var steps = new List<ReplayStep>();

            var snapshotStep = PlanRecovery(steps, entries, archive);

            long lastLeadershipTermId = -1;
            long lastLogPosition = 0;
            long lastTermPositionCommitted = -1;
            long lastTermPositionAppended = 0;

            if (null != snapshotStep)
            {
                lastLeadershipTermId = snapshotStep.entry.leadershipTermId;
                lastLogPosition = snapshotStep.entry.logPosition;
                lastTermPositionCommitted = snapshotStep.entry.termPosition;
                lastTermPositionAppended = lastTermPositionCommitted;
            }

            int size = steps.Count;
            if (size > 0)
            {
                ReplayStep replayStep = steps[size - 1];
                Entry entry = replayStep.entry;

                lastLeadershipTermId = entry.leadershipTermId;
                lastLogPosition = entry.logPosition;
                lastTermPositionCommitted = entry.termPosition;
                lastTermPositionAppended = replayStep.recordingStopPosition;
            }

            return new RecoveryPlan(lastLeadershipTermId, lastLogPosition, lastTermPositionCommitted, lastTermPositionAppended, snapshotStep, steps);
        }

        internal static ReplayStep PlanRecovery(List<ReplayStep> steps, List<Entry> entries, AeronArchive archive)
        {
            if (entries.Count == 0)
            {
                return null;
            }

            int snapshotIndex = -1;
            for (int i = entries.Count - 1; i >= 0; i--)
            {
                Entry entry = entries[i];
                if (ENTRY_TYPE_SNAPSHOT == entry.type)
                {
                    snapshotIndex = i;
                }
            }

            ReplayStep snapshotStep;
            RecordingExtent recordingExtent = new RecordingExtent();

            if (-1 != snapshotIndex)
            {
                Entry snapshot = entries[snapshotIndex];
                GetRecordingExtent(archive, recordingExtent, snapshot);

                snapshotStep = new ReplayStep(recordingExtent.startPosition, recordingExtent.stopPosition, snapshot);

                if (snapshotIndex - 1 >= 0)
                {
                    for (int i = snapshotIndex - 1; i >= 0; i--)
                    {
                        Entry entry = entries[i];
                        if (ENTRY_TYPE_TERM == entry.type)
                        {
                            GetRecordingExtent(archive, recordingExtent, snapshot);
                            long snapshotPosition = snapshot.logPosition + snapshot.termPosition;

                            if (recordingExtent.stopPosition == AeronArchive.NULL_POSITION ||
                                (entry.logPosition + recordingExtent.stopPosition) > snapshotPosition)
                            {
                                steps.Add(new ReplayStep(snapshot.termPosition, recordingExtent.stopPosition, entry));
                            }

                            break;
                        }
                    }
                }
            }
            else
            {
                snapshotStep = null;
            }

            for (int i = snapshotIndex + 1, length = entries.Count; i < length; i++)
            {
                Entry entry = entries[i];
                GetRecordingExtent(archive, recordingExtent, entry);

                steps.Add(new ReplayStep(recordingExtent.startPosition, recordingExtent.stopPosition, entry));
            }

            return snapshotStep;
        }

        /// <summary>
        /// Get the latest snapshot for a given position within a leadership term.
        /// </summary>
        /// <param name="leadershipTermId"> in which the snapshot was taken. </param>
        /// <param name="termPosition">     within the leadership term. </param>
        /// <returns> the latest snapshot for a given position or null if no match found. </returns>
        public Entry GetSnapshot(long leadershipTermId, long termPosition)
        {
            for (int i = entries.Count - 1; i >= 0; i--)
            {
                Entry entry = entries[i];
                if (entry.type == ENTRY_TYPE_SNAPSHOT && leadershipTermId == entry.leadershipTermId && termPosition == entry.termPosition)
                {
                    return entry;
                }
            }

            return null;
        }


        /// <summary>
        /// Append an index entry for a Raft term.
        /// </summary>
        /// <param name="recordingId">      in the archive for the term. </param>
        /// <param name="leadershipTermId"> for the current term. </param>
        /// <param name="logPosition">      reached at the beginning of the term. </param>
        /// <param name="timestamp">        at the beginning of the term. </param>
        /// <param name="memberIdVote">     in the leader election. </param>
        public void AppendTerm(long recordingId, long leadershipTermId, long logPosition, long timestamp, int memberIdVote)
        {
            int size = entries.Count;
            if (size > 0)
            {
                long expectedTermId = leadershipTermId - 1;
                Entry entry = entries[size - 1];

                if (entry.type != NULL_VALUE && entry.leadershipTermId != expectedTermId)
                {
                    throw new InvalidOperationException("leadershipTermId out of sequence: previous " + entry.leadershipTermId + " this " + leadershipTermId);
                }
            }

            Append(ENTRY_TYPE_TERM, recordingId, leadershipTermId, logPosition, AeronArchive.NULL_POSITION, timestamp, memberIdVote);
        }

        /// <summary>
        /// Append an index entry for a snapshot.
        /// </summary>
        /// <param name="recordingId">      in the archive for the snapshot. </param>
        /// <param name="leadershipTermId"> for the current term </param>
        /// <param name="logPosition">      at the beginning of the leadership term. </param>
        /// <param name="termPosition">     for the position in the current term or length so far for that term. </param>
        /// <param name="timestamp">        at which the snapshot was taken. </param>
        public void AppendSnapshot(long recordingId, long leadershipTermId, long logPosition, long termPosition, long timestamp)
        {
            int size = entries.Count;
            if (size > 0)
            {
                Entry entry = entries[size - 1];

                if (entry.leadershipTermId != leadershipTermId)
                {
                    throw new InvalidOperationException("leadershipTermId out of sequence: previous " + entry.leadershipTermId + " this " + leadershipTermId);
                }
            }

            Append(ENTRY_TYPE_SNAPSHOT, recordingId, leadershipTermId, logPosition, termPosition, timestamp, NULL_VALUE);
        }


        /// <summary>
        /// Commit the position reached in a leadership term before a clean shutdown.
        /// </summary>
        /// <param name="leadershipTermId"> for committing the term position reached. </param>
        /// <param name="termPosition">     reached in the leadership term. </param>
        public void CommitLeadershipTermPosition(long leadershipTermId, long termPosition)
        {
            int index = -1;
            for (int i = 0, size = entries.Count; i < size; i++)
            {
                Entry entry = entries[i];
                if (entry.leadershipTermId == leadershipTermId && entry.type == ENTRY_TYPE_TERM)
                {
                    index = entry.entryIndex;
                    break;
                }
            }

            if (-1 == index)
            {
                throw new ArgumentException("Unknown leadershipTermId: " + leadershipTermId);
            }

            buffer.PutLong(0, termPosition);
            long filePosition = (index * ENTRY_LENGTH) + TERM_POSITION_OFFSET;

            using (var fileChannel = new FileStream(indexFile.FullName, FileMode.Append, FileAccess.Write,
                FileShare.ReadWrite, BitUtil.SIZE_OF_LONG, FileOptions.WriteThrough))
            {
                fileChannel.Position = filePosition;
                fileChannel.Write(byteBuffer, 0, BitUtil.SIZE_OF_LONG); // Check 
            }
        }

        /// <summary>
        /// Tombstone an entry in the log so it is no longer valid.
        /// </summary>
        /// <param name="leadershipTermId"> to match for validation. </param>
        /// <param name="entryIndex">       reached in the leadership term. </param>
        public void TombstoneEntry(long leadershipTermId, int entryIndex)
        {
            int index = -1;
            for (int i = 0, size = entries.Count; i < size; i++)
            {
                Entry entry = entries[i];
                if (entry.leadershipTermId == leadershipTermId && entry.entryIndex == entryIndex)
                {
                    index = entry.entryIndex;
                    break;
                }
            }

            if (-1 == index)
            {
                throw new System.ArgumentException("Unknown entry index: " + entryIndex);
            }

            buffer.PutInt(0, NULL_VALUE);
            long filePosition = (index * ENTRY_LENGTH) + ENTRY_TYPE_OFFSET;

            using (var fileChannel = new FileStream(indexFile.FullName, FileMode.Append, FileAccess.Write,
                FileShare.ReadWrite, BitUtil.SIZE_OF_INT, FileOptions.WriteThrough))
            {
                fileChannel.Position = filePosition;
                fileChannel.Write(byteBuffer, 0, BitUtil.SIZE_OF_INT); // Check 
            }
        }


        private void Append(int entryType, long recordingId, long leadershipTermId, long logPosition, long termPosition, long timestamp, int memberIdVote)
        {
            buffer.PutLong(RECORDING_ID_OFFSET, recordingId);
            buffer.PutLong(LOG_POSITION_OFFSET, logPosition);
            buffer.PutLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
            buffer.PutLong(TIMESTAMP_OFFSET, timestamp);
            buffer.PutLong(TERM_POSITION_OFFSET, termPosition);
            buffer.PutInt(MEMBER_ID_VOTE_OFFSET, memberIdVote);
            buffer.PutInt(ENTRY_TYPE_OFFSET, entryType);
            
            using (var fileChannel = new FileStream(indexFile.FullName, FileMode.Append, FileAccess.Write,
                FileShare.ReadWrite, 4096, FileOptions.WriteThrough))
            {
                fileChannel.Write(byteBuffer, 0, ENTRY_LENGTH);
            }

            entries.Add(new Entry(recordingId, leadershipTermId, logPosition, AeronArchive.NULL_POSITION, timestamp, memberIdVote, entryType, nextEntryIndex++));
        }

        private void CaptureEntriesFromBuffer(int limit, UnsafeBuffer buffer, List<Entry> entries)
        {
            for (int i = 0, length = limit; i < length; i += ENTRY_LENGTH)
            {
                int entryType = buffer.GetInt(i + ENTRY_TYPE_OFFSET);

                if (NULL_VALUE != entryType)
                {
                    entries.Add(new Entry(
                        buffer.GetLong(i + RECORDING_ID_OFFSET), 
                        buffer.GetLong(i + LEADERSHIP_TERM_ID_OFFSET), 
                        buffer.GetLong(i + LOG_POSITION_OFFSET), 
                        buffer.GetLong(i + TERM_POSITION_OFFSET), 
                        buffer.GetLong(i + TIMESTAMP_OFFSET), 
                        buffer.GetInt(i + MEMBER_ID_VOTE_OFFSET), 
                        entryType, 
                        nextEntryIndex));
                }

                ++nextEntryIndex;
            }
        }

        private static void SyncDirectory(DirectoryInfo dir)
        {
            try
            {
                dir.Refresh(); // No ideal whether this is doing the correct thing
            }
            catch (Exception)
            {
            }
        }

        private static void GetRecordingExtent(AeronArchive archive, RecordingExtent recordingExtent, Entry entry)
        {
            if (archive.ListRecording(entry.recordingId, recordingExtent) == 0)
            {
                throw new InvalidOperationException("Unknown recording id: " + entry.recordingId);
            }
        }
    }
}