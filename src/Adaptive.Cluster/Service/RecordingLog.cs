using System;
using System.Collections.Generic;
using System.IO;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// An log of recordings that make up the history of a Raft log. Entries are in chronological order.
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
    ///  |         Log Position at beginning of term or snapshot         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                     Leadership Term ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |   Timestamp at beginning of term or when snapshot was taken   |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                  Entry Type (Log or Snapshot)                 |
    ///  +---------------------------------------------------------------+
    ///  |                                                               |
    ///  |                                                              ...
    /// ...                 Repeats to the end of the log                |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    /// </summary>
    public class RecordingLog
    {
        private bool InstanceFieldsInitialized = false;

        private void InitializeInstanceFields()
        {
            buffer = new UnsafeBuffer(byteBuffer);
        }

        /// <summary>
        /// A copy of the entry in the log.
        /// </summary>
        public sealed class Entry
        {
            public readonly long recordingId;
            public readonly long logPosition;
            public readonly long leadershipTermId;
            public readonly long timestamp;
            public readonly int type;

            public Entry(long recordingId, long logPosition, long leadershipTermId, long timestamp, int type)
            {
                this.recordingId = recordingId;
                this.logPosition = logPosition;
                this.leadershipTermId = leadershipTermId;
                this.timestamp = timestamp;
                this.type = type;
            }

            public override string ToString()
            {
                return "Entry{" + "recordingId=" + recordingId + ", logPosition=" + logPosition +
                       ", leadershipTermId=" + leadershipTermId + ", timestamp=" + timestamp + ", type=" + type + '}';
            }

            public void ConfirmMatch(long logPosition, long leadershipTermId, long timestamp)
            {
                if (logPosition != this.logPosition)
                {
                    throw new InvalidOperationException("Log position does not match: this=" + this.logPosition +
                                                        " that=" + logPosition);
                }

                if (leadershipTermId != this.leadershipTermId)
                {
                    throw new InvalidOperationException("Leadership term id does not match: this=" +
                                                        this.leadershipTermId + " that=" + leadershipTermId);
                }

                if (timestamp != this.timestamp)
                {
                    throw new InvalidOperationException("Timestamp does not match: this=" + this.timestamp + " that=" +
                                                        timestamp);
                }
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
                return "ReplayStep{" + ", recordingStartPosition=" + recordingStartPosition +
                       ", recordingStopPosition=" + recordingStopPosition + ", entry=" + entry + '}';
            }
        }

        /// <summary>
        /// The snapshot and steps to recover the state of a cluster.
        /// </summary>
        public class RecoveryPlan
        {
            public readonly ReplayStep snapshotStep;
            public readonly List<ReplayStep> termSteps;

            public RecoveryPlan(ReplayStep snapshotStep, List<ReplayStep> termSteps)
            {
                this.snapshotStep = snapshotStep;
                this.termSteps = termSteps;
            }
        }

        /// <summary>
        /// Filename for the recording index for the history of log terms and snapshots.
        /// </summary>
        public const string RECORDING_INDEX_FILE_NAME = "recording-index.log";

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
        /// The offset at which the absolute log position for the entry is stored.
        /// </summary>
        public static readonly int LOG_POSITION_OFFSET = RECORDING_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the leadership term id for the entry is stored.
        /// </summary>
        public static readonly int LEADERSHIP_TERM_ID_OFFSET = LOG_POSITION_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the timestamp for the entry is stored.
        /// </summary>
        public static readonly int TIMESTAMP_OFFSET = LEADERSHIP_TERM_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The offset at which the type of the entry is stored.
        /// </summary>
        public static readonly int ENTRY_TYPE_OFFSET = TIMESTAMP_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// The length of each entry.
        /// </summary>
        private static readonly int ENTRY_LENGTH =
            BitUtil.Align(ENTRY_TYPE_OFFSET + BitUtil.SIZE_OF_LONG, BitUtil.CACHE_LINE_LENGTH);

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
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }

            this.parentDir = parentDir;
            indexFile = new FileInfo(Path.Combine(parentDir.FullName, RECORDING_INDEX_FILE_NAME));

            Reload();
        }

        /// <summary>
        /// List of currently loaded entries.
        /// </summary>
        /// <returns> the list of currently loaded entries. </returns>
        public virtual IList<Entry> Entries()
        {
            return entries;
        }

        /// <summary>
        /// Reload the index from disk.
        /// </summary>
        public virtual void Reload()
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
        public virtual Entry GetLatestSnapshot()
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
            List<ReplayStep> steps = new List<ReplayStep>();

            return new RecoveryPlan(PlanRecovery(steps, entries, archive), steps);
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

                            if (recordingExtent.stopPosition == AeronArchive.NULL_POSITION ||
                                (entry.logPosition + recordingExtent.stopPosition) > snapshot.logPosition)
                            {
                                long replayRecordingFromPosition = snapshot.logPosition - entry.logPosition;
                                steps.Add(new ReplayStep(replayRecordingFromPosition, recordingExtent.stopPosition,
                                    entry));
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
        /// Get the latest snapshot for a given position.
        /// </summary>
        /// <param name="position"> to match the snapshot. </param>
        /// <returns> the latest snapshot for a given position or null if no match found. </returns>
        public virtual Entry GetSnapshotByPosition(long position)
        {
            for (int i = entries.Count - 1; i >= 0; i--)
            {
                Entry entry = entries[i];
                if (position == entry.logPosition)
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
        /// <param name="logPosition">      reached at the beginning of the term. </param>
        /// <param name="leadershipTermId"> for the current term. </param>
        /// <param name="timestamp">        at the beginning of the term. </param>
        public virtual void AppendTerm(long recordingId, long logPosition, long leadershipTermId, long timestamp)
        {
            Append(ENTRY_TYPE_TERM, recordingId, logPosition, leadershipTermId, timestamp);
        }

        /// <summary>
        /// Append an index entry for a snapshot.
        /// </summary>
        /// <param name="recordingId">      in the archive for the snapshot. </param>
        /// <param name="logPosition">      reached for the snapshot. </param>
        /// <param name="leadershipTermId"> for the current term </param>
        /// <param name="timestamp">        at which the snapshot was taken. </param>
        public virtual void AppendSnapshot(long recordingId, long logPosition, long leadershipTermId, long timestamp)
        {
            Append(ENTRY_TYPE_SNAPSHOT, recordingId, logPosition, leadershipTermId, timestamp);
        }

        private void Append(int entryType, long recordingId, long logPosition, long leadershipTermId, long timestamp)
        {
            buffer.PutLong(RECORDING_ID_OFFSET, recordingId);
            buffer.PutLong(LOG_POSITION_OFFSET, logPosition);
            buffer.PutLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
            buffer.PutLong(TIMESTAMP_OFFSET, timestamp);
            buffer.PutInt(ENTRY_TYPE_OFFSET, entryType);

            using (var fileChannel = new FileStream(indexFile.FullName, FileMode.Append, FileAccess.Write,
                FileShare.ReadWrite, 4096, FileOptions.WriteThrough))
            {
                fileChannel.WriteAsync(byteBuffer, 0, ENTRY_LENGTH);
            }

            entries.Add(new Entry(recordingId, logPosition, leadershipTermId, timestamp, entryType));
        }

        private static void CaptureEntriesFromBuffer(int limit, UnsafeBuffer buffer, List<Entry> entries)
        {
            for (int i = 0, length = limit; i < length; i += ENTRY_LENGTH)
            {
                entries.Add(new Entry(buffer.GetLong(i + RECORDING_ID_OFFSET), buffer.GetLong(i + LOG_POSITION_OFFSET),
                    buffer.GetLong(i + LEADERSHIP_TERM_ID_OFFSET), buffer.GetLong(i + TIMESTAMP_OFFSET),
                    buffer.GetInt(i + ENTRY_TYPE_OFFSET)));
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