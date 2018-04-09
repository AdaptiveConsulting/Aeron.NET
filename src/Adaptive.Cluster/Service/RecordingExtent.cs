using Adaptive.Archiver;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// The extent covered by a recording in the archive in terms of position and time.
    /// </summary>
    /// <seealso cref="AeronArchive.ListRecording(long, IRecordingDescriptorConsumer)"></seealso>
    public class RecordingExtent : IRecordingDescriptorConsumer
    {
        public long recordingId;
        public long startTimestamp;
        public long stopTimestamp;
        public long startPosition;
        public long stopPosition;
        public int sessionId;

        public void OnRecordingDescriptor(long controlSessionId,
            long correlationId,
            long recordingId,
            long startTimestamp,
            long stopTimestamp,
            long startPosition,
            long stopPosition,
            int initialTermId,
            int segmentFileLength,
            int termBufferLength,
            int mtuLength,
            int sessionId,
            int streamId,
            string strippedChannel,
            string originalChannel,
            string sourceIdentity)
        {
            this.recordingId = recordingId;
            this.startTimestamp = startTimestamp;
            this.stopTimestamp = stopTimestamp;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
            this.sessionId = sessionId;
        }

        public override string ToString()
        {
            return "RecordingExtent{" + 
                   "recordingId=" + recordingId + 
                   ", startTimestamp=" + startTimestamp + 
                   ", stopTimestamp=" + stopTimestamp + 
                   ", startPosition=" + startPosition + 
                   ", stopPosition=" + stopPosition + 
                   ", sessionId=" + sessionId + 
                   '}';
        }
    }
}