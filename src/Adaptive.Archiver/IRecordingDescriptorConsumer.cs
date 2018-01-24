namespace Adaptive.Archiver
{
    /// <summary>
    /// Consumer of events describing Aeron stream recordings.
    /// </summary>
    public interface IRecordingDescriptorConsumer
    {
        /// <summary>
        /// A recording descriptor returned as a result of requesting a listing of recordings.
        /// </summary>
        /// <param name="controlSessionId">  of the originating session requesting to list recordings. </param>
        /// <param name="correlationId">     of the associated request to list recordings. </param>
        /// <param name="recordingId">       of this recording descriptor. </param>
        /// <param name="startTimestamp">    for the recording. </param>
        /// <param name="stopTimestamp">     for the recording. </param>
        /// <param name="startPosition">     for the recording against the recorded publication. </param>
        /// <param name="stopPosition">      reached for the recording. </param>
        /// <param name="initialTermId">     for the recorded publication. </param>
        /// <param name="segmentFileLength"> for the recording which is a multiple of termBufferLength. </param>
        /// <param name="termBufferLength">  for the recorded publication. </param>
        /// <param name="mtuLength">         for the recorded publication. </param>
        /// <param name="sessionId">         for the recorded publication. </param>
        /// <param name="streamId">          for the recorded publication. </param>
        /// <param name="strippedChannel">   for the recorded publication. </param>
        /// <param name="originalChannel">   for the recorded publication. </param>
        /// <param name="sourceIdentity">    for the recorded publication. </param>
        void OnRecordingDescriptor(long controlSessionId, long correlationId, long recordingId, long startTimestamp, long stopTimestamp, long startPosition, long stopPosition, int initialTermId, int segmentFileLength, int termBufferLength, int mtuLength, int sessionId, int streamId, string strippedChannel, string originalChannel, string sourceIdentity);
    }
}