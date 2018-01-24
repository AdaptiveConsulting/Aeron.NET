namespace Adaptive.Archiver
{
    /// <summary>
    /// Event listener for observing the status of recordings for an Archive.
    /// </summary>
    public interface IRecordingEventsListener
    {
        /// <summary>
        /// Fired when a recording is started.
        /// </summary>
        /// <param name="recordingId">    assigned to the new recording. </param>
        /// <param name="startPosition">  in the stream at which the recording started. </param>
        /// <param name="sessionId">      of the publication being recorded. </param>
        /// <param name="streamId">       of the publication being recorded. </param>
        /// <param name="channel">        of the publication being recorded. </param>
        /// <param name="sourceIdentity"> of the publication being recorded. </param>
        void OnStart(long recordingId, long startPosition, int sessionId, int streamId, string channel, string sourceIdentity);

        /// <summary>
        /// Progress indication of an active recording.
        /// </summary>
        /// <param name="recordingId">   for which progress is being reported. </param>
        /// <param name="startPosition"> in the stream at which the recording started. </param>
        /// <param name="position">      reached in recording the publication. </param>
        void OnProgress(long recordingId, long startPosition, long position);

        /// <summary>
        /// Fired when a recording is stopped.
        /// </summary>
        /// <param name="recordingId">   of the publication that has stopped recording. </param>
        /// <param name="startPosition"> in the stream at which the recording started. </param>
        /// <param name="stopPosition">  at which the recording stopped. </param>
        void OnStop(long recordingId, long startPosition, long stopPosition);
    }
}