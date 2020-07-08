using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Consumer of signals representing operations applied to a recording.
    /// </summary>
    public interface IRecordingSignalConsumer
    {
        /// <summary>
        /// Signal of operation taken on a recording.
        /// </summary>
        /// <param name="controlSessionId"> that initiated the operation. </param>
        /// <param name="correlationId">    that initiated the operation would could be the replication id. </param>
        /// <param name="recordingId">      which has signalled. </param>
        /// <param name="subscriptionId">   of the Subscription associated with the recording. </param>
        /// <param name="position">         of the recorded stream at the point of signal. </param>
        /// <param name="signal">           type of the operation applied to the recording. </param>
        void OnSignal(
            long controlSessionId,
            long correlationId,
            long recordingId,
            long subscriptionId,
            long position,
            RecordingSignal signal);
    }
}