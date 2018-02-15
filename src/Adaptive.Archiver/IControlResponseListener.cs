using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Interface for listening to events from the archive in response to requests.
    /// </summary>
    public interface IControlResponseListener : IRecordingDescriptorConsumer
    {
        /// <summary>
        /// An event has been received from the Archive in response to a request with a given correlation id.
        /// </summary>
        /// <param name="controlSessionId"> of the originating session. </param>
        /// <param name="correlationId">    of the associated request. </param>
        /// <param name="relevantId">       of the object to which the response applies. </param>
        /// <param name="code">             for the response status. </param>
        /// <param name="errorMessage">     when is set if the response code is not OK. </param>
        void OnResponse(long controlSessionId, long correlationId, long relevantId, ControlResponseCode code, string errorMessage);
    }
}