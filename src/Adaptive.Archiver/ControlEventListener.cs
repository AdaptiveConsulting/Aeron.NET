using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Listener for responses to requests made on the archive control channel and async notification of errors which may
    /// happen later.
    /// </summary>
    public interface IControlEventListener
    {
        /// <summary>
        /// An event has been received from the Archive in response to a request with a given correlation id.
        /// </summary>
        /// <param name="controlSessionId"> of the originating session. </param>
        /// <param name="correlationId">    of the associated request. </param>
        /// <param name="relevantId">       of the object to which the response applies. </param>
        /// <param name="code">             for the response status. </param>
        /// <param name="errorMessage">     when is set if the response code is not OK. </param>
        void OnResponse(long controlSessionId, long correlationId, long relevantId, ControlResponseCode code,
            string errorMessage);
    }
}