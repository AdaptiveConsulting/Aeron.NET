using System.Collections.Generic;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Callback interface for receiving messages from the driver.
    /// </summary>
    internal interface IDriverListener
    {
        void OnNewPublication(string channel, int streamId, int sessionId, int publicationLimitId, string logFileName, long correlationId);

        void OnAvailableImage(int streamId, int sessionId, IDictionary<long, long> subscriberPositionMap, string logFileName, string sourceIdentity, long correlationId);

        void OnUnavailableImage(int streamId, long correlationId);

        void OnError(ErrorCode errorCode, string message, long correlationId);
    }
}