using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages on a given session from the cluster.
    /// </summary>
    public interface IEgressMessageListener
    {
        /// <summary>
        /// Message event returned from the clustered service.
        /// </summary>
        /// <param name="correlationId">    to associate with the ingress message to which it is correlated. </param>
        /// <param name="clusterSessionId"> to which the message belongs. </param>
        /// <param name="timestamp">        at which the correlated ingress was sequenced in the cluster. </param>
        /// <param name="buffer">           containing the message. </param>
        /// <param name="offset">           at which the message begins. </param>
        /// <param name="length">           of the message in bytes. </param>
        /// <param name="header">           Aeron header associated with the message fragment. </param>
        void OnMessage(
            long correlationId,
            long clusterSessionId,
            long timestamp,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header);
    }
}