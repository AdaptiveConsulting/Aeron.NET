using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages coming from the cluster that also include administrative events in a controlled
    /// fashion like <seealso cref="ControlledFragmentHandler"/>. Only session messages my be controlled in
    /// consumption, other are consumed via <seealso cref="ControlledFragmentHandlerAction.COMMIT"/>.
    /// </summary>
    public interface IControlledEgressListener
    {
        /// <summary>
        /// Message event returned from the clustered service.
        /// </summary>
        /// <param name="clusterSessionId"> to which the message belongs. </param>
        /// <param name="timestampMs">      at which the correlated ingress was sequenced in the cluster. </param>
        /// <param name="buffer">           containing the message. </param>
        /// <param name="offset">           at which the message begins. </param>
        /// <param name="length">           of the message in bytes. </param>
        /// <param name="header">           Aeron header associated with the message fragment. </param>
        /// <returns> what action should be taken regarding advancement of the stream.</returns>
        ControlledFragmentHandlerAction OnMessage(
            long clusterSessionId,
            long timestampMs,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header);

        void SessionEvent(
            long correlationId,
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            EventCode code,
            string detail);

        void NewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, string memberEndpoints);
    }
}