using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages coming from the cluster that also include administrative events in a controlled
    /// fashion like <seealso cref="ControlledFragmentHandler"/>. Only session messages may be controlled in
    /// consumption, other are consumed via <seealso cref="ControlledFragmentHandlerAction.COMMIT"/>.
    /// </summary>
    public interface IControlledEgressListener
    {
        /// <summary>
        /// Message event returned from the clustered service.
        /// </summary>
        /// <param name="clusterSessionId"> to which the message belongs. </param>
        /// <param name="timestamp">      at which the correlated ingress was sequenced in the cluster. </param>
        /// <param name="buffer">           containing the message. </param>
        /// <param name="offset">           at which the message begins. </param>
        /// <param name="length">           of the message in bytes. </param>
        /// <param name="header">           Aeron header associated with the message fragment. </param>
        /// <returns> what action should be taken regarding advancement of the stream.</returns>
        ControlledFragmentHandlerAction OnMessage(
            long clusterSessionId,
            long timestamp,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header);

        /// <summary>
        /// Session event emitted from the cluster which after connect can indicate an error or session close.
        /// </summary>
        /// <param name="correlationId">    associated with the cluster ingress. </param>
        /// <param name="clusterSessionId"> to which the event belongs. </param>
        /// <param name="leadershipTermId"> for identifying the active term of leadership </param>
        /// <param name="leaderMemberId">   identity of the active leader. </param>
        /// <param name="code">             to indicate the type of event. </param>
        /// <param name="detail">           Textual detail to explain the event. </param>
        void OnSessionEvent(
            long correlationId,
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            EventCode code,
            string detail);

        /// <summary>
        /// Event indicating a new leader has been elected.
        /// </summary>
        /// <param name="clusterSessionId"> to which the event belongs. </param>
        /// <param name="leadershipTermId"> for identifying the active term of leadership </param>
        /// <param name="leaderMemberId">   identity of the active leader. </param>
        /// <param name="ingressEndpoints">  for connecting to the cluster which can be updated due to dynamic membership. </param>
        void OnNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, string ingressEndpoints);
    }
}