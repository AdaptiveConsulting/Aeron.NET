using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages coming from the cluster that also include administrative events.
    /// </summary>
    public interface IEgressListener
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
        void OnMessage(
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
        
        /// <summary>
        /// Message returned in response to an admin request.
        /// </summary>
        /// <param name="clusterSessionId"> to which the response belongs. </param>
        /// <param name="correlationId">    of the admin request. </param>
        /// <param name="requestType">      of the admin request. </param>
        /// <param name="responseCode">     describing the response. </param>
        /// <param name="message">          describing the response (e.g. error message). </param>
        /// <param name="payload">          delivered with the response, can be empty. </param>
        /// <param name="payloadOffset">    into the payload buffer. </param>
        /// <param name="payloadLength">    of the payload. </param>
        void OnAdminResponse(long clusterSessionId, long correlationId, AdminRequestType requestType,
            AdminResponseCode responseCode, string message, IDirectBuffer payload, int payloadOffset, int payloadLength);
    }
}