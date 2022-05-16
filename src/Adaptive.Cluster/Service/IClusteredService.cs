using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Interface which a service must implement to be contained in the cluster.
    /// <para>
    /// The {@code cluster} object should only be used to send messages to the cluster or schedule timers in
    /// response to other messages and timers. Sending messages and timers should not happen from cluster lifecycle
    /// methods like <seealso cref="OnStart(ICluster, Image)"/>, <seealso cref="OnRoleChange(ClusterRole)"/> or
    /// <seealso cref="OnTakeSnapshot(ExclusivePublication)"/>, or <seealso cref="OnTerminate(ICluster)"/>, except the session lifecycle
    /// methods.
    /// </para>
    /// </summary>
    public interface IClusteredService
    {
        /// <summary>
        /// Start event for the service where the service can perform any initialisation required and load snapshot state.
        /// The snapshot image can be null if no previous snapshot exists.
        /// <para>
        /// <b>Note:</b> As this is a potentially long-running operation the implementation should use
        /// <seealso cref="ICluster.IdleStrategy()"/> and then occasionally call <seealso cref="IIdleStrategy.Idle()"/> or
        /// <seealso cref="IIdleStrategy.Idle(int)"/>, especially when polling the <seealso cref="Image"/> returns 0.
        ///    
        /// </para>
        /// </summary>
        /// <param name="cluster">       with which the service can interact. </param>
        /// <param name="snapshotImage"> from which the service can load its archived state which can be null when no snapshot. </param>

        void OnStart(ICluster cluster, Image snapshotImage);

        /// <summary>
        /// A session has been opened for a client to the cluster.
        /// </summary>
        /// <param name="session">       for the client which have been opened. </param>
        /// <param name="timestamp">   at which the session was opened. </param>
        void OnSessionOpen(IClientSession session, long timestamp);

        /// <summary>
        /// A session has been closed for a client to the cluster.
        /// </summary>
        /// <param name="session">     that has been closed. </param>
        /// <param name="timestamp"> at which the session was closed. </param>
        /// <param name="closeReason"> the session was closed. </param>
        void OnSessionClose(IClientSession session, long timestamp, CloseReason closeReason);

        /// <summary>
        /// A message has been received to be processed by a clustered service.
        /// </summary>
        /// <param name="session">      for the client which sent the message. </param>
        /// <param name="timestamp"> for when the message was received. </param>
        /// <param name="buffer">      containing the message. </param>
        /// <param name="offset">      in the buffer at which the message is encoded. </param>
        /// <param name="length">      of the encoded message. </param>
        /// <param name="header">      aeron header for the incoming message. </param>
        void OnSessionMessage(
            IClientSession session,
            long timestamp,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header);

        /// <summary>
        /// A scheduled timer has expired.
        /// </summary>
        /// <param name="correlationId"> for the expired timer. </param>
        /// <param name="timestamp">   at which the timer expired. </param>
        void OnTimerEvent(long correlationId, long timestamp);

        /// <summary>
        /// The service should take a snapshot and store its state to the provided archive <seealso cref="Publication"/>.
        /// <para>
        /// <b>Note:</b> As this is a potentially long-running operation the implementation should use
        /// <seealso cref="ICluster.IdleStrategy()"/> and then occasionally call <seealso cref="IIdleStrategy.Idle()"/> or
        /// <seealso cref="IIdleStrategy.Idle(int)"/>, especially when the <seealso cref="ExclusivePublication"/> returns <seealso cref="Publication.BACK_PRESSURED"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="snapshotPublication"> to which the state should be recorded. </param>
        void OnTakeSnapshot(ExclusivePublication snapshotPublication);

        /// <summary>
        /// Notify that the cluster node has changed role.
        /// </summary>
        /// <param name="newRole"> that the node has assumed. </param>
        void OnRoleChange(ClusterRole newRole);

        /// <summary>
        /// Called when the container is going to terminate.
        /// </summary>
        /// <param name="cluster"> with which the service can interact. </param>
        void OnTerminate(ICluster cluster);

        /// <summary>
        /// An election has been successful and a leader has entered a new term.
        /// </summary>
        /// <param name="leadershipTermId">    identity for the new leadership term. </param>
        /// <param name="logPosition">         position the log has reached as the result of this message. </param>
        /// <param name="timestamp">           for the new leadership term. </param>
        /// <param name="termBaseLogPosition"> position at the beginning of the leadership term. </param>
        /// <param name="leaderMemberId">      who won the election. </param>
        /// <param name="logSessionId">        session id for the publication of the log. </param>
        /// <param name="timeUnit">            for the timestamps in the coming leadership term. </param>
        /// <param name="appVersion">          for the application configured in the consensus module. </param>
        void OnNewLeadershipTermEvent(long leadershipTermId, long logPosition, long timestamp, long termBaseLogPosition,
            int leaderMemberId, int logSessionId, ClusterTimeUnit timeUnit, int appVersion);
    }
}