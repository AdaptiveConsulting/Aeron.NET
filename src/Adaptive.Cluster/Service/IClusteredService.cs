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
    /// </summary>
    public interface IClusteredService
    {
        /// <summary>
        /// Start event for the service where the service can perform any initialisation required. This will be called
        /// before any snapshot or logs are replayed.
        /// </summary>
        /// <param name="cluster"> with which the service can interact. </param>
        void OnStart(ICluster cluster);

        /// <summary>
        /// A session has been opened for a client to the cluster.
        /// </summary>
        /// <param name="session">       for the client which have been opened. </param>
        /// <param name="timestampMs">   at which the session was opened. </param>
        void OnSessionOpen(ClientSession session, long timestampMs);

        /// <summary>
        /// A session has been closed for a client to the cluster.
        /// </summary>
        /// <param name="session">     that has been closed. </param>
        /// <param name="timestampMs"> at which the session was closed. </param>
        /// <param name="closeReason"> the session was closed. </param>
        void OnSessionClose(ClientSession session, long timestampMs, CloseReason closeReason);

        /// <summary>
        /// A message has been received to be processed by a clustered service.
        /// </summary>
        /// <param name="session">      for the client which sent the message. </param>
        /// <param name="timestampMs"> for when the message was received. </param>
        /// <param name="buffer">      containing the message. </param>
        /// <param name="offset">      in the buffer at which the message is encoded. </param>
        /// <param name="length">      of the encoded message. </param>
        /// <param name="header">      aeron header for the incoming message. </param>
        void OnSessionMessage(
            ClientSession session,
            long timestampMs,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header);

        /// <summary>
        /// A scheduled timer has expired.
        /// </summary>
        /// <param name="correlationId"> for the expired timer. </param>
        /// <param name="timestampMs">   at which the timer expired. </param>
        void OnTimerEvent(long correlationId, long timestampMs);

        /// <summary>
        /// The service should take a snapshot and store its state to the provided archive <seealso cref="Publication"/>.
        /// <para>
        /// <b>Note:</b> As this is a potentially long running operation the implementation should occasional call
        /// <seealso cref="Thread#isInterrupted()"/> and if true then throw an <seealso cref="ThreadInterruptedException"/> or
        /// <seealso cref="AgentTerminationException"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="snapshotPublication"> to which the state should be recorded. </param>
        void OnTakeSnapshot(Publication snapshotPublication);

        /// <summary>
        /// The service should load its state from a stored snapshot in the provided archived <seealso cref="Image"/>.
        /// <para>
        /// <b>Note:</b> As this is a potentially long running operation the implementation should occasional call
        /// <seealso cref="Thread.Yield()"/> and if true then throw an <seealso cref="ThreadInterruptedException"/> or
        /// <seealso cref="AgentTerminationException"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="snapshotImage"> to which the service should store its state. </param>
        void OnLoadSnapshot(Image snapshotImage);
     
        /// <summary>
        /// Notify that the cluster node has changed role.
        /// </summary>
        /// <param name="newRole"> that the node has assumed. </param>
        void OnRoleChange(ClusterRole newRole);
    }
}