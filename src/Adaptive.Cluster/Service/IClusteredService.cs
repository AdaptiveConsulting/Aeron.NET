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
        /// Start event for the service where the service can perform any initialisation required and load snapshot state.
        /// The snapshot image can be null if no previous snapshot exists.
        /// <para>
        /// <b>Note:</b> As this is a potentially long running operation the implementation should occasional call
        /// <seealso cref="ICluster.Idle()"/> or <seealso cref="ICluster.Idle(int)"/>, especially when polling the snapshot <seealso cref="Image"/>
        /// returns 0.
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
        void OnSessionOpen(ClientSession session, long timestamp);

        /// <summary>
        /// A session has been closed for a client to the cluster.
        /// </summary>
        /// <param name="session">     that has been closed. </param>
        /// <param name="timestamp"> at which the session was closed. </param>
        /// <param name="closeReason"> the session was closed. </param>
        void OnSessionClose(ClientSession session, long timestamp, CloseReason closeReason);

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
            ClientSession session,
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
        /// <b>Note:</b> As this is a potentially long running operation the implementation should occasional call
        /// <seealso cref="ICluster.Idle()"/> or <seealso cref="ICluster.Idle(int)"/>, especially in the event of back pressure.
        ///     
        /// </para>
        /// </summary>
        /// <param name="snapshotPublication"> to which the state should be recorded. </param>
        void OnTakeSnapshot(Publication snapshotPublication);

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
    }
}