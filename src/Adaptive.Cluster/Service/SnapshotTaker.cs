using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Based class of common functions required to take a snapshot of cluster state.
    /// </summary>
    public class SnapshotTaker
    {
        /// <summary>
        /// Reusable <seealso cref="MessageHeaderEncoder"/> to avoid allocation.
        /// </summary>
        protected readonly BufferClaim bufferClaim = new BufferClaim();

        /// <summary>
        /// <seealso cref="Publication"/> to which the snapshot will be written.
        /// </summary>
        protected readonly MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        /// <summary>
        /// <seealso cref="Publication"/> to which the snapshot will be written.
        /// </summary>
        protected readonly ExclusivePublication publication;

        /// <summary>
        /// <seealso cref="IIdleStrategy"/> to be called when back pressure is propagated from the <seealso cref="publication"/>.
        /// </summary>
        protected readonly IIdleStrategy idleStrategy;

        private static readonly int ENCODED_MARKER_LENGTH =
            MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;
        private readonly AgentInvoker aeronAgentInvoker;
        private readonly SnapshotMarkerEncoder snapshotMarkerEncoder = new SnapshotMarkerEncoder();

        /// <summary>
        /// Construct a <seealso cref="SnapshotTaker"/> which will encode the snapshot to a publication.
        /// </summary>
        /// <param name="publication">       into which the snapshot will be encoded. </param>
        /// <param name="idleStrategy">      to call when the publication is back pressured. </param>
        /// <param name="aeronAgentInvoker"> to call when idling so it stays active. </param>
        public SnapshotTaker(ExclusivePublication publication, IIdleStrategy idleStrategy, AgentInvoker aeronAgentInvoker)
        {
            this.publication = publication;
            this.idleStrategy = idleStrategy;
            this.aeronAgentInvoker = aeronAgentInvoker;
        }

        /// <summary>
        /// Mark the beginning of the encoded snapshot.
        /// </summary>
        /// <param name="snapshotTypeId">   type to identify snapshot within a cluster. </param>
        /// <param name="logPosition">      at which the snapshot was taken. </param>
        /// <param name="leadershipTermId"> at which the snapshot was taken. </param>
        /// <param name="snapshotIndex">    so the snapshot can be sectioned. </param>
        /// <param name="timeUnit">         of the cluster timestamps stored in the snapshot. </param>
        /// <param name="appVersion">       associated with the snapshot from <seealso cref="ClusteredServiceContainer.Context.AppVersion()"/>. </param>
        public void MarkBegin(
            long snapshotTypeId,
            long logPosition,
            long leadershipTermId,
            int snapshotIndex,
            ClusterTimeUnit timeUnit,
            int appVersion)
        {
            MarkSnapshot(
                snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.BEGIN, timeUnit, appVersion);
        }

        /// <summary>
        /// Mark the end of the encoded snapshot.
        /// </summary>
        /// <param name="snapshotTypeId">   type to identify snapshot within a cluster. </param>
        /// <param name="logPosition">      at which the snapshot was taken. </param>
        /// <param name="leadershipTermId"> at which the snapshot was taken. </param>
        /// <param name="snapshotIndex">    so the snapshot can be sectioned. </param>
        /// <param name="timeUnit">         of the cluster timestamps stored in the snapshot. </param>
        /// <param name="appVersion">       associated with the snapshot from <seealso cref="ClusteredServiceContainer.Context.AppVersion()"/>. </param>
        public void MarkEnd(
            long snapshotTypeId,
            long logPosition,
            long leadershipTermId,
            int snapshotIndex,
            ClusterTimeUnit timeUnit,
            int appVersion)
        {
            MarkSnapshot(
                snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.END, timeUnit, appVersion);
        }

        /// <summary>
        /// Generically <seealso cref="SnapshotMark"/> a snapshot.
        /// </summary>
        /// <param name="snapshotTypeId">   type to identify snapshot within a cluster. </param>
        /// <param name="logPosition">      at which the snapshot was taken. </param>
        /// <param name="leadershipTermId"> at which the snapshot was taken. </param>
        /// <param name="snapshotIndex">    so the snapshot can be sectioned. </param>
        /// <param name="snapshotMark">     which specifies the type of snapshot mark. </param>
        /// <param name="timeUnit">         of the cluster timestamps stored in the snapshot. </param>
        /// <param name="appVersion">       associated with the snapshot from <seealso cref="ClusteredServiceContainer.Context.AppVersion()"/>. </param>
        public void MarkSnapshot(
            long snapshotTypeId,
            long logPosition,
            long leadershipTermId,
            int snapshotIndex,
            SnapshotMark snapshotMark,
            ClusterTimeUnit timeUnit,
            int appVersion)
        {
            idleStrategy.Reset();
            while (true)
            {
                long result = publication.TryClaim(ENCODED_MARKER_LENGTH, bufferClaim);
                if (result > 0)
                {
                    snapshotMarkerEncoder
                        .WrapAndApplyHeader(bufferClaim.Buffer, bufferClaim.Offset, messageHeaderEncoder)
                        .TypeId(snapshotTypeId)
                        .LogPosition(logPosition)
                        .LeadershipTermId(leadershipTermId)
                        .Index(snapshotIndex)
                        .Mark(snapshotMark)
                        .TimeUnit(timeUnit)
                        .AppVersion(appVersion);

                    bufferClaim.Commit();
                    break;
                }

                CheckResultAndIdle(result);
            }
        }

        /// <summary>
        /// Check for thread interrupt and throw an <seealso cref="AgentTerminationException"/> if interrupted.
        /// </summary>
        protected static void CheckInterruptStatus()
        {
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException("interrupted");
            }
        }

        /// <summary>
        /// Check the result of offering to a publication when writing a snapshot.
        /// </summary>
        /// <param name="result"> of an offer or try claim to a publication. </param>
        protected static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED ||
                result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("unexpected publication state: " + result);
            }
        }

        /// <summary>
        /// Check the result of offering to a publication when writing a snapshot and then idle after invoking the client
        /// agent if necessary.
        /// </summary>
        /// <param name="result"> of an offer or try claim to a publication. </param>
        protected void CheckResultAndIdle(long result)
        {
            CheckResult(result);
            CheckInterruptStatus();
            InvokeAgentClient();
            idleStrategy.Idle();
        }

        /// <summary>
        /// Invoke the Aeron client agent if necessary.
        /// </summary>
        private void InvokeAgentClient()
        {
            aeronAgentInvoker?.Invoke();
        }
        
        /// <summary>
        /// Helper method to offer a message into the snapshot publication.
        /// </summary>
        /// <param name="buffer"> containing the message. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <param name="length"> of the message. </param>
        protected void Offer(IDirectBuffer buffer, int offset, int length)
        {
            idleStrategy.Reset();
            while (true)
            {
                long result = publication.Offer(buffer, offset, length);
                if (result > 0)
                {
                    break;
                }

                CheckResultAndIdle(result);
            }
        }
    }
}