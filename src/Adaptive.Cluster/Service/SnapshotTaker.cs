using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Based class of common functions required to take a snapshot of cluster state.
    /// </summary>
    public class SnapshotTaker
    {
        protected static readonly int ENCODED_MARKER_LENGTH =
            MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;

        protected readonly BufferClaim bufferClaim = new BufferClaim();
        protected readonly MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        protected readonly ExclusivePublication publication;
        protected readonly IIdleStrategy idleStrategy;
        protected readonly AgentInvoker aeronAgentInvoker;
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

        protected static void CheckInterruptStatus()
        {
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException("unexpected interrupt during operation");
            }
        }

        protected static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED ||
                result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new AeronException("unexpected publication state: " + result);
            }
        }

        protected void CheckResultAndIdle(long result)
        {
            CheckResult(result);
            CheckInterruptStatus();
            InvokeAgentClient();
            idleStrategy.Idle();
        }

        protected void InvokeAgentClient()
        {
            aeronAgentInvoker?.Invoke();
        }
    }
}