using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    public class SnapshotTaker
    {
        protected static readonly int ENCODED_MARKER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;
        protected readonly BufferClaim bufferClaim = new BufferClaim();
        protected readonly MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        protected readonly Publication publication;
        protected readonly IIdleStrategy idleStrategy;
        protected readonly AgentInvoker aeronClientInvoker;
        private readonly SnapshotMarkerEncoder snapshotMarkerEncoder = new SnapshotMarkerEncoder();

        public SnapshotTaker(Publication publication, IIdleStrategy idleStrategy, AgentInvoker aeronClientInvoker)
        {
            this.publication = publication;
            this.idleStrategy = idleStrategy;
            this.aeronClientInvoker = aeronClientInvoker;
        }

        public void MarkBegin(long snapshotTypeId, long logPosition, long leadershipTermId, int snapshotIndex)
        {
            MarkSnapshot(snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.BEGIN);
        }

        public void MarkEnd(long snapshotTypeId, long logPosition, long leadershipTermId, int snapshotIndex)
        {
            MarkSnapshot(snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.END);
        }

        public void MarkSnapshot(long snapshotTypeId, long logPosition, long leadershipTermId, int snapshotIndex, SnapshotMark snapshotMark)
        {
            idleStrategy.Reset();
            while (true)
            {
                long result = publication.TryClaim(ENCODED_MARKER_LENGTH, bufferClaim);
                if (result > 0)
                {
                    snapshotMarkerEncoder.WrapAndApplyHeader(bufferClaim.Buffer, bufferClaim.Offset, messageHeaderEncoder).TypeId(snapshotTypeId).LogPosition(logPosition).LeadershipTermId(leadershipTermId).Index(snapshotIndex).Mark(snapshotMark);

                    bufferClaim.Commit();
                    break;
                }

                CheckResultAndIdle(result);
            }
        }

        protected static void CheckInterruptedStatus()
        {
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException("Unexpected interrupt during operation");
            }
        }

        protected static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new System.InvalidOperationException("Unexpected publication state: " + result);
            }
        }

        protected void CheckResultAndIdle(long result)
        {
            CheckResult(result);
            CheckInterruptedStatus();
            InvokeAeronClient();
            idleStrategy.Idle();
        }

        protected void InvokeAeronClient()
        {
            if (null != aeronClientInvoker)
            {
                aeronClientInvoker.Invoke();
            }
        }
    }
}