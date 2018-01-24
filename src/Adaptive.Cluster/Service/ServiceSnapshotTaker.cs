using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ServiceSnapshotTaker : SnapshotTaker
    {
        private readonly ClientSessionEncoder _clientSessionEncoder = new ClientSessionEncoder();

        internal ServiceSnapshotTaker(Publication publication, IIdleStrategy idleStrategy, AgentInvoker aeronClientInvoker) : base(publication, idleStrategy, aeronClientInvoker)
        {
        }

        public void SnapshotSession(ClientSession session)
        {
            string responseChannel = session.ResponseChannel();
            byte[] principalData = session.PrincipalData();
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH + ClientSessionEncoder.ResponseChannelHeaderLength() + responseChannel.Length + ClientSessionEncoder.PrincipalDataHeaderLength() + principalData.Length;

            idleStrategy.Reset();
            while (true)
            {
                long result = publication.TryClaim(length, bufferClaim);
                if (result > 0)
                {
                    _clientSessionEncoder.WrapAndApplyHeader(bufferClaim.Buffer, bufferClaim.Offset, messageHeaderEncoder)
                        .ClusterSessionId(session.Id())
                        .ResponseStreamId(session.ResponseStreamId())
                        .ResponseChannel(responseChannel)
                        .PutPrincipalData(principalData, 0, principalData.Length);

                    bufferClaim.Commit();
                    break;
                }

                CheckResultAndIdle(result);
            }
        }
    }
}