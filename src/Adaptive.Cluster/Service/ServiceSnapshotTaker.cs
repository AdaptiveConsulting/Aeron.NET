using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ServiceSnapshotTaker : SnapshotTaker
    {
        private readonly ClientSessionEncoder _clientSessionEncoder = new ClientSessionEncoder();

        internal ServiceSnapshotTaker(Publication publication, IIdleStrategy idleStrategy, AgentInvoker aeronClientInvoker) 
            : base(publication, idleStrategy, aeronClientInvoker)
        {
        }

        public void SnapshotSession(ClientSession session)
        {
            string responseChannel = session.ResponseChannel;
            byte[] encodedPrincipal = session.EncodedPrincipal;
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH + 
                         ClientSessionEncoder.ResponseChannelHeaderLength() + responseChannel.Length + 
                         ClientSessionEncoder.EncodedPrincipalHeaderLength() + encodedPrincipal.Length;

            idleStrategy.Reset();
            while (true)
            {
                long result = publication.TryClaim(length, bufferClaim);
                if (result > 0)
                {
                    _clientSessionEncoder.WrapAndApplyHeader(bufferClaim.Buffer, bufferClaim.Offset, messageHeaderEncoder)
                        .ClusterSessionId(session.Id)
                        .ResponseStreamId(session.ResponseStreamId)
                        .ResponseChannel(responseChannel)
                        .PutEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.Length);

                    bufferClaim.Commit();
                    break;
                }

                CheckResultAndIdle(result);
            }
        }
    }
}