using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal class ServiceSnapshotTaker : SnapshotTaker
    {
        private readonly ExpandableArrayBuffer offerBuffer = new ExpandableArrayBuffer(1024);
        private readonly ClientSessionEncoder _clientSessionEncoder = new ClientSessionEncoder();

        internal ServiceSnapshotTaker(ExclusivePublication publication, IIdleStrategy idleStrategy, AgentInvoker aeronClientInvoker) 
            : base(publication, idleStrategy, aeronClientInvoker)
        {
        }

        internal void SnapshotSession(IClientSession session)
        {
            string responseChannel = session.ResponseChannel;
            byte[] encodedPrincipal = session.EncodedPrincipal;
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH + 
                         ClientSessionEncoder.ResponseChannelHeaderLength() + responseChannel.Length + 
                         ClientSessionEncoder.EncodedPrincipalHeaderLength() + encodedPrincipal.Length;

            if (length <= publication.MaxPayloadLength)
            {
                idleStrategy.Reset();
                while (true)
                {
                    long result = publication.TryClaim(length, bufferClaim);
                    if (result > 0)
                    {
                        IMutableDirectBuffer buffer = bufferClaim.Buffer;
                        int offset = bufferClaim.Offset;

                        EncodeSession(session, responseChannel, encodedPrincipal, buffer, offset);
                        bufferClaim.Commit();
                        break;
                    }

                    CheckResultAndIdle(result);
                }
            }
            else
            {
                const int offset = 0;
                EncodeSession(session, responseChannel, encodedPrincipal, offerBuffer, offset);
                Offer(offerBuffer, offset, length);
            }

        }
        
        private void EncodeSession(IClientSession session, string responseChannel, byte[] encodedPrincipal, IMutableDirectBuffer buffer, int offset)
        {
            _clientSessionEncoder
                .WrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
                .ClusterSessionId(session.Id)
                .ResponseStreamId(session.ResponseStreamId)
                .ResponseChannel(responseChannel)
                .PutEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.Length);
        }

    }
}