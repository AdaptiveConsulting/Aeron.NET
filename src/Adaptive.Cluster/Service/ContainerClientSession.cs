using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;

namespace Adaptive.Cluster.Service
{
    public class ContainerClientSession : IClientSession
    {
        private readonly long id;
        private readonly int responseStreamId;
        private readonly string responseChannel;
        private readonly byte[] encodedPrincipal;

        private readonly ClusteredServiceAgent clusteredServiceAgent;
        private Publication responsePublication;
        private bool isClosing;

        internal ContainerClientSession(long sessionId, int responseStreamId, string responseChannel,
            byte[] encodedPrincipal, ClusteredServiceAgent clusteredServiceAgent)
        {
            this.id = sessionId;
            this.responseStreamId = responseStreamId;
            this.responseChannel = responseChannel;
            this.encodedPrincipal = encodedPrincipal;
            this.clusteredServiceAgent = clusteredServiceAgent;
        }


        public long Id => id;

        public int ResponseStreamId => responseStreamId;

        public string ResponseChannel => responseChannel;

        public byte[] EncodedPrincipal => encodedPrincipal;

        public void Close()
        {
            if (null != clusteredServiceAgent.GetClientSession(id))
            {
                clusteredServiceAgent.CloseClientSession(id);
            }
        }

        public bool IsClosing => isClosing;

        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            return clusteredServiceAgent.Offer(id, responsePublication, buffer, offset, length);
        }

        public long Offer(DirectBufferVector[] vectors)
        {
            return clusteredServiceAgent.Offer(id, responsePublication, vectors);
        }

        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            return clusteredServiceAgent.TryClaim(id, responsePublication, length, bufferClaim);
        }

        internal void Connect(Aeron.Aeron aeron)
        {
            try
            {
                if (null == responsePublication)
                {
                    responsePublication = aeron.AddPublication(responseChannel, responseStreamId);
                }
            }
            catch (RegistrationException ex)
            {
                clusteredServiceAgent.HandleError(new ClusterException(
                    "failed to connect session response publication: " + ex.Message, Category.WARN));
            }
        }

        internal void MarkClosing()
        {
            this.isClosing = true;
        }

        internal void ResetClosing()
        {
            isClosing = false;
        }

        internal void Disconnect(ErrorHandler errorHandler)
        {
            CloseHelper.Dispose(errorHandler, responsePublication);
            responsePublication = null;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "ClientSession{" +
                   "id=" + id +
                   ", responseStreamId=" + responseStreamId +
                   ", responseChannel='" + responseChannel + '\'' +
                   ", encodedPrincipal=" + encodedPrincipal +
                   ", clusteredServiceAgent=" + clusteredServiceAgent +
                   ", responsePublication=" + responsePublication +
                   ", isClosing=" + isClosing +
                   '}';
        }
    }
}