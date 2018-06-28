using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    sealed class ServiceAdapter : IFragmentHandler, IDisposable
    {
        private readonly Subscription subscription;
        private readonly ClusteredServiceAgent clusteredServiceAgent;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly JoinLogDecoder joinLogDecoder = new JoinLogDecoder();

        public ServiceAdapter(Subscription subscription, ClusteredServiceAgent clusteredServiceAgent)
        {
            this.subscription = subscription;
            this.clusteredServiceAgent = clusteredServiceAgent;
        }

        public void Dispose()
        {
            subscription?.Dispose();
        }

        public int Poll()
        {
            return subscription.Poll(this, 1);
        }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = messageHeaderDecoder.TemplateId();

            if (JoinLogDecoder.TEMPLATE_ID != templateId)
            {
                throw new ClusterException("unknown template id: " + templateId);
            }

            joinLogDecoder.Wrap(
                buffer, 
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.BlockLength(), 
                messageHeaderDecoder.Version());

            clusteredServiceAgent.OnJoinLog(
                joinLogDecoder.LeadershipTermId(),
                joinLogDecoder.CommitPositionId(),
                joinLogDecoder.LogSessionId(),
                joinLogDecoder.LogStreamId(),
                joinLogDecoder.LogChannel());
        }
    }
}