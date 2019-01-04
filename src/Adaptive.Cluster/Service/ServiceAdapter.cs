using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    sealed class ServiceAdapter : IFragmentHandler, IDisposable
    {
        private readonly Subscription subscription;
        private readonly ClusteredServiceAgent clusteredServiceAgent;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly JoinLogDecoder joinLogDecoder = new JoinLogDecoder();
        private readonly ServiceTerminationPositionDecoder serviceTerminationPositionDecoder = new ServiceTerminationPositionDecoder();

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

            if (JoinLogDecoder.TEMPLATE_ID == templateId)
            {
                joinLogDecoder.Wrap(
                    buffer, 
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(), 
                    messageHeaderDecoder.Version());

                clusteredServiceAgent.OnJoinLog(
                    joinLogDecoder.LeadershipTermId(),
                    joinLogDecoder.LogPosition(),
                    joinLogDecoder.MaxLogPosition(),
                    joinLogDecoder.MemberId(),
                    joinLogDecoder.LogSessionId(),
                    joinLogDecoder.LogStreamId(),
                    joinLogDecoder.LogChannel());
            }
            else if (ServiceTerminationPositionDecoder.TEMPLATE_ID == templateId)
            {
                serviceTerminationPositionDecoder.Wrap(
                    buffer, 
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(), 
                    messageHeaderDecoder.Version());
                
                clusteredServiceAgent.OnServiceTerminationPosition(serviceTerminationPositionDecoder.LogPosition());
            }
        }
    }
}