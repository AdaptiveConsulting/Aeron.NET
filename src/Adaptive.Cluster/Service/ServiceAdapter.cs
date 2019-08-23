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
        private readonly ServiceTerminationPositionDecoder serviceTerminationPositionDecoder = new ServiceTerminationPositionDecoder();
        private readonly ElectionStartEventDecoder electionStartEventDecoder = new ElectionStartEventDecoder();
        
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

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
            }
            
            int templateId = messageHeaderDecoder.TemplateId();

            switch (templateId)
            {
                case JoinLogDecoder.TEMPLATE_ID:
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
                    break;
                
                case ServiceTerminationPositionDecoder.TEMPLATE_ID:
                    serviceTerminationPositionDecoder.Wrap(
                        buffer, 
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), 
                        messageHeaderDecoder.Version());
                
                    clusteredServiceAgent.OnServiceTerminationPosition(serviceTerminationPositionDecoder.LogPosition());
                    break;
                
                case ElectionStartEventDecoder.TEMPLATE_ID:
                    electionStartEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    clusteredServiceAgent.OnElectionStartEvent(electionStartEventDecoder.LogPosition());
                    break;
            }
        }
    }
}