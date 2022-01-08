using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    sealed class ServiceAdapter : IDisposable
    {
        private readonly Subscription subscription;
        private readonly ClusteredServiceAgent clusteredServiceAgent;
        private readonly FragmentAssembler fragmentAssembler;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly JoinLogDecoder joinLogDecoder = new JoinLogDecoder();

        private readonly ServiceTerminationPositionDecoder serviceTerminationPositionDecoder =
            new ServiceTerminationPositionDecoder();

        public ServiceAdapter(Subscription subscription, ClusteredServiceAgent clusteredServiceAgent)
        {
            this.subscription = subscription;
            this.clusteredServiceAgent = clusteredServiceAgent;
            this.fragmentAssembler = new FragmentAssembler(OnFragment);
        }

        public void Dispose()
        {
            subscription?.Dispose();
        }

        internal int Poll()
        {
            return subscription.Poll(fragmentAssembler, 10);
        }

        private void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            switch (messageHeaderDecoder.TemplateId())
            {
                case JoinLogDecoder.TEMPLATE_ID:
                    joinLogDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    clusteredServiceAgent.OnJoinLog(
                        joinLogDecoder.LogPosition(),
                        joinLogDecoder.MaxLogPosition(),
                        joinLogDecoder.MemberId(),
                        joinLogDecoder.LogSessionId(),
                        joinLogDecoder.LogStreamId(),
                        joinLogDecoder.IsStartup() == BooleanType.TRUE,
                        (ClusterRole)joinLogDecoder.Role(),
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
            }
        }
    }
}