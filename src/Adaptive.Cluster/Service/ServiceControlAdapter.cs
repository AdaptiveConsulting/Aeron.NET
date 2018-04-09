using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    public sealed class ServiceControlAdapter : IFragmentHandler, IDisposable
    {
        internal readonly Subscription subscription;
        internal readonly IServiceControlListener serviceControlListener;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ScheduleTimerDecoder scheduleTimerDecoder = new ScheduleTimerDecoder();
        private readonly CancelTimerDecoder cancelTimerDecoder = new CancelTimerDecoder();
        private readonly ClusterActionAckDecoder clusterActionAckDecoder = new ClusterActionAckDecoder();
        private readonly JoinLogDecoder joinLogDecoder = new JoinLogDecoder();
        private readonly CloseSessionDecoder closeSessionDecoder = new CloseSessionDecoder();

        public ServiceControlAdapter(Subscription subscription, IServiceControlListener serviceControlListener)
        {
            this.subscription = subscription;
            this.serviceControlListener = serviceControlListener;
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
            switch (templateId)
            {
                case ScheduleTimerDecoder.TEMPLATE_ID:
                    scheduleTimerDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnScheduleTimer(scheduleTimerDecoder.CorrelationId(),
                        scheduleTimerDecoder.Deadline());
                    break;

                case CancelTimerDecoder.TEMPLATE_ID:
                    cancelTimerDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnCancelTimer(scheduleTimerDecoder.CorrelationId());
                    break;

                case ClusterActionAckDecoder.TEMPLATE_ID:
                    clusterActionAckDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnServiceAck(clusterActionAckDecoder.LogPosition(),
                        clusterActionAckDecoder.LeadershipTermId(), clusterActionAckDecoder.ServiceId(),
                        clusterActionAckDecoder.Action());
                    break;

                case JoinLogDecoder.TEMPLATE_ID:
                    joinLogDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnJoinLog(
                        joinLogDecoder.LeadershipTermId(),
                        joinLogDecoder.CommitPositionId(),
                        joinLogDecoder.LogSessionId(),
                        joinLogDecoder.LogStreamId(),
                        joinLogDecoder.AckBeforeImage() == BooleanType.TRUE,
                        joinLogDecoder.LogChannel());
                    break;
                
                case CloseSessionDecoder.TEMPLATE_ID:
                    closeSessionDecoder.Wrap(
                        buffer, 
                        offset + MessageHeaderDecoder.ENCODED_LENGTH, 
                        messageHeaderDecoder.BlockLength(), 
                        messageHeaderDecoder.Version());

                    serviceControlListener.OnServiceCloseSession(closeSessionDecoder.ClusterSessionId());
                    break;

                default:
                    throw new ArgumentException("Unknown template id: " + templateId);
            }
        }
    }
}