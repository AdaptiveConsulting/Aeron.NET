using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    public sealed class ServiceControlAdapter : IFragmentHandler, IDisposable
    {
        internal readonly Subscription subscription;
        internal readonly IServiceControlListener serviceControlListener;

        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ScheduleTimerRequestDecoder scheduleTimerRequestDecoder = new ScheduleTimerRequestDecoder();
        private readonly CancelTimerRequestDecoder cancelTimerRequestDecoder = new CancelTimerRequestDecoder();
        private readonly ServiceActionAckDecoder serviceActionAckDecoder = new ServiceActionAckDecoder();
        private readonly JoinLogRequestDecoder joinLogRequestDecoder = new JoinLogRequestDecoder();

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

        public void OnFragment(UnsafeBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ScheduleTimerRequestDecoder.TEMPLATE_ID:
                    scheduleTimerRequestDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnScheduleTimer(scheduleTimerRequestDecoder.CorrelationId(),
                        scheduleTimerRequestDecoder.Deadline());
                    break;

                case CancelTimerRequestDecoder.TEMPLATE_ID:
                    cancelTimerRequestDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnCancelTimer(scheduleTimerRequestDecoder.CorrelationId());
                    break;

                case ServiceActionAckDecoder.TEMPLATE_ID:
                    serviceActionAckDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnServiceAck(serviceActionAckDecoder.LogPosition(),
                        serviceActionAckDecoder.LeadershipTermId(), serviceActionAckDecoder.ServiceId(),
                        serviceActionAckDecoder.Action());
                    break;

                case JoinLogRequestDecoder.TEMPLATE_ID:
                    joinLogRequestDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(), messageHeaderDecoder.Version());

                    serviceControlListener.OnJoinLog(joinLogRequestDecoder.LeadershipTermId(),
                        joinLogRequestDecoder.CommitPositionId(), joinLogRequestDecoder.LogSessionId(),
                        joinLogRequestDecoder.LogStreamId(), joinLogRequestDecoder.LogChannel());
                    break;

                default:
                    throw new ArgumentException("Unknown template id: " + templateId);
            }
        }
    }
}