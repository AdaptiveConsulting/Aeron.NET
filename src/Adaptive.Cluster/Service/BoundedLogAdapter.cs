using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Adapter for reading a log with a upper bound applied beyond which the consumer cannot progress.
    /// </summary>
    internal sealed class BoundedLogAdapter : IControlledFragmentHandler, IDisposable
    {
        private const int FRAGMENT_LIMIT = 10;
        private const int INITIAL_BUFFER_LENGTH = 4096;
        private static readonly int SESSION_HEADER_LENGTH =
            MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;

        private readonly ImageControlledFragmentAssembler fragmentAssembler;
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
        private readonly SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
        private readonly SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
        private readonly TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
        private readonly ClusterActionRequestDecoder actionRequestDecoder = new ClusterActionRequestDecoder();
        private readonly NewLeadershipTermEventDecoder newLeadershipTermEventDecoder = new NewLeadershipTermEventDecoder();

        private readonly Image image;
        private readonly ReadableCounter upperBound;
        private readonly ClusteredServiceAgent agent;

        internal BoundedLogAdapter(Image image, ReadableCounter upperBound, ClusteredServiceAgent agent)
        {
            fragmentAssembler = new ImageControlledFragmentAssembler(this, INITIAL_BUFFER_LENGTH, true);
            this.image = image;
            this.upperBound = upperBound;
            this.agent = agent;
        }

        public void Dispose()
        {
            image.Subscription?.Dispose();
        }

        public bool IsImageClosed()
        {
            return image.Closed;
        }

        public long Position()
        {
            return image.Position();
        }

        public bool IsConsumed(CountersReader counters)
        {
            return image.Position() >= CommitPos.GetMaxLogPosition(counters, upperBound.CounterId());
        }

        public int Poll()
        {
            return image.BoundedControlledPoll(fragmentAssembler, upperBound.Get(), FRAGMENT_LIMIT);
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);
            int templateId = messageHeaderDecoder.TemplateId();

            if (templateId == SessionHeaderDecoder.TEMPLATE_ID)
            {
                sessionHeaderDecoder.Wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(),
                    messageHeaderDecoder.Version());
                
                agent.OnSessionMessage(
                    sessionHeaderDecoder.ClusterSessionId(),
                    sessionHeaderDecoder.CorrelationId(),
                    sessionHeaderDecoder.Timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);

                return ControlledFragmentHandlerAction.CONTINUE;
            }


            switch (templateId)
            {
                case TimerEventDecoder.TEMPLATE_ID:
                    timerEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    agent.OnTimerEvent(timerEventDecoder.CorrelationId(), timerEventDecoder.Timestamp());
                    break;

                case SessionOpenEventDecoder.TEMPLATE_ID:
                    openEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    string responseChannel = openEventDecoder.ResponseChannel();
                    byte[] encodedPrincipal = new byte[openEventDecoder.EncodedPrincipalLength()];
                    openEventDecoder.GetEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.Length);

                    agent.OnSessionOpen(
                        openEventDecoder.ClusterSessionId(),
                        openEventDecoder.CorrelationId(),
                        openEventDecoder.Timestamp(),
                        openEventDecoder.ResponseStreamId(),
                        responseChannel,
                        encodedPrincipal);
                    break;

                case SessionCloseEventDecoder.TEMPLATE_ID:
                    closeEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    agent.OnSessionClose(
                        closeEventDecoder.ClusterSessionId(),
                        closeEventDecoder.Timestamp(),
                        closeEventDecoder.CloseReason());
                    break;

                case ClusterActionRequestDecoder.TEMPLATE_ID:
                    actionRequestDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    agent.OnServiceAction(
                        actionRequestDecoder.LogPosition(),
                        actionRequestDecoder.LeadershipTermId(),
                        actionRequestDecoder.Timestamp(),
                        actionRequestDecoder.Action());
                    break;
                
                case NewLeadershipTermEventDecoder.TEMPLATE_ID:
                    newLeadershipTermEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    agent.OnNewLeadershipTermEvent(
                        newLeadershipTermEventDecoder.LeadershipTermId(),
                        newLeadershipTermEventDecoder.LogPosition(),
                        newLeadershipTermEventDecoder.Timestamp(),
                        newLeadershipTermEventDecoder.LeaderMemberId(),
                        newLeadershipTermEventDecoder.LogSessionId());
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}