using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Adapter for reading a log with a upper bound applied beyond which the consumer cannot progress.
    /// </summary>
    internal sealed class BoundedLogAdapter : IControlledFragmentHandler, IDisposable
    {
        private const int FRAGMENT_LIMIT = 100;

        private long maxLogPosition;
        private Image image;

        private readonly ClusteredServiceAgent agent;
        private readonly BufferBuilder builder = new BufferBuilder();
        private readonly MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionMessageHeaderDecoder sessionHeaderDecoder = new SessionMessageHeaderDecoder();
        private readonly TimerEventDecoder timerEventDecoder = new TimerEventDecoder();
        private readonly SessionOpenEventDecoder openEventDecoder = new SessionOpenEventDecoder();
        private readonly SessionCloseEventDecoder closeEventDecoder = new SessionCloseEventDecoder();
        private readonly ClusterActionRequestDecoder actionRequestDecoder = new ClusterActionRequestDecoder();

        private readonly NewLeadershipTermEventDecoder newLeadershipTermEventDecoder =
            new NewLeadershipTermEventDecoder();

        private readonly MembershipChangeEventDecoder membershipChangeEventDecoder = new MembershipChangeEventDecoder();

        internal BoundedLogAdapter(ClusteredServiceAgent agent)
        {
            this.agent = agent;
        }

        public void Dispose()
        {
            if (null != image)
            {
                image.Subscription?.Dispose();
                image = null;
            }
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            ControlledFragmentHandlerAction action = ControlledFragmentHandlerAction.CONTINUE;
            byte flags = header.Flags;

            if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
            {
                action = OnMessage(buffer, offset, length, header);
            }
            else
            {
                if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
                {
                    builder.Reset().Append(buffer, offset, length);
                }
                else
                {
                    int limit = builder.Limit();
                    if (limit > 0)
                    {
                        builder.Append(buffer, offset, length);

                        if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                        {
                            action = OnMessage(builder.Buffer(), 0, builder.Limit(), header);

                            if (ControlledFragmentHandlerAction.ABORT == action)
                            {
                                builder.Limit(limit);
                            }
                            else
                            {
                                builder.Reset();
                            }
                        }
                    }
                }
            }

            return action;
        }

        internal void MaxLogPosition(long position)
        {
            maxLogPosition = position;
        }

        public bool IsDone()
        {
            return image.Position >= maxLogPosition || image.IsEndOfStream || image.Closed;
        }

        internal void Image(Image image)
        {
            this.image = image;
        }

        internal Image Image()
        {
            return image;
        }

        public int Poll(long limit)
        {
            return image.BoundedControlledPoll(this, limit, FRAGMENT_LIMIT);
        }

        private ControlledFragmentHandlerAction OnMessage(IDirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.Wrap(buffer, offset);
            int templateId = messageHeaderDecoder.TemplateId();

            int schemaId = messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            if (templateId == SessionMessageHeaderEncoder.TEMPLATE_ID)
            {
                sessionHeaderDecoder.Wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.BlockLength(),
                    messageHeaderDecoder.Version());

                agent.OnSessionMessage(
                    header.Position,
                    sessionHeaderDecoder.ClusterSessionId(),
                    sessionHeaderDecoder.Timestamp(),
                    buffer,
                    offset + AeronCluster.SESSION_HEADER_LENGTH,
                    length - AeronCluster.SESSION_HEADER_LENGTH,
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

                    agent.OnTimerEvent(
                        header.Position,
                        timerEventDecoder.CorrelationId(),
                        timerEventDecoder.Timestamp()
                    );
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
                        openEventDecoder.LeadershipTermId(),
                        header.Position,
                        openEventDecoder.ClusterSessionId(),
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
                        closeEventDecoder.LeadershipTermId(),
                        header.Position,
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
                        actionRequestDecoder.LeadershipTermId(),
                        actionRequestDecoder.LogPosition(),
                        actionRequestDecoder.Timestamp(),
                        actionRequestDecoder.Action());
                    break;

                case NewLeadershipTermEventDecoder.TEMPLATE_ID:
                    newLeadershipTermEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version());

                    var clusterTimeUnit = newLeadershipTermEventDecoder.TimeUnit() == ClusterTimeUnit.NULL_VALUE
                        ? ClusterTimeUnit.MILLIS
                        : newLeadershipTermEventDecoder.TimeUnit();

                    agent.OnNewLeadershipTermEvent(
                        newLeadershipTermEventDecoder.LeadershipTermId(),
                        newLeadershipTermEventDecoder.LogPosition(),
                        newLeadershipTermEventDecoder.Timestamp(),
                        newLeadershipTermEventDecoder.TermBaseLogPosition(),
                        newLeadershipTermEventDecoder.LeaderMemberId(),
                        newLeadershipTermEventDecoder.LogSessionId(),
                        clusterTimeUnit,
                        newLeadershipTermEventDecoder.AppVersion()
                    );
                    break;

                case MembershipChangeEventDecoder.TEMPLATE_ID:
                    membershipChangeEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.BlockLength(),
                        messageHeaderDecoder.Version()
                    );

                    agent.OnMembershipChange(
                        membershipChangeEventDecoder.LogPosition(),
                        membershipChangeEventDecoder.Timestamp(),
                        membershipChangeEventDecoder.ChangeType(),
                        membershipChangeEventDecoder.MemberId());
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}