using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    public class EgressAdapter : IFragmentHandler
    {
        /// <summary>
        /// Length of the session header that will be prepended to the message.
        /// </summary>
        public static readonly int SESSION_HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH + SessionHeaderDecoder.BLOCK_LENGTH;

        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
        private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly SessionHeaderDecoder _sessionHeaderDecoder = new SessionHeaderDecoder();
        private readonly FragmentAssembler _fragmentAssembler;
        private readonly IEgressListener _listener;
        private readonly Subscription _subscription;
        private readonly int _fragmentLimit;

        public EgressAdapter(IEgressListener listener, Subscription subscription, int fragmentLimit)
        {
            _fragmentAssembler = new FragmentAssembler(this);
            _listener = listener;
            _subscription = subscription;
            _fragmentLimit = fragmentLimit;
        }

        public int Poll()
        {
            return _subscription.Poll(_fragmentAssembler, _fragmentLimit);
        }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = _messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case SessionEventDecoder.TEMPLATE_ID:
                    _sessionEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    _listener.SessionEvent(
                        _sessionEventDecoder.CorrelationId(),
                        _sessionEventDecoder.ClusterSessionId(),
                        _sessionEventDecoder.Code(),
                        _sessionEventDecoder.Detail());
                    break;

                case NewLeaderEventDecoder.TEMPLATE_ID:
                    _newLeaderEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, _messageHeaderDecoder.BlockLength(), _messageHeaderDecoder.Version());

                    _listener.NewLeader(_newLeaderEventDecoder.LastCorrelationId(), _newLeaderEventDecoder.ClusterSessionId(), _newLeaderEventDecoder.LastMessageTimestamp(), _newLeaderEventDecoder.LeadershipTimestamp(), _newLeaderEventDecoder.LeadershipTermId(), _newLeaderEventDecoder.LeaderMemberId(), _newLeaderEventDecoder.MemberEndpoints());
                    break;

                case SessionHeaderDecoder.TEMPLATE_ID:
                    _sessionHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    _listener.OnMessage(
                        _sessionHeaderDecoder.CorrelationId(),
                        _sessionHeaderDecoder.ClusterSessionId(),
                        _sessionHeaderDecoder.Timestamp(),
                        buffer,
                        offset + SESSION_HEADER_LENGTH,
                        length - SESSION_HEADER_LENGTH,
                        header);
                    break;
                
                case ChallengeDecoder.TEMPLATE_ID:
                    break;

                default:
                    throw new InvalidOperationException("unknown templateId: " + templateId);
            }
        }
    }
}