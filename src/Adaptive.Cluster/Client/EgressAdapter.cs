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

        private readonly long _clusterSessionId;
        private readonly int _fragmentLimit;
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
        private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly SessionHeaderDecoder _sessionHeaderDecoder = new SessionHeaderDecoder();
        private readonly FragmentAssembler _fragmentAssembler;
        private readonly IEgressListener _listener;
        private readonly Subscription _subscription;
        
        public EgressAdapter(
            IEgressListener listener,
            long clusterSessionId,
            Subscription subscription,
            int fragmentLimit)
        {
            _clusterSessionId = clusterSessionId;
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
                case SessionHeaderDecoder.TEMPLATE_ID:
                {
                    _sessionHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    var sessionId = _sessionHeaderDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _listener.OnMessage(
                            _sessionHeaderDecoder.CorrelationId(),
                            _sessionHeaderDecoder.ClusterSessionId(),
                            _sessionHeaderDecoder.Timestamp(),
                            buffer,
                            offset + SESSION_HEADER_LENGTH,
                            length - SESSION_HEADER_LENGTH,
                            header);
                    }
                    break;
                }
                
                case SessionEventDecoder.TEMPLATE_ID:
                {
                    _sessionEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    var sessionId = _sessionEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {

                        _listener.SessionEvent(
                            _sessionEventDecoder.CorrelationId(),
                            sessionId,
                            _sessionEventDecoder.LeaderMemberId(),
                            _sessionEventDecoder.Code(),
                            _sessionEventDecoder.Detail());
                    }

                    break;
                }

                case NewLeaderEventDecoder.TEMPLATE_ID:
                {
                    _newLeaderEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    var sessionId = _newLeaderEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _listener.NewLeader(
                            sessionId,
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.MemberEndpoints());
                    }

                    break;
                }

                case ChallengeDecoder.TEMPLATE_ID:
                    break;

                default:
                    throw new ClusterException("unknown templateId: " + templateId);
            }
        }
    }
}