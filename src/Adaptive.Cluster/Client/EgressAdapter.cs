using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Adapter for dispatching egress messages from a cluster to a <seealso cref="IEgressListener"/>.
    /// </summary>
    public class EgressAdapter : IFragmentHandler
    {
        private readonly long _clusterSessionId;
        private readonly int _fragmentLimit;
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
        private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly SessionMessageHeaderDecoder _sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
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

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" +
                                           schemaId);
            }

            int templateId = _messageHeaderDecoder.TemplateId();

            if (SessionMessageHeaderDecoder.TEMPLATE_ID == templateId)
            {
                _sessionMessageHeaderDecoder.Wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    _messageHeaderDecoder.BlockLength(),
                    _messageHeaderDecoder.Version());

                var sessionId = _sessionMessageHeaderDecoder.ClusterSessionId();
                if (sessionId == _clusterSessionId)
                {
                    _listener.OnMessage(
                        sessionId,
                        _sessionMessageHeaderDecoder.Timestamp(),
                        buffer,
                        offset + AeronCluster.SESSION_HEADER_LENGTH,
                        length - AeronCluster.SESSION_HEADER_LENGTH,
                        header);
                }

                return;
            }

            switch (templateId)
            {
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
                            _sessionEventDecoder.LeadershipTermId(),
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
                            _sessionEventDecoder.LeadershipTermId(),
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.MemberEndpoints());
                    }

                    break;
                }
            }
        }
    }
}