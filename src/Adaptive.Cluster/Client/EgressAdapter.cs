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

        /// <summary>
        /// Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
        /// <seealso cref="IEgressListener"/>.
        /// </summary>
        /// <param name="listener">         to dispatch events to. </param>
        /// <param name="clusterSessionId"> for the egress. </param>
        /// <param name="subscription">     over the egress stream. </param>
        /// <param name="fragmentLimit">    to poll on each <seealso cref="Poll()"/> operation. </param>
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

        /// <summary>
        /// Poll the egress subscription and dispatch assembled events to the <seealso cref="IEgressListener"/>.
        /// </summary>
        /// <returns> the number of fragments consumed. </returns>
        public int Poll()
        {
            return _subscription.Poll(_fragmentAssembler, _fragmentLimit);
        }

        /// <inheritdoc />
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
                        _listener.OnSessionEvent(
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
                        _listener.OnNewLeader(
                            sessionId,
                            _sessionEventDecoder.LeadershipTermId(),
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.IngressEndpoints());
                    }

                    break;
                }
            }
        }
    }
}