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
        private readonly AdminResponseDecoder _adminResponseDecoder = new AdminResponseDecoder();
        private readonly FragmentAssembler _fragmentAssembler;
        private readonly IEgressListener _listener;
        private readonly IEgressListenerExtension _listenerExtension;
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
            int fragmentLimit) : this(listener, null, clusterSessionId, subscription, fragmentLimit)
        {
           
        }
        
        /// <summary>
        /// Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
        /// <seealso cref="IEgressListener"/> or extension messages to <seealso cref="IEgressListenerExtension"/>.
        /// </summary>
        /// <param name="listener">          to dispatch events to. </param>
        /// <param name="listenerExtension"> to dispatch extension messages to </param>
        /// <param name="clusterSessionId">  for the egress. </param>
        /// <param name="subscription">      over the egress stream. </param>
        /// <param name="fragmentLimit">     to poll on each <seealso cref="Poll()"/> operation. </param>
        public EgressAdapter(
            IEgressListener listener,
            IEgressListenerExtension listenerExtension,
            long clusterSessionId,
            Subscription subscription,
            int fragmentLimit)
        {
            _fragmentAssembler = new FragmentAssembler(this);
            
            _clusterSessionId = clusterSessionId;
            _fragmentLimit = fragmentLimit;
            _listener = listener;
            _listenerExtension = listenerExtension;
            _subscription = subscription;
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

            int templateId = _messageHeaderDecoder.TemplateId();
            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                if (_listenerExtension != null)
                {
                    _listenerExtension.OnExtensionMessage(
                        _messageHeaderDecoder.BlockLength(),
                        templateId,
                        schemaId,
                        _messageHeaderDecoder.Version(),
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        length - MessageHeaderDecoder.ENCODED_LENGTH);
                    return;
                }
                throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
            }

            switch (templateId)
            {
                case SessionMessageHeaderDecoder.TEMPLATE_ID:
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

                case AdminResponseDecoder.TEMPLATE_ID:
                {
                    _adminResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _adminResponseDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        long correlationId = _adminResponseDecoder.CorrelationId();
                        AdminRequestType requestType = _adminResponseDecoder.RequestType();
                        AdminResponseCode responseCode = _adminResponseDecoder.ResponseCode();
                        string message = _adminResponseDecoder.Message();
                        int payloadOffset = _adminResponseDecoder.Offset() +
                                            AdminResponseDecoder.BLOCK_LENGTH +
                                            AdminResponseDecoder.MessageHeaderLength() +
                                            message.Length +
                                            AdminResponseDecoder.PayloadHeaderLength();
                        int payloadLength = _adminResponseDecoder.PayloadLength();
                        _listener.OnAdminResponse(
                            sessionId,
                            correlationId,
                            requestType,
                            responseCode,
                            message,
                            buffer,
                            payloadOffset,
                            payloadLength);
                    }

                    break;
                }
            }
        }
    }
}