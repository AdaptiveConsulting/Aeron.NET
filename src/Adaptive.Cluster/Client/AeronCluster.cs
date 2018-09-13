using System;
using System.Collections.Generic;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;
using Adaptive.Aeron.Security;
using Adaptive.Agrona.Collections;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Client for interacting with an Aeron Cluster.
    /// 
    /// A client will attempt to open a session and then offer ingress messages which are replicated to clustered services
    /// for reliability. If the clustered service responds then response messages and events come back via the egress stream.
    ///
    /// Note: Instances of this class are not threadsafe.
    /// 
    /// </summary>
    public sealed class AeronCluster : IDisposable
    {
        private const int SEND_ATTEMPTS = 3;
        private const int CONNECT_FRAGMENT_LIMIT = 1;
        private const int SESSION_FRAGMENT_LIMIT = 10;

        private long _lastCorrelationId = Aeron.Aeron.NULL_VALUE;
        private long _leadershipTermId = Aeron.Aeron.NULL_VALUE;
        private readonly long _clusterSessionId;
        private int _leaderMemberId = Aeron.Aeron.NULL_VALUE;
        private readonly bool _isUnicast;
        private readonly Context _ctx;
        private readonly Aeron.Aeron _aeron;
        private readonly Subscription _subscription;
        private Publication _publication;
        private readonly INanoClock _nanoClock;
        private readonly IIdleStrategy _idleStrategy;

        private IDictionary<int, MemberEndpoint> _endpointByMemberIdMap = new DefaultDictionary<int, MemberEndpoint>();
        private readonly BufferClaim _bufferClaim = new BufferClaim();
        private readonly MessageHeaderEncoder _messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly SessionKeepAliveEncoder _sessionKeepAliveEncoder = new SessionKeepAliveEncoder();
        private readonly IngressMessageHeaderEncoder _ingressMessageHeaderEncoder = new IngressMessageHeaderEncoder();
        private readonly DirectBufferVector[] _vectors = new DirectBufferVector[2];
        private readonly DirectBufferVector _messageVector = new DirectBufferVector();
        private readonly FragmentAssembler _fragmentAssembler;
        private readonly Poller _poller;

        private class Poller : IFragmentHandler
        {
            private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
            private readonly EgressMessageHeaderDecoder _egressMessageHeaderDecoder = new EgressMessageHeaderDecoder();
            private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();

            private readonly IEgressMessageListener _egressMessageListener;
            private readonly long _clusterSessionId;
            private readonly AeronCluster _cluster;

            public Poller(IEgressMessageListener egressMessageListener, long clusterSessionId, AeronCluster cluster)
            {
                _egressMessageListener = egressMessageListener;
                _clusterSessionId = clusterSessionId;
                _cluster = cluster;
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _messageHeaderDecoder.Wrap(buffer, offset);

                int templateId = _messageHeaderDecoder.TemplateId();
                if (EgressMessageHeaderDecoder.TEMPLATE_ID == templateId)
                {
                    _egressMessageHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _egressMessageHeaderDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _egressMessageListener.OnMessage(
                            _egressMessageHeaderDecoder.CorrelationId(),
                            sessionId,
                            _egressMessageHeaderDecoder.Timestamp(),
                            buffer,
                            offset + IngressSessionDecorator.INGRESS_MESSAGE_HEADER_LENGTH,
                            length - IngressSessionDecorator.INGRESS_MESSAGE_HEADER_LENGTH,
                            header);
                    }
                }
                else if (NewLeaderEventDecoder.TEMPLATE_ID == templateId)
                {
                    _newLeaderEventDecoder.Wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(), _messageHeaderDecoder.Version());

                    long sessionId = _newLeaderEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _cluster.OnNewLeader(
                            sessionId,
                            _newLeaderEventDecoder.LeadershipTermId(),
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.MemberEndpoints());
                    }
                }
            }
        }

        /// <summary>
        /// Connect to the cluster using default configuration.
        /// </summary>
        /// <returns> allocated cluster client if the connection is successful. </returns>
        public static AeronCluster Connect()
        {
            return Connect(new Context());
        }

        /// <summary>
        /// Connect to the cluster providing <seealso cref="Aeron.Context"/> for configuration.
        /// </summary>
        /// <param name="ctx"> for configuration. </param>
        /// <returns> allocated cluster client if the connection is successful. </returns>
        public static AeronCluster Connect(Context ctx)
        {
            return new AeronCluster(ctx);
        }

        private AeronCluster(Context ctx)
        {
            _ctx = ctx;


            Subscription subscription = null;

            try
            {
                ctx.Conclude();

                _aeron = ctx.Aeron();
                _idleStrategy = ctx.IdleStrategy();
                _nanoClock = _aeron.Ctx().NanoClock();
                _isUnicast = ctx.ClusterMemberEndpoints() != null;

                subscription = _aeron.AddSubscription(ctx.EgressChannel(), ctx.EgressStreamId());
                _subscription = subscription;

                _clusterSessionId = ConnectToCluster();

                UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[IngressSessionDecorator.INGRESS_MESSAGE_HEADER_LENGTH]);
                _ingressMessageHeaderEncoder
                    .WrapAndApplyHeader(headerBuffer, 0, _messageHeaderEncoder)
                    .ClusterSessionId(_clusterSessionId)
                    .LeadershipTermId(_leadershipTermId);

                _vectors[0] = new DirectBufferVector(headerBuffer, 0, IngressSessionDecorator.INGRESS_MESSAGE_HEADER_LENGTH);
                _vectors[1] = _messageVector;

                _poller = new Poller(ctx.EgressMessageListener(), _clusterSessionId, this);
                _fragmentAssembler = new FragmentAssembler(_poller);
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    foreach (var memberEndpoint in _endpointByMemberIdMap.Values)
                    {
                        memberEndpoint.Disconnect();
                    }

                    CloseHelper.QuietDispose(_publication);
                    CloseHelper.QuietDispose(subscription);
                }

                CloseHelper.QuietDispose(ctx);
                throw;
            }
        }

        /// <summary>
        /// Close session and release associated resources.
        /// </summary>
        public void Dispose()
        {
            if (null != _publication && _publication.IsConnected)
            {
                CloseSession();
            }

            if (!_ctx.OwnsAeronClient())
            {
                _subscription.Dispose();
                _publication?.Dispose();
            }

            _ctx.Dispose();
        }

        /// <summary>
        /// Get the context used to launch this cluster client.
        /// </summary>
        /// <returns> the context used to launch this cluster client. </returns>
        public Context Ctx()
        {
            return _ctx;
        }

        /// <summary>
        /// Cluster session id for the session that was opened as the result of a successful connect.
        /// </summary>
        /// <returns> session id for the session that was opened as the result of a successful connect. </returns>
        public long ClusterSessionId()
        {
            return _clusterSessionId;
        }

        /// <summary>
        /// Leadership term identity for the cluster. Advances with changing leadership.
        /// </summary>
        /// <returns> leadership term identity for the cluster. </returns>
        public long LeadershipTermId()
        {
            return _leadershipTermId;
        }

        /// <summary>
        /// Get the current leader member id for the cluster.
        /// </summary>
        /// <returns> the current leader member id for the cluster. </returns>
        public int LeaderMemberId()
        {
            return _leaderMemberId;
        }

        /// <summary>
        /// Get the last correlation id generated for this session. Starts with <seealso cref="Aeron.NULL_VALUE"/>.
        /// </summary>
        /// <returns> the last correlation id generated for this session. </returns>
        /// <seealso cref="NextCorrelationId"/>
        public long LastCorrelationId()
        {
            return _lastCorrelationId;
        }

        /// <summary>
        /// Generate a new correlation id to be used for this session. This is not threadsafe. If you require a threadsafe
        /// correlation id generation then use <seealso cref="Aeron.NextCorrelationId()"/>.
        /// </summary>
        /// <returns> a new correlation id to be used for this session. </returns>
        /// <seealso cref="LastCorrelationId"/>
        public long NextCorrelationId()
        {
            return ++_lastCorrelationId;
        }

        /// <summary>
        /// Get the raw <seealso cref="Publication"/> for sending to the cluster.
        /// <para>
        /// This can be wrapped with a <seealso cref="IngressSessionDecorator"/> for pre-pending the cluster session header to
        /// messages.
        /// <seealso cref="SessionHeaderEncoder"/> or equivalent should be used to raw access.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the raw <seealso cref="Publication"/> for connecting to the cluster. </returns>
        public Publication IngressPublication()
        {
            return _publication;
        }

        /// <summary>
        /// Get the raw <seealso cref="Subscription"/> for receiving from the cluster.
        ///
        /// The can be wrapped with a <seealso cref="EgressAdapter"/> for dispatching events from the cluster.
        /// 
        /// </summary>
        /// <returns> the raw <seealso cref="Subscription"/> for receiving from the cluster. </returns>
        public Subscription EgressSubscription()
        {
            return _subscription;
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
        /// <para>
        /// This version of the method will set the timestamp value in the header to zero.
        ///     
        /// </para>
        /// </summary>
        /// <param name="correlationId"> to be used to identify the message to the cluster. </param>
        /// <param name="buffer">        containing message. </param>
        /// <param name="offset">        offset in the buffer at which the encoded message begins. </param>
        /// <param name="length">        in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int)"/>. </returns>
        public long Offer(long correlationId, IDirectBuffer buffer, int offset, int length)
        {
            _ingressMessageHeaderEncoder.CorrelationId(correlationId);
            _messageVector.Reset(buffer, offset, length);

            return _publication.Offer(_vectors);
        }

        /// <summary>
        /// Send a keep alive message to the cluster to keep this session open.
        /// </summary>
        /// <returns> true if successfully sent otherwise false. </returns>
        public bool SendKeepAlive()
        {
            _idleStrategy.Reset();
            int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionKeepAliveEncoder.BLOCK_LENGTH;
            int attempts = SEND_ATTEMPTS;

            while (true)
            {
                long result = _publication.TryClaim(length, _bufferClaim);

                if (result > 0)
                {
                    _sessionKeepAliveEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .ClusterSessionId(_clusterSessionId)
                        .LeadershipTermId(_leadershipTermId);

                    _bufferClaim.Commit();

                    return true;
                }

                CheckResult(result);

                if (--attempts <= 0)
                {
                    break;
                }

                _idleStrategy.Idle();
            }

            return false;
        }

        /// <summary>
        /// Poll the <seealso cref="EgressSubscription()"/> for session messages which are dispatched to
        /// <seealso cref="Context.EgressMessageListener"/>.
        /// <para>
        /// <b>Note:</b> if <seealso cref="Context.EgressMessageListener"/> is not set then a <seealso cref="ConfigurationException"/> could
        /// result.
        ///    
        /// </para>
        /// </summary>
        /// <returns> the number of fragments processed. </returns>
        public int PollEgress()
        {
            return _subscription.Poll(_fragmentAssembler, SESSION_FRAGMENT_LIMIT);
        }

        /// <summary>
        /// To be called when a new leader event is delivered. This method needs to be called when using the
        /// <seealso cref="EgressAdapter"/> or <seealso cref="EgressPoller"/> rather than <seealso cref="PollEgress()"/> method.
        /// </summary>
        /// <param name="clusterSessionId"> which must match <seealso cref="ClusterSessionId()"/>. </param>
        /// <param name="leadershipTermId"> that identifies the term for which the new leader has been elected.</param>
        /// <param name="leaderMemberId">   which has become the new leader. </param>
        /// <param name="memberEndpoints">  comma separated list of cluster members endpoints to connect to with the leader first. </param>
        public void OnNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, string memberEndpoints)
        {
            if (clusterSessionId != _clusterSessionId)
            {
                throw new ClusterException("invalid clusterSessionId=" + clusterSessionId + " expected " +
                                           _clusterSessionId);
            }


            _leadershipTermId = leadershipTermId;
            _leaderMemberId = leaderMemberId;
            _ingressMessageHeaderEncoder.LeadershipTermId(leadershipTermId);

            if (_isUnicast)
            {
                _publication?.Dispose();
                _fragmentAssembler.Clear();
                _ctx.ClusterMemberEndpoints(memberEndpoints);
                UpdateMemberEndpoints(memberEndpoints, leaderMemberId);
            }
        }

        private void UpdateMemberEndpoints(string memberEndpoints, int leaderMemberId)
        {
            var tempMap = new DefaultDictionary<int, MemberEndpoint>();
            
            foreach (string endpoint in memberEndpoints.Split(','))
            {
                int separatorIndex = endpoint.IndexOf('=');
                if (-1 == separatorIndex)
                {
                    throw new ConfigurationException("invalid format - member missing '=' separator: " +
                                                     memberEndpoints);
                }

                int memberId = int.Parse(endpoint.Substring(0, separatorIndex));
                tempMap[memberId] = new MemberEndpoint(memberId, endpoint.Substring(separatorIndex + 1));
            }

            var existingLeaderEndpoint = _endpointByMemberIdMap[leaderMemberId];
            var leaderEndpoint = tempMap[leaderMemberId];

            if (null != existingLeaderEndpoint && null != existingLeaderEndpoint.publication)
            {
                if (null != leaderEndpoint && leaderEndpoint.endpoint.Equals(existingLeaderEndpoint.endpoint))
                {
                    leaderEndpoint.publication = existingLeaderEndpoint.publication;
                    existingLeaderEndpoint.publication = null;
                    _publication = leaderEndpoint.publication;
                }
            }

            if (null != leaderEndpoint && null == leaderEndpoint.publication)
            {
                var channelUri = ChannelUri.Parse(_ctx.IngressChannel());
                channelUri.Put(Aeron.Aeron.Context.ENDPOINT_PARAM_NAME, leaderEndpoint.endpoint);
                _publication = AddIngressPublication(channelUri.ToString(), _ctx.IngressStreamId());
                leaderEndpoint.publication = _publication;
            }

            foreach (var memberEndpoint in _endpointByMemberIdMap.Values)
            {
                memberEndpoint.Disconnect();
            }

            _endpointByMemberIdMap = tempMap;
        }

        private void CloseSession()
        {
            _idleStrategy.Reset();
            int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseRequestEncoder.BLOCK_LENGTH;
            SessionCloseRequestEncoder sessionCloseRequestEncoder = new SessionCloseRequestEncoder();
            int attempts = SEND_ATTEMPTS;

            while (true)
            {
                long result = _publication.TryClaim(length, _bufferClaim);

                if (result > 0)
                {
                    sessionCloseRequestEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .ClusterSessionId(_clusterSessionId);

                    _bufferClaim.Commit();
                    break;
                }

                CheckResult(result);

                if (--attempts <= 0)
                {
                    break;
                }

                _idleStrategy.Idle();
            }
        }

        private long ConnectToCluster()
        {
            long deadlineNs = _nanoClock.NanoTime() + _ctx.MessageTimeoutNs();

            if (_isUnicast)
            {
                UpdateMemberEndpoints(_ctx.ClusterMemberEndpoints(), Aeron.Aeron.NULL_VALUE);

                ChannelUri channelUri = ChannelUri.Parse(_ctx.IngressChannel());
                foreach (var member in _endpointByMemberIdMap.Values)
                {
                    channelUri.Put(Aeron.Aeron.Context.ENDPOINT_PARAM_NAME, member.endpoint);
                    member.publication = AddIngressPublication(channelUri.ToString(), _ctx.IngressStreamId());
                }

                while (true)
                {
                    MemberEndpoint connectedMember = null;
                    foreach (var member in _endpointByMemberIdMap.Values)
                    {
                        if (member.publication.IsConnected)
                        {
                            connectedMember = member;
                            break;
                        }
                    }

                    if (null != connectedMember)
                    {
                        _publication = connectedMember.publication;
                        EgressPoller poller = new EgressPoller(_subscription, CONNECT_FRAGMENT_LIMIT);
                        byte[] encodedCredentials = _ctx.CredentialsSupplier().EncodedCredentials();
                        long clusterSessionId = OpenSession(deadlineNs, poller, encodedCredentials);

                        _endpointByMemberIdMap[_leaderMemberId].publication = null;

                        foreach (var member in _endpointByMemberIdMap.Values)
                        {
                            member.Disconnect();
                        }
                        
                        return clusterSessionId;
                    }

                    if (_nanoClock.NanoTime() > deadlineNs)
                    {
                        throw new TimeoutException("awaiting connection to cluster");
                    }

                    _idleStrategy.Idle();
                }
            }
            else
            {
                _publication = AddIngressPublication(_ctx.IngressChannel(), _ctx.IngressStreamId());
                AwaitConnectedPublication(deadlineNs);
                byte[] encodedCredentials = _ctx.CredentialsSupplier().EncodedCredentials();

                return OpenSession(deadlineNs, new EgressPoller(_subscription, CONNECT_FRAGMENT_LIMIT), encodedCredentials);
            }
        }


        private Publication AddIngressPublication(string channel, int streamId)
        {
            if (_ctx.IsIngressExclusive())
            {
                return _aeron.AddExclusivePublication(channel, streamId);
            }
            else
            {
                return _aeron.AddPublication(channel, streamId);
            }
        }

        private long OpenSession(long deadlineNs, EgressPoller poller, byte[] encodedCredentials)
        {
            long correlationId = SendConnectRequest(_publication, encodedCredentials, deadlineNs);

            while (true)
            {
                PollNextResponse(deadlineNs, correlationId, poller);

                if (poller.CorrelationId() == correlationId)
                {
                    if (poller.IsChallenged())
                    {
                        correlationId = SendChallengeResponse(
                            poller.ClusterSessionId(), 
                            _ctx.CredentialsSupplier().OnChallenge(poller.EncodedChallenge()), 
                            deadlineNs);
                        continue;
                    }

                    switch (poller.EventCode())
                    {
                        case EventCode.OK:
                            _leadershipTermId = poller.LeadershipTermId();
                            _leaderMemberId = poller.LeaderMemberId();
                            return poller.ClusterSessionId();

                        case EventCode.ERROR:
                            throw new ClusterException(poller.Detail());

                        case EventCode.REDIRECT:
                            UpdateMemberEndpoints(poller.Detail(), poller.LeaderMemberId());
                            AwaitConnectedPublication(deadlineNs);
                            return OpenSession(deadlineNs, poller, encodedCredentials);

                        case EventCode.AUTHENTICATION_REJECTED:
                            throw new AuthenticationException(poller.Detail());
                    }
                }
            }
        }

        private void AwaitConnectedPublication(long deadlineNs)
        {
            while (!_publication.IsConnected)
            {
                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("awaiting connection to cluster");
                }

                _idleStrategy.Idle();
            }
        }

        
        private void PollNextResponse(long deadlineNs, long correlationId, EgressPoller poller)
        {
            _idleStrategy.Reset();

            while (poller.Poll() <= 0 && !poller.IsPollComplete())
            {
                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("awaiting response for correlationId=" + correlationId);
                }

                _idleStrategy.Idle();
            }
        }

        private long SendConnectRequest(Publication publication, byte[] encodedCredentials, long deadlineNs)
        {
            _lastCorrelationId = _aeron.NextCorrelationId();

            SessionConnectRequestEncoder sessionConnectRequestEncoder = new SessionConnectRequestEncoder();

            var length = MessageHeaderEncoder.ENCODED_LENGTH +
                         SessionConnectRequestEncoder.BLOCK_LENGTH +
                         SessionConnectRequestEncoder.ResponseChannelHeaderLength() +
                         _ctx.EgressChannel().Length +
                         SessionConnectRequestEncoder.EncodedCredentialsHeaderLength() + encodedCredentials.Length;

            var buffer = new UnsafeBuffer(new byte[length]); // TODO switch to ExpandableBuffer and remove above length

            sessionConnectRequestEncoder
                .WrapAndApplyHeader(buffer, 0, _messageHeaderEncoder)
                .CorrelationId(_lastCorrelationId)
                .ResponseStreamId(_ctx.EgressStreamId())
                .ResponseChannel(_ctx.EgressChannel())
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            _idleStrategy.Reset();

            while (true)
            {
                long result = publication.Offer(buffer);
                if (result > 0)
                {
                    break;
                }

                if (Publication.CLOSED == result)
                {
                    throw new ClusterException("unexpected close from cluster");
                }

                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("failed to connect to cluster");
                }

                _idleStrategy.Idle();
            }

            return _lastCorrelationId;
        }

        private long SendChallengeResponse(long sessionId, byte[] encodedCredentials, long deadlineNs)
        {
            _lastCorrelationId = _aeron.NextCorrelationId();

            ChallengeResponseEncoder challengeResponseEncoder = new ChallengeResponseEncoder();
            int length = MessageHeaderEncoder.ENCODED_LENGTH +
                         ChallengeResponseEncoder.BLOCK_LENGTH +
                         ChallengeResponseEncoder.EncodedCredentialsHeaderLength() +
                         encodedCredentials.Length;

            var buffer = new UnsafeBuffer(new byte[length]); // TODO switch to ExpandableBuffer and remove above length

            challengeResponseEncoder
                .WrapAndApplyHeader(buffer, 0, _messageHeaderEncoder)
                .CorrelationId(_lastCorrelationId)
                .ClusterSessionId(sessionId)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            _idleStrategy.Reset();

            while (true)
            {
                long result = _publication.Offer(buffer);
                if (result > 0)
                {
                    break;
                }

                CheckResult(result);

                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("failed to connect to cluster");
                }

                _idleStrategy.Idle();
            }

            return _lastCorrelationId;
        }

        private static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED ||
                result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ClusterException("unexpected publication state: " + result);
            }
        }

        /// <summary>
        /// Configuration options for cluster client.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Timeout when waiting on a message to be sent or received.
            /// </summary>
            public const string MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

            /// <summary>
            /// Default timeout when waiting on a message to be sent or received.
            /// </summary>
            public static readonly long MESSAGE_TIMEOUT_DEFAULT_NS = 5000000000;

            /// <summary>
            /// Property name for the comma separated list of cluster member endpoints for use with unicast. This is the
            /// endpoint values which get substituted into the <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> when using UDP unicast.
            ///
            /// <code>0=endpoint,1=endpoint,2=endpoint</code>
            /// 
            /// Each member of the list will be substituted for the endpoint in the <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> value.
            /// 
            /// </summary>
            public const string CLUSTER_MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";

            /// <summary>
            /// Default comma separated list of cluster member endpoints.
            /// </summary>
            public const string CLUSTER_MEMBER_ENDPOINTS_DEFAULT = null;

            /// <summary>
            /// Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
            /// be required and the <seealso cref="CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"/> is used to substitute the endpoints from
            /// the <seealso cref="CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"/> list.
            /// </summary>
            public const string INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

            /// <summary>
            /// Channel for sending messages to a cluster.
            /// </summary>
            public const string INGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9010";

            /// <summary>
            /// Stream id within a channel for sending messages to a cluster.
            /// </summary>
            public const string INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";

            /// <summary>
            /// Default stream id within a channel for sending messages to a cluster.
            /// </summary>
            public const int INGRESS_STREAM_ID_DEFAULT = 101;

            /// <summary>
            /// Channel for receiving response messages from a cluster.
            /// </summary>
            public const string EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

            /// <summary>
            /// Channel for receiving response messages from a cluster.
            /// </summary>
            public const string EGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9020";

            /// <summary>
            /// Stream id within a channel for receiving messages from a cluster.
            /// </summary>
            public const string EGRESS_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

            /// <summary>
            /// Default stream id within a channel for receiving messages from a cluster.
            /// </summary>
            public const int EGRESS_STREAM_ID_DEFAULT = 102;

            /// <summary>
            /// The timeout in nanoseconds to wait for a message.
            /// </summary>
            /// <returns> timeout in nanoseconds to wait for a message. </returns>
            /// <seealso cref="MESSAGE_TIMEOUT_PROP_NAME"></seealso>
            public static long MessageTimeoutNs()
            {
                return Config.GetDurationInNanos(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
            }

            /// <summary>
            /// The value <seealso cref="CLUSTER_MEMBER_ENDPOINTS_DEFAULT"/> or system property
            /// <seealso cref="CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CLUSTER_MEMBER_ENDPOINTS_DEFAULT"/> or system property
            /// <seealso cref="CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"/> if set. </returns>
            public static string ClusterMemberEndpoints()
            {
                return
                    Config.GetProperty(CLUSTER_MEMBER_ENDPOINTS_PROP_NAME, CLUSTER_MEMBER_ENDPOINTS_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#NGRESS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="INGRESS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string IngressChannel()
            {
                return Config.GetProperty(INGRESS_CHANNEL_PROP_NAME, INGRESS_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="INGRESS_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="INGRESS_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int IngressStreamId()
            {
                return Config.GetInteger(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="EGRESS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="EGRESS_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="EGRESS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="EGRESS_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string EgressChannel()
            {
                return Config.GetProperty(EGRESS_CHANNEL_PROP_NAME, EGRESS_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="EGRESS_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="EGRESS_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="EGRESS_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="EGRESS_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int EgressStreamId()
            {
                return Config.GetInteger(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT);
            }
        }

        /// <summary>
        /// Context for cluster session and connection.
        /// </summary>
        public class Context : IDisposable
        {
            private class MissingEgressMessageListener : IEgressMessageListener
            {
                public void OnMessage(
                    long correlationId,
                    long clusterSessionId,
                    long timestampMs,
                    IDirectBuffer buffer,
                    int offset,
                    int length,
                    Header header)
                {
                    throw new ConfigurationException("egressMessageListener must be specified on AeronCluster.Context");
                }
            }

            private long _messageTimeoutNs = Configuration.MessageTimeoutNs();
            private string _clusterMemberEndpoints = Configuration.ClusterMemberEndpoints();
            private string _ingressChannel = Configuration.IngressChannel();
            private int _ingressStreamId = Configuration.IngressStreamId();
            private string _egressChannel = Configuration.EgressChannel();
            private int _egressStreamId = Configuration.EgressStreamId();
            private IIdleStrategy _idleStrategy;
            private string _aeronDirectoryName = Adaptive.Aeron.Aeron.Context.GetAeronDirectoryName();
            private Aeron.Aeron _aeron;
            private ICredentialsSupplier _credentialsSupplier;
            private bool _ownsAeronClient = false;
            private bool _isIngressExclusive = true;
            private ErrorHandler _errorHandler = Adaptive.Aeron.Aeron.Configuration.DEFAULT_ERROR_HANDLER;
            private IEgressMessageListener _egressMessageListener;

            /// <summary>
            /// Perform a shallow copy of the object.
            /// </summary>
            /// <returns> a shall copy of the object.</returns>
            public Context Clone()
            {
                return (Context) MemberwiseClone();
            }

            public void Conclude()
            {
                if (null == _aeron)
                {
                    _aeron = Adaptive.Aeron.Aeron.Connect(
                        new Aeron.Aeron.Context()
                            .AeronDirectoryName(_aeronDirectoryName)
                            .ErrorHandler(_errorHandler));
                    _ownsAeronClient = true;
                }

                if (null == _idleStrategy)
                {
                    _idleStrategy = new BackoffIdleStrategy(1, 10, 1, 1);
                }

                if (null == _credentialsSupplier)
                {
                    _credentialsSupplier = new NullCredentialsSupplier();
                }

                if (null == _egressMessageListener)
                {
                    _egressMessageListener = new MissingEgressMessageListener();
                }
            }

            /// <summary>
            /// Set the message timeout in nanoseconds to wait for sending or receiving a message.
            /// </summary>
            /// <param name="messageTimeoutNs"> to wait for sending or receiving a message. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.MESSAGE_TIMEOUT_PROP_NAME"></seealso>
            public Context MessageTimeoutNs(long messageTimeoutNs)
            {
                _messageTimeoutNs = messageTimeoutNs;
                return this;
            }

            /// <summary>
            /// The message timeout in nanoseconds to wait for sending or receiving a message.
            /// </summary>
            /// <returns> the message timeout in nanoseconds to wait for sending or receiving a message. </returns>
            /// <seealso cref="Configuration.MESSAGE_TIMEOUT_PROP_NAME"></seealso>
            public long MessageTimeoutNs()
            {
                return _messageTimeoutNs;
            }

            /// <summary>
            /// The endpoints representing members for use with unicast to be substituted into the <seealso cref="IngressChannel()"/>
            /// for endpoints. A null value can be used when multicast where the <seealso cref="IngressChannel()"/> contains the
            /// multicast endpoint.
            /// </summary>
            /// <param name="clusterMembers"> which are all candidates to be leader. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"></seealso>
            public Context ClusterMemberEndpoints(string clusterMembers)
            {
                _clusterMemberEndpoints = clusterMembers;
                return this;
            }

            /// <summary>
            /// The endpoints representing members for use with unicast to be substituted into the <seealso cref="IngressChannel()"/>
            /// for endpoints. A null value can be used when multicast where the <seealso cref="IngressChannel()"/> contains the
            /// multicast endpoint.
            /// </summary>
            /// <returns> members of the cluster which are all candidates to be leader. </returns>
            /// <seealso cref="Configuration.CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"></seealso>
            public string ClusterMemberEndpoints()
            {
                return _clusterMemberEndpoints;
            }

            /// <summary>
            /// Set the channel parameter for the ingress channel.
            /// <para>
            /// The endpoints representing members for use with unicast are substituted from the
            /// <seealso cref="ClusterMemberEndpoints()"/> for endpoints. A null value can be used when multicast
            /// where this contains the multicast endpoint.
            ///         
            /// </para>
            /// </summary>
            /// <param name="channel"> parameter for the ingress channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.INGRESS_CHANNEL_PROP_NAME"></seealso>
            public Context IngressChannel(string channel)
            {
                _ingressChannel = channel;
                return this;
            }

            /// <summary>
            /// Set the channel parameter for the ingress channel.
            ///
            /// The endpoints representing members for use with unicast are substituted from the
            /// <seealso cref="ClusterMemberEndpoints()"/> for endpoints. A null value can be used when multicast
            /// where this contains the multicast endpoint.
            ///        
            /// </summary>
            /// <returns> the channel parameter for the ingress channel. </returns>
            /// <seealso cref="Configuration.INGRESS_CHANNEL_PROP_NAME"></seealso>
            public string IngressChannel()
            {
                return _ingressChannel;
            }

            /// <summary>
            /// Set the stream id for the ingress channel.
            /// </summary>
            /// <param name="streamId"> for the ingress channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.INGRESS_STREAM_ID_PROP_NAME"></seealso>
            public Context IngressStreamId(int streamId)
            {
                _ingressStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the ingress channel.
            /// </summary>
            /// <returns> the stream id for the ingress channel. </returns>
            /// <seealso cref="Configuration.INGRESS_STREAM_ID_PROP_NAME"></seealso>
            public int IngressStreamId()
            {
                return _ingressStreamId;
            }

            /// <summary>
            /// Set the channel parameter for the egress channel.
            /// </summary>
            /// <param name="channel"> parameter for the egress channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.EGRESS_CHANNEL_PROP_NAME"></seealso>
            public Context EgressChannel(string channel)
            {
                _egressChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the egress channel.
            /// </summary>
            /// <returns> the channel parameter for the egress channel. </returns>
            /// <seealso cref="Configuration.EGRESS_CHANNEL_PROP_NAME"></seealso>
            public string EgressChannel()
            {
                return _egressChannel;
            }

            /// <summary>
            /// Set the stream id for the egress channel.
            /// </summary>
            /// <param name="streamId"> for the egress channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.EGRESS_STREAM_ID_PROP_NAME"></seealso>
            public Context EgressStreamId(int streamId)
            {
                _egressStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the egress channel.
            /// </summary>
            /// <returns> the stream id for the egress channel. </returns>
            /// <seealso cref="Configuration.EGRESS_STREAM_ID_PROP_NAME"/></seealso>
            public int EgressStreamId()
            {
                return _egressStreamId;
            }

            /// <summary>
            /// Set the <seealso cref="IIdleStrategy"/> used when waiting for responses.
            /// </summary>
            /// <param name="idleStrategy"> used when waiting for responses. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IdleStrategy(IIdleStrategy idleStrategy)
            {
                _idleStrategy = idleStrategy;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IIdleStrategy"/> used when waiting for responses.
            /// </summary>
            /// <returns> the <seealso cref="IIdleStrategy"/> used when waiting for responses. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return _idleStrategy;
            }

            /// <summary>
            /// Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <param name="aeronDirectoryName"> the top level Aeron directory. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AeronDirectoryName(string aeronDirectoryName)
            {
                _aeronDirectoryName = aeronDirectoryName;
                return this;
            }

            /// <summary>
            /// Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <returns> The top level Aeron directory. </returns>
            public string AeronDirectoryName()
            {
                return _aeronDirectoryName;
            }

            /// <summary>
            /// <seealso cref="Adaptive.Aeron.Aeron"/> client for communicating with the local Media Driver.
            /// <para>
            /// This client will be closed when the <seealso cref="AeronCluster.Dispose()"/> or <seealso cref="Dispose()"/> methods are called if
            /// <seealso cref="OwnsAeronClient()"/> is true.
            /// 
            /// </para>
            /// </summary>
            /// <param name="aeron"> client for communicating with the local Media Driver. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Adaptive.Aeron.Aeron.Connect()"/>.
            public Context Aeron(Aeron.Aeron aeron)
            {
                _aeron = aeron;
                return this;
            }

            /// <summary>
            /// <seealso cref="Adaptive.Aeron.Aeron"/> client for communicating with the local Media Driver.
            /// <para>
            /// If not provided then a default will be established during <seealso cref="Conclude()"/> by calling
            /// <seealso cref="Adaptive.Aeron.Aeron.Connect()"/>.
            /// 
            /// </para>
            /// </summary>
            /// <returns> client for communicating with the local Media Driver. </returns>
            public Aeron.Aeron Aeron()
            {
                return _aeron;
            }

            /// <summary>
            /// Does this context own the <seealso cref="Aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="Aeron()"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                _ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="Aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="Aeron()"/> client and this takes responsibility for closing it? </returns>
            public bool OwnsAeronClient()
            {
                return _ownsAeronClient;
            }

            /// <summary>
            /// Is ingress to the cluster exclusively from a single thread for this client?
            /// </summary>
            /// <param name="isIngressExclusive"> true if ingress to the cluster is exclusively from a single thread for this client? </param>
            /// <returns> this for a fluent API. </returns>
            public Context IsIngressExclusive(bool isIngressExclusive)
            {
                _isIngressExclusive = isIngressExclusive;
                return this;
            }

            /// <summary>
            /// Is ingress to the cluster exclusively from a single thread for this client?
            /// </summary>
            /// <returns> true if ingress to the cluster exclusively from a single thread for this client? </returns>
            public bool IsIngressExclusive()
            {
                return _isIngressExclusive;
            }

            /// <summary>
            /// Get the <seealso cref="ICredentialsSupplier"/> to be used for authentication with the cluster.
            /// </summary>
            /// <returns> the <seealso cref="ICredentialsSupplier"/> to be used for authentication with the cluster. </returns>
            public ICredentialsSupplier CredentialsSupplier()
            {
                return _credentialsSupplier;
            }

            /// <summary>
            /// Set the <seealso cref="ICredentialsSupplier"/> to be used for authentication with the cluster.
            /// </summary>
            /// <param name="credentialsSupplier"> to be used for authentication with the cluster. </param>
            /// <returns> this for fluent API. </returns>
            public Context CredentialsSupplier(ICredentialsSupplier credentialsSupplier)
            {
                _credentialsSupplier = credentialsSupplier;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.ErrorHandler"/> to be used for handling any exceptions.
            /// </summary>
            /// <returns> The <seealso cref="Agrona.ErrorHandler"/> to be used for handling any exceptions. </returns>
            public ErrorHandler ErrorHandler()
            {
                return _errorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.ErrorHandler"/> to be used for handling any exceptions.
            /// </summary>
            /// <param name="errorHandler"> Method to handle objects of type Throwable. </param>
            /// <returns> this for fluent API. </returns>
            public virtual Context ErrorHandler(ErrorHandler errorHandler)
            {
                _errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IEgressMessageListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.PollEgress()"/>.
            /// </summary>
            /// <returns> the <seealso cref="IEgressMessageListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.PollEgress()"/>. </returns>
            public IEgressMessageListener EgressMessageListener()
            {
                return _egressMessageListener;
            }

            /// <summary>
            /// Get the <seealso cref="IEgressMessageListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.PollEgress()"/>.
            /// </summary>
            /// <param name="listener"> function that will be called when polling for egress via <seealso cref="AeronCluster.PollEgress()"/>. </param>
            /// <returns> this for a fluent API. </returns>
            public Context EgressMessageListener(IEgressMessageListener listener)
            {
                _egressMessageListener = listener;
                return this;
            }


            /// <summary>
            /// Close the context and free applicable resources.
            /// <para>
            /// If <seealso cref="OwnsAeronClient()"/> is true then the <seealso cref="Aeron()"/> client will be closed.
            /// </para>
            /// </summary>
            public void Dispose()
            {
                if (_ownsAeronClient)
                {
                    _aeron.Dispose();
                }
            }
        }
        
        class MemberEndpoint
        {
            readonly int memberId;
            internal readonly string endpoint;
            internal Publication publication;

            internal MemberEndpoint(int memberId, string endpoint)
            {
                this.memberId = memberId;
                this.endpoint = endpoint;
            }

            internal void Disconnect()
            {
                publication?.Dispose();
                publication = null;
            }

            public override string ToString()
            {
                return "MemberEndpoint{" +
                       "memberId=" + memberId +
                       ", endpoint='" + endpoint + '\'' +
                       ", publication=" + publication +
                       '}';
            }
        }
    }
}