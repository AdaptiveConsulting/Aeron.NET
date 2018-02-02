using System;
using System.Security.Authentication;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Client for interacting with an Aeron Cluster.
    /// 
    /// A client will connect to open a session and then offer ingress messages which are replicated to clustered service
    /// for reliability. If the clustered service responds then these response messages and events come back via the egress
    /// stream.
    /// 
    /// </summary>
    public sealed class AeronCluster : IDisposable
    {
        private const int SEND_ATTEMPTS = 3;
        private const int FRAGMENT_LIMIT = 1;

        private readonly long _clusterSessionId;
        private readonly bool _isUnicast;
        private readonly Context _ctx;
        private readonly Aeron.Aeron _aeron;
        private readonly Subscription _subscription;
        private readonly Publication _publication;
        private readonly INanoClock _nanoClock;
        private readonly ILock _lock;
        private readonly IIdleStrategy _idleStrategy;
        private readonly BufferClaim _bufferClaim = new BufferClaim();
        private readonly MessageHeaderEncoder _messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly SessionKeepAliveRequestEncoder _keepAliveRequestEncoder = new SessionKeepAliveRequestEncoder();

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
            Publication publication = null;

            try
            {
                ctx.Conclude();

                _aeron = ctx.Aeron();
                _lock = ctx.Lock();
                _idleStrategy = ctx.IdleStrategy();
                _nanoClock = _aeron.Ctx().NanoClock();
                _isUnicast = ctx.ClusterMemberEndpoints() != null;

                publication = ConnectToCluster();
                _publication = publication;

                subscription = _aeron.AddSubscription(ctx.EgressChannel(), ctx.EgressStreamId());
                _subscription = subscription;

                _clusterSessionId = OpenSession();
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    CloseHelper.QuietDispose(publication);
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
            _lock.Lock();
            try
            {
                if (_publication.IsConnected)
                {
                    CloseSession();
                }

                if (!_ctx.OwnsAeronClient())
                {
                    _subscription.Dispose();
                    _publication.Dispose();
                }

                _ctx.Dispose();
            }
            finally
            {
                _lock.Unlock();
            }
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
        /// Get the raw <seealso cref="Publication"/> for sending to the cluster.
        /// <para>
        /// This can be wrapped with a <seealso cref="SessionDecorator"/> for pre-pending the cluster session header to messages.
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
        /// Send a keep alive message to the cluster to keep this session open.
        /// </summary>
        /// <returns> true if successfully sent otherwise false. </returns>
        public bool SendKeepAlive()
        {
            _lock.Lock();
            try
            {
                _idleStrategy.Reset();
                int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionKeepAliveRequestEncoder.BLOCK_LENGTH;
                int attempts = SEND_ATTEMPTS;

                while (true)
                {
                    long result = _publication.TryClaim(length, _bufferClaim);

                    if (result > 0)
                    {
                        _keepAliveRequestEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder).CorrelationId(0L).ClusterSessionId(_clusterSessionId);

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
            finally
            {
                _lock.Unlock();
            }
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
                    sessionCloseRequestEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder).ClusterSessionId(_clusterSessionId);

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

        private Publication ConnectToCluster()
        {
            Publication publication = null;
            string ingressChannel = _ctx.IngressChannel();
            int ingressStreamId = _ctx.IngressStreamId();
            long deadlineNs = _nanoClock.NanoTime() + _ctx.MessageTimeoutNs();

            if (_isUnicast)
            {
                ChannelUri channelUri = ChannelUri.Parse(ingressChannel);
                string[] memberEndpoints = _ctx.ClusterMemberEndpoints();
                int memberCount = memberEndpoints.Length;
                Publication[] publications = new Publication[memberCount];

                for (int i = 0; i < memberCount; i++)
                {
                    channelUri.Put(Aeron.Aeron.Context.ENDPOINT_PARAM_NAME, memberEndpoints[i]);
                    string channel = channelUri.ToString();
                    publications[i] = AddIngressPublication(channel, ingressStreamId);
                }

                int connectedIndex = -1;
                while (true)
                {
                    for (int i = 0; i < memberCount; i++)
                    {
                        if (publications[i].IsConnected)
                        {
                            connectedIndex = i;
                            break;
                        }
                    }

                    if (-1 != connectedIndex)
                    {
                        for (int i = 0; i < memberCount; i++)
                        {
                            if (i == connectedIndex)
                            {
                                publication = publications[i];
                            }
                            else
                            {
                                publications[i].Dispose();
                            }
                        }

                        break;
                    }

                    if (_nanoClock.NanoTime() > deadlineNs)
                    {
                        throw new TimeoutException("Awaiting connection to cluster");
                    }

                    _idleStrategy.Idle();
                }
            }
            else
            {
                publication = AddIngressPublication(ingressChannel, ingressStreamId);

                _idleStrategy.Reset();
                while (!publication.IsConnected)
                {
                    if (_nanoClock.NanoTime() > deadlineNs)
                    {
                        throw new TimeoutException("Awaiting connection to cluster");
                    }

                    _idleStrategy.Idle();
                }
            }

            return publication;
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

        private long OpenSession()
        {
            long deadlineNs = _nanoClock.NanoTime() + _ctx.MessageTimeoutNs();
            long correlationId = SendConnectRequest(_ctx.CredentialsSupplier().ConnectRequestCredentialData(), deadlineNs);
            EgressPoller poller = new EgressPoller(_subscription, FRAGMENT_LIMIT);

            while (true)
            {
                PollNextResponse(deadlineNs, correlationId, poller);

                if (poller.CorrelationId() == correlationId)
                {
                    if (poller.Challenged())
                    {
                        byte[] credentialData = _ctx.CredentialsSupplier().OnChallenge(poller.ChallengeData());
                        correlationId = SendChallengeResponse(poller.ClusterSessionId(), credentialData, deadlineNs);
                        continue;
                    }

                    switch (poller.EventCode())
                    {
                        case EventCode.OK:
                            return poller.ClusterSessionId();

                        case EventCode.ERROR:
                            throw new AuthenticationException(poller.Detail());

                        case EventCode.AUTHENTICATION_REJECTED:
                            throw new AuthenticationException(poller.Detail());
                    }
                }
            }
        }

        private void PollNextResponse(long deadlineNs, long correlationId, EgressPoller poller)
        {
            _idleStrategy.Reset();

            while (poller.Poll() <= 0 && !poller.IsPollComplete())
            {
                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("Awaiting response for correlationId=" + correlationId);
                }

                _idleStrategy.Idle();
            }
        }

        private long SendConnectRequest(byte[] credentialData, long deadlineNs)
        {
            long correlationId = _aeron.NextCorrelationId();

            SessionConnectRequestEncoder sessionConnectRequestEncoder = new SessionConnectRequestEncoder();
            int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionConnectRequestEncoder.BLOCK_LENGTH + SessionConnectRequestEncoder.ResponseChannelHeaderLength() + _ctx.EgressChannel().Length + SessionConnectRequestEncoder.CredentialDataHeaderLength() + credentialData.Length;

            _idleStrategy.Reset();

            while (true)
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    sessionConnectRequestEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .CorrelationId(correlationId)
                        .ResponseStreamId(_ctx.EgressStreamId())
                        .ResponseChannel(_ctx.EgressChannel())
                        .PutCredentialData(credentialData, 0, credentialData.Length);

                    _bufferClaim.Commit();

                    break;
                }

                if (Publication.CLOSED == result)
                {
                    throw new InvalidOperationException("Unexpected close from cluster");
                }

                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("Failed to connect to cluster");
                }

                _idleStrategy.Idle();
            }

            return correlationId;
        }

        private long SendChallengeResponse(long sessionId, byte[] credentialData, long deadlineNs)
        {
            long correlationId = _aeron.NextCorrelationId();

            ChallengeResponseEncoder challengeResponseEncoder = new ChallengeResponseEncoder();
            int length = MessageHeaderEncoder.ENCODED_LENGTH + ChallengeResponseEncoder.BLOCK_LENGTH + ChallengeResponseEncoder.CredentialDataHeaderLength() + credentialData.Length;

            _idleStrategy.Reset();

            while (true)
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    challengeResponseEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder).CorrelationId(correlationId).ClusterSessionId(sessionId).PutCredentialData(credentialData, 0, credentialData.Length);

                    _bufferClaim.Commit();

                    break;
                }

                CheckResult(result);

                if (_nanoClock.NanoTime() > deadlineNs)
                {
                    throw new TimeoutException("Failed to connect to cluster");
                }

                _idleStrategy.Idle();
            }

            return correlationId;
        }

        private static void CheckResult(long result)
        {
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new InvalidOperationException("Unexpected publication state: " + result);
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
            /// Timeout when waiting on a message to be sent or received. Default to 5 seconds in nanoseconds.
            /// </summary>
            public static readonly long MESSAGE_TIMEOUT_DEFAULT_NS = 5000000000;

            /// <summary>
            /// Property name for the comma separated list of cluster member endpoints for use with unicast.
            /// <para>
            /// Each member of the list will be substituted for the endpoint in the <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> value.
            /// </para>
            /// </summary>
            public const string CLUSTER_MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";

            /// <summary>
            /// Property name for the comma separated list of cluster member endpoints. Default of null is for multicast.
            /// </summary>
            public const string CLUSTER_MEMBER_ENDPOINTS_DEFAULT = null;

            /// <summary>
            /// Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
            /// be required and the <seealso cref="CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"/> is used to substitute the endpoints.
            /// </summary>
            public const string INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

            /// <summary>
            /// Channel for sending messages to a cluster. Default to localhost:9010 for testing.
            /// </summary>
            public const string INGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9010";

            /// <summary>
            /// Stream id within a channel for sending messages to a cluster.
            /// </summary>
            public const string INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";

            /// <summary>
            /// Stream id within a channel for sending messages to a cluster. Default to stream id of 1.
            /// </summary>
            public const int INGRESS_STREAM_ID_DEFAULT = 1;

            /// <summary>
            /// Channel for receiving response messages from a cluster.
            /// </summary>
            public const string EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

            /// <summary>
            /// Channel for receiving response messages from a cluster. Default to localhost:9020 for testing.
            /// </summary>
            public const string EGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9020";

            /// <summary>
            /// Stream id within a channel for receiving messages from a cluster.
            /// </summary>
            public const string EGRESS_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

            /// <summary>
            /// Stream id within a channel for receiving messages from a cluster. Default to stream id of 2.
            /// </summary>
            public const int EGRESS_STREAM_ID_DEFAULT = 2;

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
            public static string[] ClusterMemberEndpoints()
            {
                string memberEndpoints = Config.GetProperty(CLUSTER_MEMBER_ENDPOINTS_PROP_NAME, CLUSTER_MEMBER_ENDPOINTS_DEFAULT);

                return memberEndpoints?.Split(',');
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
            private long _messageTimeoutNs = Configuration.MessageTimeoutNs();
            private string[] _clusterMemberEndpoints = Configuration.ClusterMemberEndpoints();
            private string _ingressChannel = Configuration.IngressChannel();
            private int _ingressStreamId = Configuration.IngressStreamId();
            private string _egressChannel = Configuration.EgressChannel();
            private int _egressStreamId = Configuration.EgressStreamId();
            private IIdleStrategy _idleStrategy;
            private ILock _lock;
            private string _aeronDirectoryName;
            private Aeron.Aeron _aeron;
            private ICredentialsSupplier _credentialsSupplier;
            private bool _ownsAeronClient = true;
            private bool _isIngressExclusive = true;

            public void Conclude()
            {
                if (null == _aeron)
                {
                    var ctx = new Aeron.Aeron.Context();

                    if (_aeronDirectoryName != null)
                    {
                        ctx.AeronDirectoryName(_aeronDirectoryName);
                    }
                    
                    _aeron = Adaptive.Aeron.Aeron.Connect(ctx);
                }

                if (null == _idleStrategy)
                {
                    _idleStrategy = new BackoffIdleStrategy(1, 10, 1, 1);
                }

                if (null == _lock)
                {
                    _lock = new ReentrantLock();
                }

                if (null == _credentialsSupplier)
                {
                    _credentialsSupplier = new NullCredentialsSupplier();
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
            /// The endpoints representing members for use with unicast. A null value can be used when multicast.
            /// </summary>
            /// <param name="clusterMembers"> which are all candidates to be leader. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"></seealso>
            public Context ClusterMemberEndpoints(params string[] clusterMembers)
            {
                _clusterMemberEndpoints = clusterMembers;
                return this;
            }

            /// <summary>
            /// The endpoints representing members for use with unicast. A null value can be used when multicast.
            /// </summary>
            /// <returns> members of the cluster which are all candidates to be leader. </returns>
            /// <seealso cref="Configuration.CLUSTER_MEMBER_ENDPOINTS_PROP_NAME"></seealso>
            public string[] ClusterMemberEndpoints()
            {
                return _clusterMemberEndpoints;
            }

            /// <summary>
            /// Set the channel parameter for the ingress channel.
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
            /// Get the channel parameter for the ingress channel.
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
            /// The <seealso cref="ILock"/> that is used to provide mutual exclusion in the <seealso cref="AeronCluster"/> client.
            /// <para>
            /// If the <seealso cref="AeronCluster"/> is used from only a single thread then the lock can be set to
            /// <seealso cref="NoOpLock"/> to elide the lock overhead.
            /// 
            /// </para>
            /// </summary>
            /// <param name="lock"> that is used to provide mutual exclusion in the <seealso cref="AeronCluster"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context Lock(ILock @lock)
            {
                _lock = @lock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Lock"/> that is used to provide mutual exclusion in the <seealso cref="AeronCluster"/> client.
            /// </summary>
            /// <returns> the <seealso cref="Lock"/> that is used to provide mutual exclusion in the <seealso cref="AeronCluster"/> client. </returns>
            public ILock Lock()
            {
                return _lock;
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
            /// Close the context and free applicable resources.
            /// <para>
            /// If the <seealso cref="OwnsAeronClient()"/> is true then the <seealso cref="Aeron()"/> client will be closed.
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
    }
}