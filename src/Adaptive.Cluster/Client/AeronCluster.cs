using System;
using System.Collections.Generic;
using System.Threading;
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
    /// for reliability. If the clustered service responds then response messages and events are sent via the egress stream.
    ///
    /// Note: Instances of this class are not threadsafe.
    /// 
    /// </summary>
    public sealed class AeronCluster : IDisposable
    {
        /// <summary>
        /// Length of a session message header for cluster ingress or egress.
        /// </summary>
        public static readonly int SESSION_HEADER_LENGTH =
            MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH;

        private const int SEND_ATTEMPTS = 3;
        private const int FRAGMENT_LIMIT = 10;

        private readonly long _clusterSessionId;
        private long _leadershipTermId;
        private int _leaderMemberId;
        private bool _isClosed;
        private readonly Context _ctx;
        private readonly Subscription _subscription;
        private Image egressImage;
        private Publication _publication;
        private readonly IIdleStrategy _idleStrategy;
        private readonly BufferClaim _bufferClaim = new BufferClaim();
        private readonly UnsafeBuffer _headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
        private readonly DirectBufferVector _headerVector;
        private readonly MessageHeaderEncoder _messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly SessionMessageHeaderEncoder _sessionMessageEncoder = new SessionMessageHeaderEncoder();
        private readonly SessionKeepAliveEncoder _sessionKeepAliveEncoder = new SessionKeepAliveEncoder();
        private readonly AdminRequestEncoder _adminRequestEncoder = new AdminRequestEncoder();

        private readonly FragmentAssembler _fragmentAssembler;
        private readonly IEgressListener _egressListener;
        private readonly ControlledFragmentAssembler _controlledFragmentAssembler;
        private readonly IControlledEgressListener _controlledEgressListener;
        private Map<int, MemberIngress> _endpointByIdMap = new Map<int, MemberIngress>();

        private readonly Poller _poller;
        private readonly ControlledPoller _controlledPoller;

        private class Poller : IFragmentHandler
        {
            private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();

            private readonly SessionMessageHeaderDecoder _sessionMessageHeaderDecoder =
                new SessionMessageHeaderDecoder();

            private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
            private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
            private readonly AdminResponseDecoder _adminResponseDecoder = new AdminResponseDecoder();

            private readonly IEgressListener _egressListener;
            private readonly long _clusterSessionId;
            private readonly AeronCluster _cluster;

            public Poller(IEgressListener egressListener, long clusterSessionId, AeronCluster cluster)
            {
                _egressListener = egressListener;
                _clusterSessionId = clusterSessionId;
                _cluster = cluster;
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _messageHeaderDecoder.Wrap(buffer, offset);

                int templateId = _messageHeaderDecoder.TemplateId();
                if (SessionMessageHeaderDecoder.TEMPLATE_ID == templateId)
                {
                    _sessionMessageHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _sessionMessageHeaderDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _egressListener.OnMessage(
                            sessionId,
                            _sessionMessageHeaderDecoder.Timestamp(),
                            buffer,
                            offset + SESSION_HEADER_LENGTH,
                            length - SESSION_HEADER_LENGTH,
                            header);
                    }
                }
                else if (NewLeaderEventDecoder.TEMPLATE_ID == templateId)
                {
                    _newLeaderEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _newLeaderEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _cluster.egressImage = (Image)header.Context;
                        _cluster.OnNewLeader(
                            sessionId,
                            _newLeaderEventDecoder.LeadershipTermId(),
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.IngressEndpoints());
                    }
                }
                else if (SessionEventDecoder.TEMPLATE_ID == templateId)
                {
                    _sessionEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _sessionEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        EventCode code = _sessionEventDecoder.Code();
                        if (EventCode.CLOSED == code)
                        {
                            _cluster._isClosed = true;
                        }

                        _egressListener.OnSessionEvent(
                            _sessionEventDecoder.CorrelationId(),
                            sessionId,
                            _sessionEventDecoder.LeadershipTermId(),
                            _sessionEventDecoder.LeaderMemberId(),
                            code,
                            _sessionEventDecoder.Detail());
                    }
                }
                else if (AdminResponseDecoder.TEMPLATE_ID == templateId)
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
                        _egressListener.OnAdminResponse(
                            sessionId,
                            correlationId,
                            requestType,
                            responseCode,
                            message,
                            buffer,
                            payloadOffset,
                            payloadLength);
                    }
                }
            }
        }

        private class ControlledPoller : IControlledFragmentHandler
        {
            private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();

            private readonly SessionMessageHeaderDecoder _sessionMessageHeaderDecoder =
                new SessionMessageHeaderDecoder();

            private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
            private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
            private readonly AdminResponseDecoder _adminResponseDecoder = new AdminResponseDecoder();

            private readonly IControlledEgressListener _egressListener;
            private readonly long _clusterSessionId;
            private readonly AeronCluster _cluster;

            public ControlledPoller(IControlledEgressListener egressListener, long clusterSessionId,
                AeronCluster cluster)
            {
                _egressListener = egressListener;
                _clusterSessionId = clusterSessionId;
                _cluster = cluster;
            }

            public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length,
                Header header)
            {
                _messageHeaderDecoder.Wrap(buffer, offset);

                int templateId = _messageHeaderDecoder.TemplateId();
                if (SessionMessageHeaderDecoder.TEMPLATE_ID == templateId)
                {
                    _sessionMessageHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _sessionMessageHeaderDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        return _egressListener.OnMessage(
                            sessionId,
                            _sessionMessageHeaderDecoder.Timestamp(),
                            buffer,
                            offset + SESSION_HEADER_LENGTH,
                            length - SESSION_HEADER_LENGTH,
                            header);
                    }
                }
                else if (NewLeaderEventDecoder.TEMPLATE_ID == templateId)
                {
                    _newLeaderEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _newLeaderEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _cluster.egressImage = (Image)header.Context;
                        _cluster.OnNewLeader(
                            sessionId,
                            _newLeaderEventDecoder.LeadershipTermId(),
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.IngressEndpoints());

                        return ControlledFragmentHandlerAction.COMMIT;
                    }
                }
                else if (SessionEventDecoder.TEMPLATE_ID == templateId)
                {
                    _sessionEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    long sessionId = _sessionEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        EventCode code = _sessionEventDecoder.Code();
                        if (EventCode.CLOSED == code)
                        {
                            _cluster._isClosed = true;
                        }

                        _egressListener.OnSessionEvent(
                            _sessionEventDecoder.CorrelationId(),
                            sessionId,
                            _sessionEventDecoder.LeadershipTermId(),
                            _sessionEventDecoder.LeaderMemberId(),
                            code,
                            _sessionEventDecoder.Detail());

                        return ControlledFragmentHandlerAction.COMMIT;
                    }
                }
                else if (AdminResponseDecoder.TEMPLATE_ID == templateId)
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
                        _egressListener.OnAdminResponse(
                            sessionId,
                            correlationId,
                            requestType,
                            responseCode,
                            message,
                            buffer,
                            payloadOffset,
                            payloadLength);
                    }
                }

                return ControlledFragmentHandlerAction.CONTINUE;
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
        /// Connect to the cluster providing <seealso cref="Adaptive.Aeron.Aeron.Context"/> for configuration.
        /// </summary>
        /// <param name="ctx"> for configuration. </param>
        /// <returns> allocated cluster client if the connection is successful. </returns>
        public static AeronCluster Connect(Context ctx)
        {
            Subscription subscription = null;
            AsyncConnect asyncConnect = null;

            try
            {
                ctx.Conclude();

                Aeron.Aeron aeron = ctx.Aeron();
                subscription = aeron.AddSubscription(ctx.EgressChannel(), ctx.EgressStreamId());

                long deadlineNs = aeron.Ctx.NanoClock().NanoTime() + ctx.MessageTimeoutNs();
                asyncConnect = new AsyncConnect(ctx, subscription, deadlineNs);
                AgentInvoker aeronClientInvoker = aeron.ConductorAgentInvoker;
                AgentInvoker agentInvoker = ctx.AgentInvoker();
                IIdleStrategy idleStrategy = ctx.IdleStrategy();

                AeronCluster aeronCluster;
                int step = asyncConnect.Step();
                while (null == (aeronCluster = asyncConnect.Poll()))
                {
                    if (null != aeronClientInvoker)
                    {
                        aeronClientInvoker.Invoke();
                    }
                    
                    if (null != agentInvoker)
                    {
                        agentInvoker.Invoke();
                    }

                    if (step != asyncConnect.Step())
                    {
                        step = asyncConnect.Step();
                        idleStrategy.Reset();
                    }
                    else
                    {
                        idleStrategy.Idle();
                    }
                }

                return aeronCluster;
            }
            catch (ConcurrentConcludeException)
            {
                throw;
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    CloseHelper.QuietDispose(subscription);
                    CloseHelper.QuietDispose(asyncConnect);
                }

                CloseHelper.QuietDispose(ctx.Dispose);

                throw;
            }
        }

        /// <summary>
        /// Begin an attempt at creating a connection which can be completed by calling <seealso cref="AsyncConnect.Poll()"/> until
        /// it returns the client, before complete it will return null.
        /// </summary>
        /// <returns> the <seealso cref="AsyncConnect"/> that can be polled for completion. </returns>
        public static AsyncConnect ConnectAsync()
        {
            return ConnectAsync(new Context());
        }

        /// <summary>
        /// Begin an attempt at creating a connection which can be completed by calling <seealso cref="AsyncConnect.Poll()"/> until
        /// it returns the client, before complete it will return null.
        /// </summary>
        /// <param name="ctx"> for the cluster. </param>
        /// <returns> the <seealso cref="AsyncConnect"/> that can be polled for completion. </returns>
        public static AsyncConnect ConnectAsync(Context ctx)
        {
            Subscription subscription = null;
            try
            {
                ctx.Conclude();

                long deadlineNs = ctx.Aeron().Ctx.NanoClock().NanoTime() + ctx.MessageTimeoutNs();
                subscription = ctx.Aeron().AddSubscription(ctx.EgressChannel(), ctx.EgressStreamId());

                return new AsyncConnect(ctx, subscription, deadlineNs);
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    subscription?.Dispose();
                }

                ctx.Dispose();

                throw;
            }
        }

        internal AeronCluster(
            Context ctx,
            MessageHeaderEncoder messageHeaderEncoder,
            Publication publication,
            Subscription subscription,
            Image egressImage,
            Map<int, MemberIngress> endpointByIdMap,
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId
        )
        {
            _headerVector = new DirectBufferVector(_headerBuffer, 0, _headerBuffer.Capacity);

            _ctx = ctx;
            _messageHeaderEncoder = messageHeaderEncoder;
            _subscription = subscription;
            this.egressImage = egressImage; 
            _endpointByIdMap = endpointByIdMap;
            _clusterSessionId = clusterSessionId;
            _leadershipTermId = leadershipTermId;
            _leaderMemberId = leaderMemberId;
            _publication = publication;
            _idleStrategy = ctx.IdleStrategy();
            _egressListener = ctx.EgressListener();
            _controlledEgressListener = ctx.ControlledEgressListener();
            _poller = new Poller(ctx.EgressListener(), _clusterSessionId, this);
            _fragmentAssembler = new FragmentAssembler(_poller);
            _controlledPoller = new ControlledPoller(ctx.ControlledEgressListener(), _clusterSessionId, this);
            _controlledFragmentAssembler = new ControlledFragmentAssembler(_controlledPoller);

            _sessionMessageEncoder
                .WrapAndApplyHeader(_headerBuffer, 0, _messageHeaderEncoder)
                .ClusterSessionId(_clusterSessionId)
                .LeadershipTermId(_leadershipTermId);
        }

        /// <summary>
        /// Close session and release associated resources.
        /// </summary>
        public void Dispose()
        {
            if (null != _publication && _publication.IsConnected && !_isClosed)
            {
                CloseSession();
            }

            if (!_ctx.OwnsAeronClient())
            {
                ErrorHandler errorHandler = _ctx.ErrorHandler();
                CloseHelper.Dispose(errorHandler, _subscription);
                CloseHelper.Dispose(errorHandler, _publication);
            }

            _isClosed = true;
            _ctx.Dispose();
        }

        /// <summary>
        /// Is the client closed? The client can be closed by calling <seealso cref="Dispose()"/> or the cluster sending an event.
        /// </summary>
        /// <returns> true if closed otherwise false. </returns>
        public bool Closed => _isClosed;

        /// <summary>
        /// Get the context used to launch this cluster client.
        /// </summary>
        /// <returns> the context used to launch this cluster client. </returns>
        public Context Ctx => _ctx;

        /// <summary>
        /// Cluster session id for the session that was opened as the result of a successful connect.
        /// </summary>
        /// <returns> session id for the session that was opened as the result of a successful connect. </returns>
        public long ClusterSessionId => _clusterSessionId;

        /// <summary>
        /// Leadership term identity for the cluster. Advances with changing leadership.
        /// </summary>
        /// <returns> leadership term identity for the cluster. </returns>
        public long LeadershipTermId => _leadershipTermId;


        /// <summary>
        /// Get the current leader member id for the cluster.
        /// </summary>
        /// <returns> the current leader member id for the cluster. </returns>
        public int LeaderMemberId => _leaderMemberId;

        /// <summary>
        /// Get the raw <seealso cref="Publication"/> for sending to the cluster.
        /// <para>
        /// This can be wrapped with a <seealso cref="IngressSessionDecorator"/> for pre-pending the cluster session header to
        /// messages.
        /// <seealso cref="SessionMessageHeaderEncoder"/> should be used for raw access.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the raw <seealso cref="Publication"/> for connecting to the cluster. </returns>
        public Publication IngressPublication => _publication;

        /// <summary>
        /// Get the raw <seealso cref="Subscription"/> for receiving from the cluster.
        ///
        /// this can be wrapped with a <seealso cref="EgressAdapter"/> for dispatching events from the cluster.
        /// <see cref="SessionMessageHeaderDecoder"/> should be used for raw access.
        /// 
        /// </summary>
        /// <returns> the raw <seealso cref="Subscription"/> for receiving from the cluster. </returns>
        public Subscription EgressSubscription => _subscription;

        /// <summary>
        /// Try to claim a range in the publication log into which a message can be written with zero copy semantics.
        /// Once the message has been written then <seealso cref="BufferClaim.Commit()"/> should be called thus making it available.
        /// <para>
        /// On successful claim, the Cluster ingress header will be written to the start of the claimed buffer section.
        /// Clients <b>MUST</b> write into the claimed buffer region at offset + <seealso cref="SESSION_HEADER_LENGTH"/>.
        /// <pre>{@code
        ///     final IDirectBuffer srcBuffer = AcquireMessage();
        ///    
        ///     if (aeronCluster.TryClaim(length, bufferClaim) > 0L)
        ///     {
        ///         try
        ///         {
        ///              final IMutableDirectBuffer buffer = bufferClaim.Buffer;
        ///              final int offset = bufferClaim.Offset;
        ///              // ensure that data is written at the correct offset
        ///              buffer.PutBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
        ///         }
        ///         finally
        ///         {
        ///             bufferClaim.Commit();
        ///         }
        ///     }
        /// }</pre>
        ///    
        /// </para>
        /// </summary>
        /// <param name="length">      of the range to claim, in bytes. The additional bytes for the session header will be added. </param>
        /// <param name="bufferClaim"> to be populated if the claim succeeds. </param>
        /// <returns> The new stream position, otherwise a negative error value as specified in
        /// <seealso cref="Publication.TryClaim(int, BufferClaim)"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="Publication.MaxPayloadLength"/>. </exception>
        /// <seealso cref="Publication.TryClaim(int, BufferClaim)"/>
        /// <seealso cref="BufferClaim.Commit()"/>
        /// <seealso cref="BufferClaim.Abort()"/>
        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            long offset = _publication.TryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);
            if (offset > 0)
            {
                bufferClaim.PutBytes(_headerBuffer, 0, SESSION_HEADER_LENGTH);
            }

            return offset;
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
        /// <para>
        /// This version of the method will set the timestamp value in the header to zero.
        ///     
        /// </para>
        /// </summary>
        /// <param name="buffer">        containing message. </param>
        /// <param name="offset">        offset in the buffer at which the encoded message begins. </param>
        /// <param name="length">        in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int, ReservedValueSupplier)"/>. </returns>
        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            return _publication.Offer(_headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
        }

        /// <summary>
        /// Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced by the cluster
        /// session message header so must be left unused.
        /// </summary>
        /// <param name="vectors"> which make up the message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/>. </returns>
        /// <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/>
        public long Offer(DirectBufferVector[] vectors)
        {
            vectors[0] = _headerVector;

            return _publication.Offer(vectors);
        }

        /// <summary>
        /// Send a keep alive message to the cluster to keep this session open.
        ///
        /// Note: Sending keep-alives can fail during a leadership transition. The application should continue to call
        /// <see cref="PollEgress"/> to ensure a connection to the new leader is established.
        /// 
        /// </summary>
        /// <returns> true if successfully sent otherwise false if back pressured. </returns>
        public bool SendKeepAlive()
        {
            _idleStrategy.Reset();
            int attempts = SEND_ATTEMPTS;

            while (true)
            {
                int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionKeepAliveEncoder.BLOCK_LENGTH;

                long result = _publication.TryClaim(length, _bufferClaim);

                if (result > 0)
                {
                    _sessionKeepAliveEncoder
                        .WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .LeadershipTermId(_leadershipTermId)
                        .ClusterSessionId(_clusterSessionId);

                    _bufferClaim.Commit();
                    
                    return true;
                }

                if (result == Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new ClusterException("max position exceeded");
                }

                if (--attempts <= 0)
                {
                    break;
                }

                _idleStrategy.Idle();
                InvokeInvokers();
            }

            return false;
        }
        
        /// <summary>
        /// Sends an admin request to initiate a snapshot action in the cluster. This request requires elevated privileges.
        /// </summary>
        /// <param name="correlationId"> for the request. </param>
        /// <returns> {@code true} if the request was sent or {@code false} otherwise. </returns>
        /// <seealso cref="IEgressListener.OnAdminResponse(long, long, AdminRequestType, AdminResponseCode, String, IDirectBuffer, int, int)"/>
        /// <seealso cref="IControlledEgressListener.OnAdminResponse(long, long, AdminRequestType, AdminResponseCode, String, IDirectBuffer, int, int)"/>
        public bool SendAdminRequestToTakeASnapshot(long correlationId)
        {
            _idleStrategy.Reset();
            int attempts = SEND_ATTEMPTS;

            int length = MessageHeaderEncoder.ENCODED_LENGTH + AdminRequestEncoder.BLOCK_LENGTH + AdminRequestEncoder.PayloadHeaderLength();

            while (true)
            {
                long result = _publication.TryClaim(length, _bufferClaim);
                if (result > 0)
                {
                    _adminRequestEncoder.WrapAndApplyHeader(_bufferClaim.Buffer, _bufferClaim.Offset, _messageHeaderEncoder)
                        .LeadershipTermId(_leadershipTermId)
                        .ClusterSessionId(_clusterSessionId)
                        .CorrelationId(correlationId)
                        .RequestType(AdminRequestType.SNAPSHOT)
                        .PutPayload(Array.Empty<byte>(), 0, 0);

                    _bufferClaim.Commit();

                    return true;
                }

                if (result == Publication.CLOSED)
                {
                    throw new ClusterException("ingress publication is closed");
                }

                if (result == Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new ClusterException("max position exceeded");
                }

                if (--attempts <= 0)
                {
                    break;
                }

                _idleStrategy.Idle();
                InvokeInvokers();
            }

            return false;
        }


        /// <summary>
        /// Poll the <seealso cref="EgressSubscription()"/> for session messages which are dispatched to
        /// <seealso cref="Context.EgressListener()"/>. Invoking this method, or <seealso cref="ControlledPollEgress()"/>, frequently is
        /// important for detecting leadership changes in a cluster.
        /// <para>
        /// <b>Note:</b> if <seealso cref="Context.EgressListener()"/> is not set then a <seealso cref="ConfigurationException"/> could result.
        ///     
        /// </para>
        /// </summary>
        /// <returns> the number of fragments processed. </returns>
        /// <seealso cref="ControlledPollEgress()"/>
        public int PollEgress()
        {
            var fragments = _subscription.Poll(_fragmentAssembler, FRAGMENT_LIMIT);

            if (egressImage.Closed)
            {
                _publication.Dispose();
            }

            if (_isClosed)
            {
                Dispose();
            }

            return fragments;
        }

        /// <summary>
        /// Poll the <seealso cref="EgressSubscription()"/> for session messages which are dispatched to
        /// <seealso cref="Context.ControlledEgressListener()"/>. Invoking this method, or <seealso cref="PollEgress()"/>, frequently is
        /// important for detecting leadership changes in a cluster.
        /// <para>
        /// <b>Note:</b> if <seealso cref="Context.ControlledEgressListener()"/> is not set then a <seealso cref="ConfigurationException"/>
        /// could result.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the number of fragments processed. </returns>
        /// <seealso cref="PollEgress()"/>
        public int ControlledPollEgress()
        {
            var fragments = _subscription.ControlledPoll(_controlledFragmentAssembler, FRAGMENT_LIMIT);

            if (egressImage.Closed)
            {
                _publication.Dispose();
            }
            
            if (_isClosed)
            {
                Dispose();
            }

            return fragments;
        }

        /// <summary>
        /// To be called when a new leader event is delivered. This method needs to be called when using the
        /// <seealso cref="EgressAdapter"/> or <seealso cref="EgressPoller"/> rather than <seealso cref="PollEgress()"/> method.
        /// </summary>
        /// <param name="clusterSessionId"> which must match <seealso cref="ClusterSessionId()"/>. </param>
        /// <param name="leadershipTermId"> that identifies the term for which the new leader has been elected.</param>
        /// <param name="leaderMemberId">   which has become the new leader. </param>
        /// <param name="ingressEndpoints">  comma separated list of cluster ingress endpoints to connect to with the leader first. </param>
        public void OnNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId,
            string ingressEndpoints)
        {
            if (clusterSessionId != _clusterSessionId)
            {
                throw new ClusterException("invalid clusterSessionId=" + clusterSessionId + " expected=" +
                                           _clusterSessionId);
            }


            _leadershipTermId = leadershipTermId;
            _leaderMemberId = leaderMemberId;
            _sessionMessageEncoder.LeadershipTermId(leadershipTermId);

            _publication?.Dispose();
            if (_ctx.IngressEndpoints() != null)
            {
                _fragmentAssembler.Clear();
                _ctx.IngressEndpoints(ingressEndpoints);
                UpdateMemberEndpoints(ingressEndpoints, leaderMemberId);
            }
            else
            {
                _publication = AddIngressPublication(_ctx, _ctx.IngressChannel(), _ctx.IngressStreamId());
            }

            _fragmentAssembler.Clear();
            _controlledFragmentAssembler.Clear();
            _egressListener.OnNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
            _controlledEgressListener.OnNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
        }

        private static Map<int, MemberIngress> ParseIngressEndpoints(string endpoints)
        {
            var endpointByIdMap = new Map<int, MemberIngress>();

            if (null != endpoints)
            {
                foreach (var endpoint in endpoints.Split(','))
                {
                    int separatorIndex = endpoint.IndexOf('=');
                    if (-1 == separatorIndex)
                    {
                        throw new ConfigurationException("invalid format - member missing '=' separator: " + endpoints);
                    }

                    int memberId = int.Parse(endpoint.Substring(0, separatorIndex));
                    endpointByIdMap.Put(memberId, new MemberIngress(memberId, endpoint.Substring(separatorIndex + 1)));
                }
            }

            return endpointByIdMap;
        }

        private static Publication AddIngressPublication(Context ctx, string channel, int streamId)
        {
            if (ctx.IsIngressExclusive())
            {
                return ctx.Aeron().AddExclusivePublication(channel, streamId);
            }
            else
            {
                return ctx.Aeron().AddPublication(channel, streamId);
            }
        }

        private void UpdateMemberEndpoints(string ingressEndpoints, int leaderMemberId)
        {
            CloseHelper.CloseAll(_endpointByIdMap.Values);
            
            var map = ParseIngressEndpoints(ingressEndpoints);
            var newLeader = map.Get(leaderMemberId);
            var channelUri = ChannelUri.Parse(_ctx.IngressChannel());
            
            if (channelUri.IsUdp)
            {
                channelUri.Put(Aeron.Aeron.Context.ENDPOINT_PARAM_NAME, newLeader.endpoint);
            }
            
            _publication = AddIngressPublication(_ctx, channelUri.ToString(), _ctx.IngressStreamId());
            newLeader.publication = _publication;
            _endpointByIdMap = map;
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
                        .LeadershipTermId(_leadershipTermId)
                        .ClusterSessionId(_clusterSessionId);

                    _bufferClaim.Commit();
                    break;
                }

                if (--attempts <= 0)
                {
                    break;
                }

                _idleStrategy.Idle();
                InvokeInvokers();
            }
        }

        private void InvokeInvokers()
        {
            if (null != _ctx.Aeron().ConductorAgentInvoker)
            {
                _ctx.Aeron().ConductorAgentInvoker.Invoke();
            }

            if (null != _ctx.AgentInvoker())
            {
                _ctx.AgentInvoker().Invoke();
            }
        }

        /// <summary>
        /// Configuration options for cluster client.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Major version of the network protocol from client to consensus module. If these don't match then client
            /// and consensus module are not compatible.
            /// </summary>
            public const int PROTOCOL_MAJOR_VERSION = 0;

            /// <summary>
            /// Minor version of the network protocol from client to consensus module. If these don't match then some
            /// features may not be available.
            /// </summary>
            public const int PROTOCOL_MINOR_VERSION = 3;

            /// <summary>
            /// Patch version of the network protocol from client to consensus module. If these don't match then bug fixes
            /// may not have been applied.
            /// </summary>
            public const int PROTOCOL_PATCH_VERSION = 0;

            /// <summary>
            /// Combined semantic version for the client to consensus module protocol.
            /// </summary>
            /// <seealso cref="SemanticVersion"/>
            public static readonly int PROTOCOL_SEMANTIC_VERSION =
                SemanticVersion.Compose(PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION, PROTOCOL_PATCH_VERSION);

            /// <summary>
            /// Timeout when waiting on a message to be sent or received.
            /// </summary>
            public const string MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

            /// <summary>
            /// Default timeout when waiting on a message to be sent or received.
            /// </summary>
            public static readonly long MESSAGE_TIMEOUT_DEFAULT_NS = 5000000000;

            /// <summary>
            /// Property name for the comma separated list of cluster ingress endpoints for use with unicast. This is the
            /// endpoint values which get substituted into the <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> when using UDP unicast.
            ///
            /// <code>0=endpoint,1=endpoint,2=endpoint</code>
            /// 
            /// Each member of the list will be substituted for the endpoint in the <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> value.
            /// 
            /// </summary>
            public const string INGRESS_ENDPOINTS_PROP_NAME = "aeron.cluster.ingress.endpoints";

            /// <summary>
            /// Default comma separated list of cluster ingress endpoints.
            /// </summary>
            public const string INGRESS_ENDPOINTS_DEFAULT = null;

            /// <summary>
            /// Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
            /// be required and the <seealso cref="INGRESS_ENDPOINTS_PROP_NAME"/> is used to substitute the endpoints from
            /// the <seealso cref="INGRESS_ENDPOINTS_PROP_NAME"/> list.
            /// </summary>
            public const string INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

            /// <summary>
            /// Channel for sending messages to a cluster.
            /// </summary>
            public const string INGRESS_CHANNEL_DEFAULT = null;

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
            /// <para>
            /// Channel's <em>endpoint</em> can be specified explicitly (i.e. by providing address and port pair) or
            /// by using zero as a port number. Here is an example of valid response channels:
            /// <ul>
            ///     <li>{@code aeron:udp?endpoint=localhost:9020} - listen on port {@code 9020} on localhost.</li>
            ///     <li>{@code aeron:udp?endpoint=192.168.10.10:9020} - listen on port {@code 9020} on
            ///     {@code 192.168.10.10}.</li>
            ///     <li>{@code aeron:udp?endpoint=localhost:0} - in this case the port is unspecified and the OS
            ///     will assign a free port from the
            ///     <a href="https://en.wikipedia.org/wiki/Ephemeral_port">ephemeral port range</a>.</li>
            /// </ul>
            /// </para>
            /// </summary>
            public const string EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

            /// <summary>
            /// Channel for receiving response messages from a cluster.
            /// </summary>
            public const string EGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:0";

            /// <summary>
            /// Stream id within a channel for receiving messages from a cluster.
            /// </summary>
            public const string EGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.egress.stream.id";

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
            /// The value <seealso cref="INGRESS_ENDPOINTS_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_ENDPOINTS_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="INGRESS_ENDPOINTS_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_ENDPOINTS_PROP_NAME"/> if set. </returns>
            public static string IngressEndpoints()
            {
                return
                    Config.GetProperty(INGRESS_ENDPOINTS_PROP_NAME, INGRESS_ENDPOINTS_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="INGRESS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="INGRESS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="INGRESS_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string IngressChannel()
            {
                return Config.GetProperty(INGRESS_CHANNEL_PROP_NAME, INGRESS_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="INGRESS_STREAM_ID_DEFAULT"/> or system property <seealso cref="INGRESS_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="INGRESS_STREAM_ID_DEFAULT"/> or system property <seealso cref="INGRESS_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int IngressStreamId()
            {
                return Config.GetInteger(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="EGRESS_CHANNEL_DEFAULT"/> or system property <seealso cref="EGRESS_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="EGRESS_CHANNEL_DEFAULT"/> or system property <seealso cref="EGRESS_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string EgressChannel()
            {
                return Config.GetProperty(EGRESS_CHANNEL_PROP_NAME, EGRESS_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="EGRESS_STREAM_ID_DEFAULT"/> or system property <seealso cref="EGRESS_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="EGRESS_STREAM_ID_DEFAULT"/> or system property <seealso cref="EGRESS_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int EgressStreamId()
            {
                return Config.GetInteger(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT);
            }
        }

        /// <summary>
        /// Context for cluster session and connection.
        /// </summary>
        public class Context
        {
            private class MissingEgressMessageListener : IEgressListener, IControlledEgressListener
            {
                public void OnMessage(
                    long clusterSessionId,
                    long timestamp,
                    IDirectBuffer buffer,
                    int offset,
                    int length,
                    Header header)
                {
                    throw new ConfigurationException("egressMessageListener must be specified on AeronCluster.Context");
                }

                ControlledFragmentHandlerAction IControlledEgressListener.OnMessage(
                    long clusterSessionId,
                    long timestamp,
                    IDirectBuffer buffer,
                    int offset,
                    int length,
                    Header header)
                {
                    throw new ConfigurationException(
                        "controlledEgressListened must be specified on AeronCluster.Context");
                }

                public void OnSessionEvent(
                    long correlationId,
                    long clusterSessionId,
                    long leadershipTermId,
                    int leaderMemberId,
                    EventCode code,
                    string detail)
                {
                }

                public void OnNewLeader(
                    long clusterSessionId,
                    long leadershipTermId,
                    int leaderMemberId,
                    string memberEndpoints)
                {
                }

                void IControlledEgressListener.OnAdminResponse(long clusterSessionId, long correlationId, AdminRequestType requestType,
                    AdminResponseCode responseCode, string message, IDirectBuffer payload, int payloadOffset, int payloadLength)
                {
                }

                void IEgressListener.OnAdminResponse(long clusterSessionId, long correlationId, AdminRequestType requestType,
                    AdminResponseCode responseCode, string message, IDirectBuffer payload, int payloadOffset, int payloadLength)
                {
                }
            }

            private int _isConcluded = 0;
            private long _messageTimeoutNs = Configuration.MessageTimeoutNs();
            private string _ingressEndpoints = Configuration.IngressEndpoints();
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
            private bool _isDirectAssemblers = false;
            private IEgressListener _egressListener;
            private IControlledEgressListener _controlledEgressListener;
            private AgentInvoker agentInvoker;

            /// <summary>
            /// Perform a shallow copy of the object.
            /// </summary>
            /// <returns> a shall copy of the object.</returns>
            public Context Clone()
            {
                return (Context)MemberwiseClone();
            }

            /// <summary>
            /// Conclude configuration by setting up defaults when specifics are not provided.
            /// </summary>
            public void Conclude()
            {
                if (0 != Interlocked.Exchange(ref _isConcluded, 1))
                {
                    throw new ConcurrentConcludeException();
                }

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

                if (null == _egressListener)
                {
                    _egressListener = new MissingEgressMessageListener();
                }

                if (null == _controlledEgressListener)
                {
                    _controlledEgressListener = new MissingEgressMessageListener();
                }
                
                if (_ingressChannel.StartsWith(Adaptive.Aeron.Aeron.Context.IPC_CHANNEL))
                {
                    if (null != _ingressEndpoints)
                    {
                        throw new ConfigurationException("AeronCluster.Context ingressEndpoints must be null when using IPC ingress");
                    }
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
                return Adaptive.Aeron.Aeron.Context.CheckDebugTimeout(_messageTimeoutNs, TimeUnit.NANOSECONDS,
                    nameof(MessageTimeoutNs));
            }

            /// <summary>
            /// The endpoints representing members for use with unicast to be substituted into the <seealso cref="IngressChannel()"/>
            /// for endpoints. A null value can be used when multicast where the <seealso cref="IngressChannel()"/> contains the
            /// multicast endpoint.
            /// </summary>
            /// <param name="clusterMembers"> which are all candidates to be leader. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.INGRESS_ENDPOINTS_PROP_NAME"></seealso>
            public Context IngressEndpoints(string clusterMembers)
            {
                _ingressEndpoints = clusterMembers;
                return this;
            }

            /// <summary>
            /// The endpoints representing members for use with unicast to be substituted into the <seealso cref="IngressChannel()"/>
            /// for endpoints. A null value can be used when multicast where the <seealso cref="IngressChannel()"/> contains the
            /// multicast endpoint.
            /// </summary>
            /// <returns> members of the cluster which are all candidates to be leader. </returns>
            /// <seealso cref="Configuration.INGRESS_ENDPOINTS_PROP_NAME"></seealso>
            public string IngressEndpoints()
            {
                return _ingressEndpoints;
            }

            /// <summary>
            /// Set the channel parameter for the ingress channel.
            /// <para>
            /// The endpoints representing members for use with unicast are substituted from the <seealso cref="IngressEndpoints()"/>
            /// for endpoints. If this channel contains a multicast endpoint, then <see cref="IngressEndpoints()"/> should
            /// be set to null. 
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
            /// <seealso cref="IngressEndpoints()"/> for endpoints. A null value can be used when multicast
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
            /// <seealso cref="Configuration.EGRESS_STREAM_ID_PROP_NAME"/>
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
            /// Is ingress to the cluster exclusively from a single thread to this client? The client should not be used
            /// from another thread, e.g. a separate thread calling <seealso cref="AeronCluster.SendKeepAlive()"/> - which is awful
            /// design by the way!
            /// </summary>
            /// <param name="isIngressExclusive"> true if ingress to the cluster is exclusively from a single thread for this client? </param>
            /// <returns> this for a fluent API. </returns>
            public Context IsIngressExclusive(bool isIngressExclusive)
            {
                _isIngressExclusive = isIngressExclusive;
                return this;
            }

            /// <summary>
            /// Is ingress the <seealso cref="Publication"/> to the cluster used exclusively from a single thread to this client?
            /// </summary>
            /// <returns> true if the ingress <seealso cref="Publication"/> is to be used exclusively from a single thread? </returns>
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
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                _errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Is direct buffers used for fragment assembly on egress.
            /// </summary>
            /// <returns> true if direct buffers used for fragment assembly on egress. </returns>
            public bool IsDirectAssemblers()
            {
                return _isDirectAssemblers;
            }

            /// <summary>
            /// Is direct buffers used for fragment assembly on egress.
            /// </summary>
            /// <param name="isDirectAssemblers"> true if direct buffers used for fragment assembly on egress. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IsDirectAssemblers(bool isDirectAssemblers)
            {
                _isDirectAssemblers = isDirectAssemblers;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IEgressListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.PollEgress()"/>.
            /// </summary>
            /// <returns> the <seealso cref="IEgressListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.PollEgress()"/>. </returns>
            public IEgressListener EgressListener()
            {
                return _egressListener;
            }

            /// <summary>
            /// Get the <seealso cref="IEgressListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.PollEgress()"/>.
            ///
            /// Only <see cref="IEgressListener.OnMessage"/> will be dispatched
            /// when using <see cref="AeronCluster.PollEgress()"/>
            /// 
            /// </summary>
            /// <param name="listener"> function that will be called when polling for egress via <seealso cref="AeronCluster.PollEgress()"/>. </param>
            /// <returns> this for a fluent API. </returns>
            public Context EgressListener(IEgressListener listener)
            {
                _egressListener = listener;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IControlledEgressListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.ControlledPollEgress"/>.
            /// </summary>
            /// <returns> the <seealso cref="IControlledEgressListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.ControlledPollEgress"/>. </returns>
            public IControlledEgressListener ControlledEgressListener()
            {
                return _controlledEgressListener;
            }

            /// <summary>
            /// Get the <seealso cref="IControlledEgressListener"/> function that will be called when polling for egress via
            /// <seealso cref="AeronCluster.ControlledPollEgress"/>.
            /// 
            /// Only <seealso cref="IControlledEgressListener.OnMessage(long, long, IDirectBuffer, int, int, Header)"/> will be
            /// dispatched when using <seealso cref="AeronCluster.ControlledPollEgress"/>.
            /// </summary>
            /// <param name="listener"> function that will be called when polling for egress via
            ///                 <seealso cref="AeronCluster.ControlledPollEgress"/>. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ControlledEgressListener(IControlledEgressListener listener)
            {
                _controlledEgressListener = listener;
                return this;
            }
            
            /// <summary>
            /// Set the <seealso cref="Adaptive.Agrona.Concurrent.AgentInvoker"/> to be invoked in addition to any invoker used by the <seealso cref="Aeron()"/> instance.
            /// <para>
            /// Useful for when running on a low thread count scenario.
            /// 
            /// </para>
            /// </summary>
            /// <param name="agentInvoker"> to be invoked while awaiting a response in the client or when awaiting completion. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AgentInvoker(AgentInvoker agentInvoker)
            {
                this.agentInvoker = agentInvoker;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Adaptive.Agrona.Concurrent.AgentInvoker"/> to be invoked in addition to any invoker used by the <seealso cref="Aeron()"/> instance.
            /// </summary>
            /// <returns> the <seealso cref="Adaptive.Agrona.Concurrent.AgentInvoker"/> that is used. </returns>
            public AgentInvoker AgentInvoker()
            {
                return agentInvoker;
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

            public override string ToString()
            {
                return "AeronCluster.Context" +
                       "\n{" +
                       "\n    isConcluded=" + (1 == _isConcluded) +
                       "\n    ownsAeronClient=" + _ownsAeronClient +
                       "\n    aeronDirectoryName='" + _aeronDirectoryName + '\'' +
                       "\n    aeron=" + _aeron +
                       "\n    messageTimeoutNs=" + _messageTimeoutNs +
                       "\n    ingressEndpoints='" + _ingressEndpoints + '\'' +
                       "\n    ingressChannel='" + _ingressChannel + '\'' +
                       "\n    ingressStreamId=" + _ingressStreamId +
                       "\n    egressChannel='" + _egressChannel + '\'' +
                       "\n    egressStreamId=" + _egressStreamId +
                       "\n    idleStrategy=" + _idleStrategy +
                       "\n    credentialsSupplier=" + _credentialsSupplier +
                       "\n    isIngressExclusive=" + _isIngressExclusive +
                       "\n    errorHandler=" + _errorHandler +
                       "\n    isDirectAssemblers=" + _isDirectAssemblers +
                       "\n    egressListener=" + _egressListener +
                       "\n    controlledEgressListener=" + _controlledEgressListener +
                       "\n}";
            }
        }

        /// <summary>
        /// Allows for the async establishment of a cluster session. <seealso cref="Poll()"/> should be called repeatedly until
        /// it returns a non-null value with the new <seealso cref="AeronCluster"/> client. On error <seealso cref="Dispose()"/> should be called
        /// to clean up allocated resources.
        /// </summary>
        public class AsyncConnect : IDisposable
        {
            private readonly Subscription egressSubscription;
            private Image egressImage;
            private readonly long deadlineNs;
            private long correlationId = Aeron.Aeron.NULL_VALUE;
            private long clusterSessionId;
            private long leadershipTermId;
            private int leaderMemberId;
            private int step = 0;
            private int messageLength = 0;

            private readonly Context ctx;
            private readonly INanoClock nanoClock;
            private readonly EgressPoller egressPoller;
            private readonly UnsafeBuffer buffer = new UnsafeBuffer(new byte[64 * 1024]); // TODO ExpandableArrayBuffer
            private readonly MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
            private Map<int, MemberIngress> memberByIdMap;
            private Publication ingressPublication;
            private RegistrationException registrationException;

            internal AsyncConnect(Context ctx, Subscription egressSubscription, long deadlineNs)
            {
                this.ctx = ctx;

                memberByIdMap = ParseIngressEndpoints(ctx.IngressEndpoints());
                this.egressSubscription = egressSubscription;
                egressPoller = new EgressPoller(egressSubscription, FRAGMENT_LIMIT);
                nanoClock = ctx.Aeron().Ctx.NanoClock();
                this.deadlineNs = deadlineNs;
            }

            /// <summary>
            /// Close allocated resources. Must be called on error. On success this is a no op.
            /// </summary>
            public void Dispose()
            {
                if (5 != step)
                {
                    ErrorHandler errorHandler = ctx.ErrorHandler();
                    CloseHelper.Dispose(errorHandler, ingressPublication);

                    foreach (var memberEndpoint in memberByIdMap.Values)
                    {
                        CloseHelper.Dispose(errorHandler, memberEndpoint);
                    }

                    ctx.Dispose();
                }
            }

            /// <summary>
            /// Indicates which step in the connect process has been reached.
            /// </summary>
            /// <returns> which step in the connect process has been reached. </returns>
            public int Step()
            {
                return step;
            }

            private void Step(int newStep)
            {
                //Console.WriteLine(this.step + " -> " + step);
                this.step = newStep;
            }

            /// <summary>
            /// Poll to advance steps in the connection until complete or error.
            /// </summary>
            /// <returns> null if not yet complete then <seealso cref="AeronCluster"/> when complete. </returns>
            public AeronCluster Poll()
            {
                AeronCluster aeronCluster = null;
                CheckDeadline();

                switch (step)
                {
                    case 0:
                        CreateIngressPublications();
                        break;

                    case 1:
                        AwaitPublicationConnected();
                        break;

                    case 2:
                        SendMessage();
                        break;

                    case 3:
                        PollResponse();
                        break;
                }

                if (4 == step)
                {
                    aeronCluster = NewInstance();
                    ingressPublication = null;
                    memberByIdMap.Remove(leaderMemberId);
                    CloseHelper.CloseAll(memberByIdMap.Values);

                    Step(5);
                }

                return aeronCluster;
            }

            private void CheckDeadline()
            {
                if (deadlineNs - nanoClock.NanoTime() < 0)
                {
                    var exceptionMessage = "cluster connect timeout: step=" + step + " ingressChannel=" +
                                           ctx.IngressChannel() + " ingressPublication=" + ingressPublication +
                                           " egress.isConnected=" + egressSubscription.IsConnected +
                                           " responseChannel=" +
                                           egressSubscription.TryResolveChannelEndpointPort();
                    if (null != registrationException)
                    {
                        throw new AeronTimeoutException(exceptionMessage, registrationException);
                    }

                    throw new AeronTimeoutException(exceptionMessage);
                }

                try
                {
                    Thread.Sleep(0);
                }
                catch (ThreadInterruptedException)
                {
                    throw new AeronException("unexpected interrupt");
                }
            }

            private void CreateIngressPublications()
            {
                if (null == ctx.IngressEndpoints())
                {
                    ingressPublication = AddIngressPublication(ctx, ctx.IngressChannel(), ctx.IngressStreamId());
                }
                else
                {
                    int publicationCount = 0;
                    ChannelUri channelUri = ChannelUri.Parse(ctx.IngressChannel());

                    foreach (MemberIngress member in memberByIdMap.Values)
                    {
                        try
                        {
                            if (channelUri.IsUdp)
                            {
                                channelUri.Put(Aeron.Aeron.Context.ENDPOINT_PARAM_NAME, member.endpoint);
                            }

                            member.publication =
                                AddIngressPublication(ctx, channelUri.ToString(), ctx.IngressStreamId());
                            ++publicationCount;
                        }
                        catch (RegistrationException ex)
                        {
                            registrationException = ex;
                        }
                    }

                    if (0 == publicationCount && null != registrationException)
                    {
                        throw registrationException;
                    }
                }

                Step(1);
            }

            private void AwaitPublicationConnected()
            {
                string responseChannel = egressSubscription.TryResolveChannelEndpointPort();
                if (null != responseChannel)
                {
                    if (null == ingressPublication)
                    {
                        foreach (MemberIngress member in memberByIdMap.Values)
                        {
                            if (null != member.publication && member.publication.IsConnected)
                            {
                                ingressPublication = member.publication;
                                PrepareConnectRequest(responseChannel);
                                break;
                            }
                        }
                    }
                    else if (ingressPublication.IsConnected)
                    {
                        PrepareConnectRequest(responseChannel);
                    }
                }
            }


            private void PrepareConnectRequest(String responseChannel)
            {
                correlationId = ctx.Aeron().NextCorrelationId();
                var encodedCredentials = ctx.CredentialsSupplier().EncodedCredentials();

                var encoder = new SessionConnectRequestEncoder()
                    .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                    .CorrelationId(correlationId)
                    .ResponseStreamId(ctx.EgressStreamId())
                    .Version(Configuration.PROTOCOL_SEMANTIC_VERSION)
                    .ResponseChannel(responseChannel)
                    .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

                messageLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.EncodedLength();
                Step(2);
            }

            private void SendMessage()
            {
                long result = ingressPublication.Offer(buffer, 0, messageLength);
                if (result > 0)
                {
                    Step(3);
                }
                else if (Publication.CLOSED == result || Publication.NOT_CONNECTED == result)
                {
                    throw new ClusterException("unexpected loss of connection to cluster");
                }
            }

            private void PollResponse()
            {
                if (egressPoller.Poll() > 0 && egressPoller.IsPollComplete() &&
                    egressPoller.CorrelationId() == correlationId)
                {
                    if (egressPoller.IsChallenged())
                    {
                        correlationId = Aeron.Aeron.NULL_VALUE;
                        clusterSessionId = egressPoller.ClusterSessionId();
                        PrepareChallengeResponse(ctx.CredentialsSupplier()
                            .OnChallenge(egressPoller.EncodedChallenge()));
                        return;
                    }

                    switch (egressPoller.EventCode())
                    {
                        case EventCode.OK:
                            leadershipTermId = egressPoller.LeadershipTermId();
                            leaderMemberId = egressPoller.LeaderMemberId();
                            clusterSessionId = egressPoller.ClusterSessionId();
                            egressImage = egressPoller.EgressImage();
                            Step(4);
                            break;

                        case EventCode.ERROR:
                            throw new ClusterException(egressPoller.Detail());

                        case EventCode.REDIRECT:
                            UpdateMembers();
                            break;

                        case EventCode.AUTHENTICATION_REJECTED:
                            throw new AuthenticationException(egressPoller.Detail());
                    }
                }
            }

            private void PrepareChallengeResponse(byte[] encodedCredentials)
            {
                correlationId = ctx.Aeron().NextCorrelationId();

                var encoder = new ChallengeResponseEncoder()
                    .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                    .CorrelationId(correlationId).ClusterSessionId(clusterSessionId)
                    .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

                messageLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.EncodedLength();

                Step(2);
            }

            private void UpdateMembers()
            {
                leaderMemberId = egressPoller.LeaderMemberId();
                MemberIngress leader = memberByIdMap.Get(leaderMemberId);
                if (null != leader)
                {
                    ingressPublication = leader.publication;
                    leader.publication = null;
                }

                CloseHelper.CloseAll(memberByIdMap.Values);
                memberByIdMap = ParseIngressEndpoints(egressPoller.Detail());

                if (null == ingressPublication)
                {
                    MemberIngress member = memberByIdMap.Get(leaderMemberId);
                    ChannelUri channelUri = ChannelUri.Parse(ctx.IngressChannel());
                    if (channelUri.IsUdp)
                    {
                        channelUri.Put(Aeron.Aeron.Context.ENDPOINT_PARAM_NAME, member.endpoint);
                    }
                    ingressPublication = AddIngressPublication(ctx, channelUri.ToString(), ctx.IngressStreamId());
                }

                Step(1);
            }

            private AeronCluster NewInstance()
            {
                return new AeronCluster(
                    ctx,
                    messageHeaderEncoder,
                    ingressPublication,
                    egressSubscription,
                    egressImage,
                    memberByIdMap,
                    clusterSessionId,
                    leadershipTermId,
                    leaderMemberId);
            }
        }

        internal class MemberIngress : IDisposable
        {
            readonly int memberId;
            internal readonly string endpoint;
            internal Publication publication;

            internal MemberIngress(int memberId, string endpoint)
            {
                this.memberId = memberId;
                this.endpoint = endpoint;
            }

            public void Dispose()
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