using System;
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Typesafe means of building a channel URI associated with a <seealso cref="Publication"/> or <seealso cref="Subscription"/>.
    /// </summary>
    /// <seealso cref="Aeron.AddPublication"/>
    /// <seealso cref="Aeron.AddSubscription(string,int)"/>
    /// <seealso cref="ChannelUri"/>
    public sealed class ChannelUriStringBuilder
    {
        /// <summary>
        /// Can be used when the likes of session-id wants to reference another entity such as a tagged publication.
        /// <para>
        /// For example {@code session-id=tag:777} where the publication uses {@code tags=777}.
        /// </para>
        /// </summary>
        public const string TAG_PREFIX = "tag:";

        private readonly StringBuilder _sb = new StringBuilder(64);

        private string _prefix;
        private string _media;
        private string _endpoint;
        private string _networkInterface;
        private string _controlEndpoint;
        private string _controlMode;
        private string _tags;
        private string _alias;
        private string _cc;
        private string _fc;
        private bool? _reliable;
        private int? _ttl;
        private int? _mtu;
        private int? _termLength;
        private int? _initialTermId;
        private int? _termId;
        private int? _termOffset;
        private long? _sessionId;
        private long? _groupTag;
        private long? _linger;
        private bool? _sparse;
        private bool? _eos;
        private bool? _tether;
        private bool? _group;
        private bool? _rejoin;
        private bool? _ssc;
        private bool _isSessionIdTagged;
        private int? _socketSndbufLength;
        private int? _socketRcvbufLength;
        private int? _receiverWindowLength;
        private string _mediaReceiveTimestampOffset;
        private string _channelReceiveTimestampOffset;
        private string _channelSendTimestampOffset;

        /// <summary>
        /// Default constructor
        /// </summary>
        public ChannelUriStringBuilder()
        {
        }

        /// <summary>
        /// Constructs the ChannelUriStringBuilder with the initial values derived from the supplied URI. Will parse the
        /// incoming URI during this process, so could through an exception at this point of the URI is badly formed.
        /// </summary>
        /// <param name="initialUri"> initial values for the builder. </param>
        public ChannelUriStringBuilder(string initialUri) : this(ChannelUri.Parse(initialUri))
        {
        }

        /// <summary>
        /// Constructs the ChannelUriStringBuilder with the initial values derived from the supplied ChannelUri.
        /// </summary>
        /// <param name="channelUri"> initial values for the builder. </param>
        public ChannelUriStringBuilder(ChannelUri channelUri)
        {
            _isSessionIdTagged = false;

            Prefix(channelUri);
            Media(channelUri);
            Endpoint(channelUri);
            NetworkInterface(channelUri);
            ControlEndpoint(channelUri);
            ControlMode(channelUri);
            Tags(channelUri);
            Alias(channelUri);
            CongestionControl(channelUri);
            FlowControl(channelUri);
            Reliable(channelUri);
            Ttl(channelUri);
            Mtu(channelUri);
            TermLength(channelUri);
            InitialTermId(channelUri);
            TermId(channelUri);
            TermOffset(channelUri);
            SessionId(channelUri);
            Group(channelUri);
            Linger(channelUri);
            Sparse(channelUri);
            Eos(channelUri);
            Tether(channelUri);
            GroupTag(channelUri);
            Rejoin(channelUri);
            SpiesSimulateConnection(channelUri);
            SocketRcvbufLength(channelUri);
            SocketSndbufLength(channelUri);
            ReceiverWindowLength(channelUri);
            MediaReceiveTimestampOffset(channelUri);
            ChannelReceiveTimestampOffset(channelUri);
            ChannelSendTimestampOffset(channelUri);
        }

        /// <summary>
        /// Clear out all the values thus setting back to the initial state.
        /// </summary>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder Clear()
        {
            _prefix = null;
            _media = null;
            _endpoint = null;
            _networkInterface = null;
            _controlEndpoint = null;
            _controlMode = null;
            _tags = null;
            _alias = null;
            _cc = null;
            _fc = null;
            _reliable = null;
            _ttl = null;
            _mtu = null;
            _termLength = null;
            _initialTermId = null;
            _termId = null;
            _termOffset = null;
            _sessionId = null;
            _groupTag = null;
            _linger = null;
            _sparse = null;
            _eos = null;
            _tether = null;
            _group = null;
            _rejoin = null;
            _isSessionIdTagged = false;
            _socketRcvbufLength = null;
            _socketSndbufLength = null;
            _receiverWindowLength = null;
            _mediaReceiveTimestampOffset = null;
            _channelReceiveTimestampOffset = null;
            _channelSendTimestampOffset = null;

            return this;
        }

        /// <summary>
        /// Validates that the collection of set parameters are valid together.
        /// </summary>
        /// <returns> this for a fluent API. </returns>
        /// <exception cref="InvalidOperationException"> if the combination of params is invalid. </exception>
        public ChannelUriStringBuilder Validate()
        {
            if (null == _media)
            {
                throw new InvalidOperationException("media type is mandatory");
            }

            if (Aeron.Context.UDP_MEDIA.Equals(_media) && (null == _endpoint && null == _controlEndpoint))
            {
                throw new InvalidOperationException("either 'endpoint' or 'control' must be specified for UDP.");
            }

            
            bool anyNonNull = null != _initialTermId || null != _termId || null != _termOffset;
            bool anyNull = null == _initialTermId || null == _termId || null == _termOffset;
            if (anyNonNull)
            {
                if (anyNull)
                {
                    throw new ArgumentException(
                        "either all or none of the parameters ['initialTermId', 'termId', 'termOffset'] must be provided");
                }

                if (_termId - _initialTermId < 0)
                {
                    throw new ArgumentException(
                        "difference greater than 2^31 - 1: termId=" + _termId + " - initialTermId=" + _initialTermId);
                }

                if (null != _termLength && _termOffset > _termLength)
                {
                    throw new ArgumentException("termOffset=" + _termOffset + " > termLength=" + _termLength);
                }
            }

            return this;
        }

        /// <summary>
        /// Set the prefix for taking an additional action such as spying on an outgoing publication with "aeron-spy".
        /// </summary>
        /// <param name="prefix"> to be applied to the URI before the scheme. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="ChannelUri.SPY_QUALIFIER"/>
        public ChannelUriStringBuilder Prefix(string prefix)
        {
            if (null != prefix && !string.IsNullOrEmpty(prefix) && !prefix.Equals(ChannelUri.SPY_QUALIFIER))
            {
                throw new ArgumentException("invalid prefix: " + prefix);
            }

            _prefix = prefix;
            return this;
        }

        /// <summary>
        /// Set the prefix value to be what is in the <seealso cref="ChannelUri"/>.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="ChannelUri.SPY_QUALIFIER"/>
        public ChannelUriStringBuilder Prefix(ChannelUri channelUri)
        {
            return Prefix(channelUri.Prefix());
        }

        /// <summary>
        /// Get the prefix for the additional action to be taken on the request.
        /// </summary>
        /// <returns> the prefix for the additional action to be taken on the request. </returns>
        public string Prefix()
        {
            return _prefix;
        }

        /// <summary>
        /// Set the media for this channel. Valid values are "udp" and "ipc".
        /// </summary>
        /// <param name="media"> for this channel. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder Media(string media)
        {
            switch (media)
            {
                case Aeron.Context.UDP_MEDIA:
                case Aeron.Context.IPC_MEDIA:
                    break;

                default:
                    throw new ArgumentException("invalid media: " + media);
            }

            _media = media;
            return this;
        }

        /// <summary>
        /// Set the endpoint value to be what is in the <seealso cref="ChannelUri"/>.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder Media(ChannelUri channelUri)
        {
            return Media(channelUri.Media());
        }

        /// <summary>
        /// The media over which the channel transmits.
        /// </summary>
        /// <returns> the media over which the channel transmits. </returns>
        public string Media()
        {
            return _media;
        }

        /// <summary>
        /// Set the endpoint address:port pairing for the channel. This is the address the publication sends to and the
        /// address the subscription receives from.
        /// </summary>
        /// <param name="endpoint"> address and port for the channel. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.ENDPOINT_PARAM_NAME"/>
        public ChannelUriStringBuilder Endpoint(string endpoint)
        {
            _endpoint = endpoint;
            return this;
        }

        /// <summary>
        /// Set the endpoint value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.ENDPOINT_PARAM_NAME"/>
        public ChannelUriStringBuilder Endpoint(ChannelUri channelUri)
        {
            return Endpoint(channelUri.Get(Aeron.Context.ENDPOINT_PARAM_NAME));
        }

        /// <summary>
        /// Get the endpoint address:port pairing for the channel.
        /// </summary>
        /// <returns> the endpoint address:port pairing for the channel. </returns>
        /// <seealso cref="Aeron.Context.ENDPOINT_PARAM_NAME"/>
        public string Endpoint()
        {
            return _endpoint;
        }

        /// <summary>
        /// Set the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
        /// </summary>
        /// <param name="networkInterface"> for routing traffic. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.INTERFACE_PARAM_NAME"/>
        public ChannelUriStringBuilder NetworkInterface(string networkInterface)
        {
            _networkInterface = networkInterface;
            return this;
        }

        /// <summary>
        /// Set the network interface value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.INTERFACE_PARAM_NAME"/>
        public ChannelUriStringBuilder NetworkInterface(ChannelUri channelUri)
        {
            return NetworkInterface(channelUri.Get(Aeron.Context.INTERFACE_PARAM_NAME));
        }

        /// <summary>
        /// Get the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
        /// </summary>
        /// <returns> the address of the local interface in the form host:[port]/[subnet mask] for routing traffic. </returns>
        /// <seealso cref="Aeron.Context.INTERFACE_PARAM_NAME"/>
        public string NetworkInterface()
        {
            return _networkInterface;
        }

        /// <summary>
        /// Set the control address:port pair for dynamically joining a multi-destination-cast publication.
        /// </summary>
        /// <param name="controlEndpoint"> for joining MDC control socket. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME"/>
        public ChannelUriStringBuilder ControlEndpoint(string controlEndpoint)
        {
            _controlEndpoint = controlEndpoint;
            return this;
        }

        /// <summary>
        /// Set the control endpoint value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder ControlEndpoint(ChannelUri channelUri)
        {
            return ControlEndpoint(channelUri.Get(Aeron.Context.MDC_CONTROL_PARAM_NAME));
        }

        /// <summary>
        /// Get the control address:port pair for dynamically joining a multi-destination-cast publication.
        /// </summary>
        /// <returns> the control address:port pair for dynamically joining a multi-destination-cast publication. </returns>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME"/>
        public string ControlEndpoint()
        {
            return _controlEndpoint;
        }

        /// <summary>
        /// Set the control mode for multi-destination-cast. Set to "manual" for allowing control from the publication API.
        /// </summary>
        /// <param name="controlMode"> for taking control of MDC. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Publication.AddDestination(String)"></seealso>
        /// <seealso cref="Publication.RemoveDestination(String)"></seealso>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_MANUAL"/>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_DYNAMIC"/>
        public ChannelUriStringBuilder ControlMode(string controlMode)
        {
            if (null != controlMode &&
                !controlMode.Equals(Aeron.Context.MDC_CONTROL_MODE_MANUAL) &&
                !controlMode.Equals(Aeron.Context.MDC_CONTROL_MODE_DYNAMIC)
               )
            {
                throw new ArgumentException("invalid control mode: " + controlMode);
            }

            _controlMode = controlMode;
            return this;
        }

        /// <summary>
        /// Set the control mode to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME"/>
        public ChannelUriStringBuilder ControlMode(ChannelUri channelUri)
        {
            return ControlMode(channelUri.Get(Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME));
        }

        /// <summary>
        /// Get the control mode for multi-destination-cast.
        /// </summary>
        /// <returns> the control mode for multi-destination-cast. </returns>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_MANUAL"/>
        /// <seealso cref="Aeron.Context.MDC_CONTROL_MODE_DYNAMIC"/>
        public string ControlMode()
        {
            return _controlMode;
        }

        /// <summary>
        /// Set the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
        /// </summary>
        /// <param name="isReliable"> false if loss can be gap-filled. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.RELIABLE_STREAM_PARAM_NAME"/>
        public ChannelUriStringBuilder Reliable(bool? isReliable)
        {
            _reliable = isReliable;
            return this;
        }

        /// <summary>
        /// Set the reliable value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.RELIABLE_STREAM_PARAM_NAME"/>
        public ChannelUriStringBuilder Reliable(ChannelUri channelUri)
        {
            string reliableValue = channelUri.Get(Aeron.Context.RELIABLE_STREAM_PARAM_NAME);
            if (null == reliableValue)
            {
                _reliable = null;
                return this;
            }
            else
            {
                return Reliable(Convert.ToBoolean(reliableValue));
            }
        }

        /// <summary>
        /// Get the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
        /// </summary>
        /// <returns> the subscription semantics for if loss is acceptable, or not, for a reliable message delivery. </returns>
        /// <seealso cref="Aeron.Context.RELIABLE_STREAM_PARAM_NAME"/>
        public bool? Reliable()
        {
            return _reliable;
        }

        /// <summary>
        /// Set the Time To Live (TTL) for a multicast datagram. Valid values are 0-255 for the number of hops the datagram
        /// can progress along.
        /// </summary>
        /// <param name="ttl"> value for a multicast datagram. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TTL_PARAM_NAME"/>
        public ChannelUriStringBuilder Ttl(int? ttl)
        {
            if (null != ttl && (ttl < 0 || ttl > 255))
            {
                throw new ArgumentException("TTL not in range 0-255: " + ttl);
            }

            _ttl = ttl;
            return this;
        }

        /// <summary>
        /// Set the ttl value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TTL_PARAM_NAME"/>
        public ChannelUriStringBuilder Ttl(ChannelUri channelUri)
        {
            string ttlValue = channelUri.Get(Aeron.Context.TTL_PARAM_NAME);
            if (null == ttlValue)
            {
                _ttl = null;
                return this;
            }
            else
            {
                try
                {
                    return Ttl(Convert.ToInt32(ttlValue));
                }
                catch (FormatException ex)
                {
                    throw new ArgumentException("'ttl' must be a value integer", ex);
                }
            }
        }

        /// <summary>
        /// Get the Time To Live (TTL) for a multicast datagram.
        /// </summary>
        /// <returns> the Time To Live (TTL) for a multicast datagram. </returns>
        /// <seealso cref="Aeron.Context.TTL_PARAM_NAME"/>
        public int? Ttl()
        {
            return _ttl;
        }

        /// <summary>
        /// Set the maximum transmission unit (MTU) including Aeron header for a datagram payload. If this is greater
        /// than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
        /// </summary>
        /// <param name="mtu"> the maximum transmission unit including Aeron header for a datagram payload. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.MTU_LENGTH_PARAM_NAME"/>
        public ChannelUriStringBuilder Mtu(int? mtu)
        {
            if (null != mtu)
            {
                if (mtu < 32 || mtu > 65504)
                {
                    throw new ArgumentException("MTU not in range 32-65504: " + mtu);
                }

                if ((mtu & (FRAME_ALIGNMENT - 1)) != 0)
                {
                    throw new ArgumentException("MTU not a multiple of FRAME_ALIGNMENT: mtu=" + mtu);
                }
            }

            _mtu = mtu;
            return this;
        }

        /// <summary>
        /// Set the mtu value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.MTU_LENGTH_PARAM_NAME"/>
        public ChannelUriStringBuilder Mtu(ChannelUri channelUri)
        {
            string mtuValue = channelUri.Get(Aeron.Context.MTU_LENGTH_PARAM_NAME);
            if (null == mtuValue)
            {
                _mtu = null;
                return this;
            }
            else
            {
                long value = SystemUtil.ParseSize(Aeron.Context.MTU_LENGTH_PARAM_NAME, mtuValue);
                if (value > int.MaxValue)
                {
                    throw new InvalidOperationException(Aeron.Context.MTU_LENGTH_PARAM_NAME + " " + value + " > " +
                                                        int.MaxValue);
                }

                return Mtu((int)value);
            }
        }

        /// <summary>
        /// Get the maximum transmission unit (MTU) including Aeron header for a datagram payload. If this is greater
        /// than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
        /// </summary>
        /// <returns> the maximum transmission unit (MTU) including Aeron header for a datagram payload. </returns>
        /// <seealso cref="Aeron.Context.MTU_LENGTH_PARAM_NAME"/>
        public int? Mtu()
        {
            return _mtu;
        }

        /// <summary>
        /// Set the length of buffer used for each term of the log. Valid values are powers of 2 in the 64K - 1G range.
        /// </summary>
        /// <param name="termLength"> of the buffer used for each term of the log. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TERM_LENGTH_PARAM_NAME"/>
        public ChannelUriStringBuilder TermLength(int? termLength)
        {
            if (null != termLength)
            {
                LogBufferDescriptor.CheckTermLength(termLength.Value);
            }

            _termLength = termLength;
            return this;
        }

        /// <summary>
        /// Set the termLength value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TERM_LENGTH_PARAM_NAME"/>
        public ChannelUriStringBuilder TermLength(ChannelUri channelUri)
        {
            string termLengthValue = channelUri.Get(Aeron.Context.TERM_LENGTH_PARAM_NAME);
            if (null == termLengthValue)
            {
                _termLength = null;
                return this;
            }
            else
            {
                long value = SystemUtil.ParseSize(Aeron.Context.TERM_LENGTH_PARAM_NAME, termLengthValue);
                if (value > int.MaxValue)
                {
                    throw new InvalidOperationException("term length more than max length of " +
                                                        LogBufferDescriptor.TERM_MAX_LENGTH + ": length=" +
                                                        _termLength);
                }

                return TermLength((int)value);
            }
        }

        /// <summary>
        /// Get the length of buffer used for each term of the log.
        /// </summary>
        /// <returns> the length of buffer used for each term of the log. </returns>
        /// <seealso cref="Aeron.Context.TERM_LENGTH_PARAM_NAME"/>
        public int? TermLength()
        {
            return _termLength;
        }

        /// <summary>
        /// Set the initial term id at which a publication will start.
        /// </summary>
        /// <param name="initialTermId"> the initial term id at which a publication will start. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.INITIAL_TERM_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder InitialTermId(int? initialTermId)
        {
            _initialTermId = initialTermId;
            return this;
        }

        /// <summary>
        /// Set the initialTermId value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.INITIAL_TERM_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder InitialTermId(ChannelUri channelUri)
        {
            string initialTermIdValue = channelUri.Get(Aeron.Context.INITIAL_TERM_ID_PARAM_NAME);
            if (null == initialTermIdValue)
            {
                _initialTermId = null;
                return this;
            }
            else
            {
                try
                {
                    return InitialTermId(Convert.ToInt32(initialTermIdValue));
                }
                catch (FormatException ex)
                {
                    throw new ArgumentException("'initial-term-id' must be a valid integer", ex);
                }
            }
        }

        /// <summary>
        /// the initial term id at which a publication will start.
        /// </summary>
        /// <returns> the initial term id at which a publication will start. </returns>
        /// <seealso cref="Aeron.Context.INITIAL_TERM_ID_PARAM_NAME"/>
        public int? InitialTermId()
        {
            return _initialTermId;
        }

        /// <summary>
        /// Set the current term id at which a publication will start. This when combined with the initial term can
        /// establish a starting position.
        /// </summary>
        /// <param name="termId"> at which a publication will start. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TERM_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder TermId(int? termId)
        {
            _termId = termId;
            return this;
        }

        /// <summary>
        /// Set the termId value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TERM_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder TermId(ChannelUri channelUri)
        {
            string termIdValue = channelUri.Get(Aeron.Context.TERM_ID_PARAM_NAME);
            if (null == termIdValue)
            {
                _termId = null;
                return this;
            }
            else
            {
                try
                {
                    return TermId(Convert.ToInt32(termIdValue));
                }
                catch (FormatException ex)
                {
                    throw new ArgumentException("'term-id' must be a valid integer", ex);
                }
            }
        }

        /// <summary>
        /// Get the current term id at which a publication will start.
        /// </summary>
        /// <returns> the current term id at which a publication will start. </returns>
        /// <seealso cref="Aeron.Context.TERM_ID_PARAM_NAME"/>
        public int? TermId()
        {
            return _termId;
        }

        /// <summary>
        /// Set the offset within a term at which a publication will start. This when combined with the term id can establish
        /// a starting position.
        /// </summary>
        /// <param name="termOffset"> within a term at which a publication will start. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TERM_OFFSET_PARAM_NAME"/>
        public ChannelUriStringBuilder TermOffset(int? termOffset)
        {
            if (null != termOffset)
            {
                if ((termOffset < 0 || termOffset > LogBufferDescriptor.TERM_MAX_LENGTH))
                {
                    throw new ArgumentException("term offset not in range 0-1g: " + termOffset);
                }

                if (0 != (termOffset & (FRAME_ALIGNMENT - 1)))
                {
                    throw new ArgumentException("term offset not multiple of FRAME_ALIGNMENT: " + termOffset);
                }
            }

            _termOffset = termOffset;
            return this;
        }

        /// <summary>
        /// Set the termOffset value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TERM_OFFSET_PARAM_NAME"/>
        public ChannelUriStringBuilder TermOffset(ChannelUri channelUri)
        {
            string termOffsetValue = channelUri.Get(Aeron.Context.TERM_OFFSET_PARAM_NAME);
            if (null == termOffsetValue)
            {
                _termOffset = null;
                return this;
            }
            else
            {
                try
                {
                    return TermOffset(Convert.ToInt32(termOffsetValue));
                }
                catch (FormatException ex)
                {
                    throw new ArgumentException("'term-offset' must be a valid integer", ex);
                }
            }
        }

        /// <summary>
        /// Get the offset within a term at which a publication will start.
        /// </summary>
        /// <returns> the offset within a term at which a publication will start. </returns>
        /// <seealso cref="Aeron.Context.TERM_OFFSET_PARAM_NAME"/>
        public int? TermOffset()
        {
            return _termOffset;
        }


        /// <summary>
        /// Set the session id for a publication or restricted subscription.
        /// </summary>
        /// <param name="sessionId"> for the publication or a restricted subscription. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder SessionId(int? sessionId)
        {
            _sessionId = sessionId;
            return this;
        }

        /// <summary>
        /// Set the session id for a publication or restricted subscription from a formatted string.  Supports a format of
        /// either a string encoded signed 32-bit number or 'tag:' followed by a signed 64 bit value.
        /// </summary>
        /// <param name="sessionIdStr"> for the publication or a restricted subscription. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>seealso>
        public ChannelUriStringBuilder SessionId(string sessionIdStr)
        {
            if (null != sessionIdStr)
            {
                if (ChannelUri.IsTagged(sessionIdStr))
                {
                    TaggedSessionId(ChannelUri.GetTag(sessionIdStr));
                }
                else
                {
                    IsSessionIdTagged(false);
                    try
                    {
                        SessionId(Convert.ToInt32(sessionIdStr));
                    }
                    catch (FormatException ex)
                    {
                        throw new ArgumentException("'session-id' must be a valid integer", ex);
                    }
                }
            }
            else
            {
                SessionId((int?)null);
            }

            return this;
        }

        /// <summary>
        /// Set the session id for a publication or restricted subscription as a tag referenced value.
        /// </summary>
        /// <param name="sessionId"> for the publication or a restricted subscription. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder TaggedSessionId(long? sessionId)
        {
            IsSessionIdTagged(true);
            _sessionId = sessionId;
            return this;
        }

        /// <summary>
        /// Set the sessionId value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder SessionId(ChannelUri channelUri)
        {
            return SessionId(channelUri.Get(Aeron.Context.SESSION_ID_PARAM_NAME));
        }

        /// <summary>
        /// Get the session id for a publication or restricted subscription.
        /// </summary>
        /// <returns> the session id for a publication or restricted subscription. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        /// @deprecated this method will not correctly handle tagged sessionId values that are outside the range of
        /// a signed 32-bit number.  If this is called and a tagged value outside this range is currently held in this
        /// object, then the result will be the the least significant bits. 
        [Obsolete("this method will not correctly handle tagged sessionId values that are outside the range of")]
        public int? SessionId()
        {
            return unchecked((int?)_sessionId);
        }

        /// <summary>
        /// Set the time a network publication will linger in nanoseconds after being drained. This time is so that tail
        /// loss can be recovered.
        /// </summary>
        /// <param name="lingerNs"> time for the publication after it is drained. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.LINGER_PARAM_NAME"></seealso>
        public ChannelUriStringBuilder Linger(long? lingerNs)
        {
            if (null != lingerNs && lingerNs < 0)
            {
                throw new ArgumentException("linger value cannot be negative: " + lingerNs);
            }

            _linger = lingerNs;
            return this;
        }

        /// <summary>
        /// Set the linger value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.LINGER_PARAM_NAME"/>
        public ChannelUriStringBuilder Linger(ChannelUri channelUri)
        {
            string lingerValue = channelUri.Get(Aeron.Context.LINGER_PARAM_NAME);
            if (null == lingerValue)
            {
                _linger = null;
                return this;
            }
            else
            {
                return Linger(SystemUtil.ParseDuration(Aeron.Context.LINGER_PARAM_NAME, lingerValue));
            }
        }

        /// <summary>
        /// Get the time a network publication will linger in nanoseconds after being drained. This time is so that tail
        /// loss can be recovered.
        /// </summary>
        /// <returns> the linger time in nanoseconds a publication will linger after being drained. </returns>
        /// <seealso cref="Aeron.Context.LINGER_PARAM_NAME"></seealso>
        public long? Linger()
        {
            return _linger;
        }

        /// <summary>
        /// Set to indicate if a term log buffer should be sparse on disk or not. Sparse saves space at the potential
        /// expense of latency.
        /// </summary>
        /// <param name="isSparse"> true if the term buffer log is sparse on disk. </param>
        /// <returns> this for a fluent API. </returns>
        /// <see cref="Aeron.Context.SPARSE_PARAM_NAME"/>
        public ChannelUriStringBuilder Sparse(bool? isSparse)
        {
            _sparse = isSparse;
            return this;
        }

        /// <summary>
        /// Set the sparse value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SPARSE_PARAM_NAME"/>
        public ChannelUriStringBuilder Sparse(ChannelUri channelUri)
        {
            string sparseValue = channelUri.Get(Aeron.Context.SPARSE_PARAM_NAME);
            if (null == sparseValue)
            {
                _sparse = null;
                return this;
            }
            else
            {
                return Sparse(Convert.ToBoolean(sparseValue));
            }
        }

        /// <summary>
        /// Should term log buffer be sparse on disk or not. Sparse saves space at the potential expense of latency.
        /// </summary>
        /// <returns> true if the term buffer log is sparse on disk. </returns>
        /// <see cref="Aeron.Context.SPARSE_PARAM_NAME"/>
        public bool? Sparse()
        {
            return _sparse;
        }

        /// <summary>
        /// Set to indicate if an EOS should be sent on the media or not.
        /// </summary>
        /// <param name="eos"> true if the EOS should be sent. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.EOS_PARAM_NAME"/>
        public ChannelUriStringBuilder Eos(bool? eos)
        {
            _eos = eos;
            return this;
        }

        /// <summary>
        /// Set the eos value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.EOS_PARAM_NAME"/>
        public ChannelUriStringBuilder Eos(ChannelUri channelUri)
        {
            string eosValue = channelUri.Get(Aeron.Context.EOS_PARAM_NAME);
            if (null == eosValue)
            {
                _eos = null;
                return this;
            }
            else
            {
                return Eos(Convert.ToBoolean(eosValue));
            }
        }

        /// <summary>
        /// Should an EOS flag be sent on the media or not.
        /// </summary>
        /// <returns> true if the EOS param should be set. </returns>
        /// <seealso cref="Aeron.Context.EOS_PARAM_NAME"/>
        public bool? Eos()
        {
            return _eos;
        }

        /// <summary>
        /// Should the subscription channel be tethered or not for local flow control.
        /// </summary>
        /// <param name="tether"> value to be set for the tether param. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TETHER_PARAM_NAME"/>
        public ChannelUriStringBuilder Tether(bool? tether)
        {
            _tether = tether;
            return this;
        }

        /// <summary>
        /// Set the tether value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TETHER_PARAM_NAME"/>
        public ChannelUriStringBuilder Tether(ChannelUri channelUri)
        {
            string tetherValue = channelUri.Get(Aeron.Context.TETHER_PARAM_NAME);
            if (null == tetherValue)
            {
                _tether = null;
                return this;
            }
            else
            {
                return Tether(Convert.ToBoolean(tetherValue));
            }
        }

        /// <summary>
        /// Should the subscription channel be tethered or not for local flow control.
        /// </summary>
        /// <returns> value of the tether param. </returns>
        /// <seealso cref="Aeron.Context.TETHER_PARAM_NAME"/>
        public bool? Tether()
        {
            return _tether;
        }

        /// <summary>
        /// Is the receiver likely to be part of a group. This informs behaviour such as loss handling.
        /// </summary>
        /// <param name="group"> value to be set for the group param. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.GROUP_PARAM_NAME"/>
        /// <seealso cref="ControlMode()"/>
        /// <seealso cref="ControlEndpoint()"/>
        public ChannelUriStringBuilder Group(bool? group)
        {
            this._group = group;
            return this;
        }

        /// <summary>
        /// Set the group value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.GROUP_PARAM_NAME"/>
        public ChannelUriStringBuilder Group(ChannelUri channelUri)
        {
            string groupValue = channelUri.Get(Aeron.Context.GROUP_PARAM_NAME);
            if (null == groupValue)
            {
                _group = null;
                return this;
            }
            else
            {
                return Group(Convert.ToBoolean(groupValue));
            }
        }

        /// <summary>
        /// Is the receiver likely to be part of a group. This informs behaviour such as loss handling.
        /// </summary>
        /// <returns> value of the group param. </returns>
        /// <seealso cref="Aeron.Context.GROUP_PARAM_NAME"/>
        /// <seealso cref="ControlMode()"/>
        /// <seealso cref="ControlEndpoint()"/>
        public bool? Group()
        {
            return _group;
        }

        /// <summary>
        /// Set the tags for a channel used by a publication or subscription. Tags can be used to identify or tag a
        /// channel so that a configuration can be referenced and reused.
        /// </summary>
        /// <param name="tags"> for the channel, publication or subscription. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.TAG_PREFIX"/>
        public ChannelUriStringBuilder Tags(string tags)
        {
            _tags = tags;
            return this;
        }

        /// <summary>
        /// Set the tags to be value which is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"/>
        public ChannelUriStringBuilder Tags(ChannelUri channelUri)
        {
            return Tags(channelUri.Get(Aeron.Context.TAGS_PARAM_NAME));
        }

        /// <summary>
        /// Set the tags to the specified channel and publication/subscription tag <seealso cref="ChannelUri"/>. The
        /// publication/subscription may be null. If channel tag is null, then the pubSubTag must be null.
        /// </summary>
        /// <param name="channelTag"> optional value for the channel tag. </param>
        /// <param name="pubSubTag">  option value for the publication/subscription tag. </param>
        /// <returns> this for a fluent API. </returns>
        /// <exception cref="ArgumentException"> if channelTag is null and pubSubTag is not. </exception>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"></seealso>
        public ChannelUriStringBuilder Tags(long? channelTag, long? pubSubTag)
        {
            if (null == channelTag && null != pubSubTag)
            {
                throw new ArgumentException("null == channelTag && null != pubSubTag");
            }

            if (null == channelTag)
            {
                return Tags((string)null);
            }

            return Tags(channelTag + (null != pubSubTag ? "," + pubSubTag : ""));
        }

        /// <summary>
        /// Get the tags for a channel used by a publication or subscription. Tags can be used to identify or tag a
        /// channel so that a configuration can be referenced and reused.
        /// </summary>
        /// <returns> the tags for a channel, publication or subscription. </returns>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.TAG_PREFIX"/>
        public string Tags()
        {
            return _tags;
        }

        /// <summary>
        /// Toggle the value for <seealso cref="SessionId()"/> being tagged or not.
        /// </summary>
        /// <param name="isSessionIdTagged"> for session id </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.TAG_PREFIX"/>
        public ChannelUriStringBuilder IsSessionIdTagged(bool isSessionIdTagged)
        {
            _isSessionIdTagged = isSessionIdTagged;
            return this;
        }

        /// <summary>
        /// Is the value for <seealso cref="SessionId()"/> a tag..
        /// </summary>
        /// <returns> whether the value for <seealso cref="SessionId()"/> a tag reference or not. </returns>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.TAG_PREFIX"/>
        public bool IsSessionIdTagged()
        {
            return _isSessionIdTagged;
        }

        /// <summary>
        /// Set the alias for a URI. Aliases are not interpreted by Aeron and are to be used by the application.
        /// </summary>
        /// <param name="alias"> for the URI. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.ALIAS_PARAM_NAME"/>
        public ChannelUriStringBuilder Alias(string alias)
        {
            _alias = alias;
            return this;
        }

        /// <summary>
        /// Set the alias to be value which is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.ALIAS_PARAM_NAME"/>
        public ChannelUriStringBuilder Alias(ChannelUri channelUri)
        {
            return Alias(channelUri.Get(Aeron.Context.ALIAS_PARAM_NAME));
        }

        /// <summary>
        /// Get the alias present in the URI.
        /// </summary>
        /// <returns> alias for the URI. </returns>
        /// <seealso cref="Aeron.Context.ALIAS_PARAM_NAME"/>
        public string Alias()
        {
            return _alias;
        }

        /// <summary>
        /// Set the congestion control algorithm to be used on a stream.
        /// </summary>
        /// <param name="congestionControl"> for the URI. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.CONGESTION_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder CongestionControl(string congestionControl)
        {
            this._cc = congestionControl;
            return this;
        }

        /// <summary>
        /// Set the congestion control to be value which is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.CONGESTION_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder CongestionControl(ChannelUri channelUri)
        {
            return CongestionControl(channelUri.Get(Aeron.Context.CONGESTION_CONTROL_PARAM_NAME));
        }

        /// <summary>
        /// Get the congestion control algorithm to be used on a stream.
        /// </summary>
        /// <returns> congestion control strategy for the channel. </returns>
        /// <seealso cref="Aeron.Context.CONGESTION_CONTROL_PARAM_NAME"/>
        public string CongestionControl()
        {
            return _cc;
        }

        /// <summary>
        /// Set the flow control strategy to be used on a stream.
        /// </summary>
        /// <param name="flowControl"> for the URI. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.FLOW_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder FlowControl(string flowControl)
        {
            this._fc = flowControl;
            return this;
        }

        /// <summary>
        /// Set tagged flow control settings to be used on a stream. All specified values may be null and the default
        /// specified in the MediaDriver.Context will be used instead.
        /// </summary>
        /// <param name="groupTag">  receiver tag for this stream. </param>
        /// <param name="minGroupSize"> group size required to allow publications for this channel to be moved to connected status. </param>
        /// <param name="timeout">      timeout receivers, default is ns, but allows suffixing of time units (e.g. 5s). </param>
        /// <returns> this for fluent API. </returns>
        public ChannelUriStringBuilder TaggedFlowControl(long? groupTag, int? minGroupSize, string timeout)
        {
            string flowControlValue = "tagged";

            if (null != groupTag || null != minGroupSize)
            {
                flowControlValue += ",g:";

                if (null != groupTag)
                {
                    flowControlValue += groupTag;
                }

                if (null != minGroupSize)
                {
                    flowControlValue += ("/" + minGroupSize);
                }
            }

            if (null != timeout)
            {
                flowControlValue += (",t:" + timeout);
            }

            return FlowControl(flowControlValue);
        }

        /// <summary>
        /// Set min flow control settings to be used on a stream. All specified values may be null and the default
        /// specified in the MediaDriver.Context will be used instead.
        /// </summary>
        /// <param name="minGroupSize"> group size required to allow publications for this stream to be moved to connected status. </param>
        /// <param name="timeout">      timeout receivers, default is ns, but allows suffixing of time units (e.g. 5s). </param>
        /// <returns> this for fluent API. </returns>
        public ChannelUriStringBuilder MinFlowControl(int? minGroupSize, string timeout)
        {
            string flowControlValue = "min";

            if (null != minGroupSize)
            {
                flowControlValue += (",g:/" + minGroupSize);
            }

            if (null != timeout)
            {
                flowControlValue += (",t:" + timeout);
            }

            return FlowControl(flowControlValue);
        }

        /// <summary>
        /// Set the flow control to be value which is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.FLOW_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder FlowControl(ChannelUri channelUri)
        {
            return FlowControl(channelUri.Get(Aeron.Context.FLOW_CONTROL_PARAM_NAME));
        }

        /// <summary>
        /// Get the flow control strategy to be used on a stream.
        /// </summary>
        /// <returns> flow control strategy for the stream. </returns>
        /// <seealso cref="Aeron.Context.FLOW_CONTROL_PARAM_NAME"/>
        public string FlowControl()
        {
            return _fc;
        }

        /// <summary>
        /// Set the group tag (gtag) to be sent in SMs (Status Messages).
        /// </summary>
        /// <param name="groupTag"> to be sent in SMs </param>
        /// <returns> this for fluent API. </returns>
        /// <seealso cref="Aeron.Context.GROUP_TAG_PARAM_NAME"/>
        public ChannelUriStringBuilder GroupTag(long? groupTag)
        {
            this._groupTag = groupTag;
            return this;
        }

        /// <summary>
        /// Set the group tag (gtag) to be the value which is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.GROUP_TAG_PARAM_NAME"/>
        public ChannelUriStringBuilder GroupTag(ChannelUri channelUri)
        {
            string groupTagValue = channelUri.Get(Aeron.Context.GROUP_TAG_PARAM_NAME);
            if (null == groupTagValue)
            {
                _groupTag = null;
                return this;
            }
            else
            {
                try
                {
                    return GroupTag(Convert.ToInt64(groupTagValue));
                }
                catch (FormatException ex)
                {
                    throw new ArgumentException("'gtag# must be a valid long value", ex);
                }
            }
        }

        /// <summary>
        /// Get the group tag (gtag) to be sent in SMs (Status Messages).
        /// </summary>
        /// <returns> receiver tag to be sent in SMs. </returns>
        /// <seealso cref="Aeron.Context.GROUP_TAG_PARAM_NAME"/>
        public long? GroupTag()
        {
            return _groupTag;
        }

        /// <summary>
        /// Set the subscription semantics for if a stream should be rejoined after going unavailable.
        /// </summary>
        /// <param name="rejoin"> false if stream is not to be rejoined. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.REJOIN_PARAM_NAME"/>
        public ChannelUriStringBuilder Rejoin(bool? rejoin)
        {
            this._rejoin = rejoin;
            return this;
        }

        /// <summary>
        /// Set the rejoin value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.REJOIN_PARAM_NAME"/>
        public ChannelUriStringBuilder Rejoin(ChannelUri channelUri)
        {
            string rejoinValue = channelUri.Get(Aeron.Context.REJOIN_PARAM_NAME);
            if (null == rejoinValue)
            {
                _rejoin = null;
                return this;
            }
            else
            {
                return Rejoin(Convert.ToBoolean(rejoinValue));
            }
        }

        /// <summary>
        /// Get the subscription semantics for if a stream should be rejoined after going unavailable.
        /// </summary>
        /// <returns> the subscription semantics for if a stream should be rejoined after going unavailable. </returns>
        /// <seealso cref="Aeron.Context.REJOIN_PARAM_NAME"/>
        public bool? Rejoin()
        {
            return _rejoin;
        }

        /// <summary>
        /// Set the publication semantics for whether the presence of spy subscriptions simulate a connection.
        /// </summary>
        /// <param name="spiesSimulateConnection"> true if the presence of spy subscriptions simulate a connection. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SPIES_SIMULATE_CONNECTION_PARAM_NAME"></seealso>
        public ChannelUriStringBuilder SpiesSimulateConnection(bool? spiesSimulateConnection)
        {
            this._ssc = spiesSimulateConnection;
            return this;
        }

        /// <summary>
        /// Set the publication semantics for whether the presence of spy subscriptions simulate a connection to be what is in
        /// the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SPIES_SIMULATE_CONNECTION_PARAM_NAME"></seealso>
        public ChannelUriStringBuilder SpiesSimulateConnection(ChannelUri channelUri)
        {
            string sscValue = channelUri.Get(Aeron.Context.SPIES_SIMULATE_CONNECTION_PARAM_NAME);
            if (null == sscValue)
            {
                _ssc = null;
                return this;
            }
            else
            {
                return SpiesSimulateConnection(Convert.ToBoolean(sscValue));
            }
        }

        /// <summary>
        /// Get the publication semantics for whether the presence of spy subscriptions simulate a connection.
        /// </summary>
        /// <returns> true if the presence of spy subscriptions simulate a connection, otherwise false. </returns>
        /// <seealso cref="Aeron.Context.SPIES_SIMULATE_CONNECTION_PARAM_NAME"></seealso>
        public bool? SpiesSimulateConnection()
        {
            return _ssc;
        }

        /// <summary>
        /// Initialise a channel for restarting a publication at a given position.
        /// </summary>
        /// <param name="position">      at which the publication should be started. </param>
        /// <param name="initialTermId"> what which the stream would start. </param>
        /// <param name="termLength">    for the stream. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder InitialPosition(long position, int initialTermId, int termLength)
        {
            if (position < 0)
            {
                throw new ArgumentException("invalid position=" + position + " < 0");
            }
            if (0 != (position & (FRAME_ALIGNMENT - 1)))
            {
                throw new ArgumentException("invalid position=" + position + " does not have frame alignment=" + FRAME_ALIGNMENT);
            }

            int bitsToShift = LogBufferDescriptor.PositionBitsToShift(termLength);

            _initialTermId = initialTermId;
            _termId = LogBufferDescriptor.ComputeTermIdFromPosition(position, bitsToShift, initialTermId);
            _termOffset = (int)(position & (termLength - 1));
            _termLength = termLength;

            return this;
        }

        /// <summary>
        /// Set the underlying OS send buffer length.
        /// </summary>
        /// <param name="socketSndbufLength"> parameter to be passed as SO_SNDBUF value. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_SNDBUF_PARAM_NAME"/>
        public ChannelUriStringBuilder SocketSndbufLength(int? socketSndbufLength)
        {
            _socketSndbufLength = socketSndbufLength;
            return this;
        }

        /// <summary>
        /// Set the underlying OS send buffer length from an existing <seealso cref="ChannelUri"/> which may be (null).
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_SNDBUF_PARAM_NAME"/>
        public ChannelUriStringBuilder SocketSndbufLength(ChannelUri channelUri)
        {
            string valueStr = channelUri.Get(Aeron.Context.SOCKET_SNDBUF_PARAM_NAME);
            if (null == valueStr)
            {
                _socketSndbufLength = null;
                return this;
            }
            else
            {
                long value = SystemUtil.ParseSize(Aeron.Context.SOCKET_SNDBUF_PARAM_NAME, valueStr);
                if (value > int.MaxValue)
                {
                    throw new InvalidOperationException("value exceeds maximum permitted: value=" + value);
                }

                return SocketSndbufLength((int)value);
            }
        }

        /// <summary>
        /// Get the underling OS send buffer length setting
        /// </summary>
        /// <returns> underlying OS send buffer length setting or null if not specified. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_SNDBUF_PARAM_NAME"/>
        public int? SocketSndbufLength()
        {
            return _socketSndbufLength;
        }

        /// <summary>
        /// Set the underlying OS receive buffer length.
        /// </summary>
        /// <param name="socketRcvbufLength"> parameter to be passed as SO_SNDBUF value. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_SNDBUF_PARAM_NAME"/>
        public ChannelUriStringBuilder SocketRcvbufLength(int? socketRcvbufLength)
        {
            _socketRcvbufLength = socketRcvbufLength;
            return this;
        }

        /// <summary>
        /// Set the underlying OS receive buffer length from an existing <seealso cref="ChannelUri"/>, which may have a null value for
        /// this field.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_RCVBUF_PARAM_NAME"/>
        public ChannelUriStringBuilder SocketRcvbufLength(ChannelUri channelUri)
        {
            string valueStr = channelUri.Get(Aeron.Context.SOCKET_RCVBUF_PARAM_NAME);
            if (null == valueStr)
            {
                this._socketRcvbufLength = null;
                return this;
            }
            else
            {
                long value = SystemUtil.ParseSize(Aeron.Context.SOCKET_RCVBUF_PARAM_NAME, valueStr);
                if (value > int.MaxValue)
                {
                    throw new InvalidOperationException("value exceeds maximum permitted: value=" + value);
                }

                return SocketRcvbufLength((int)value);
            }
        }

        /// <summary>
        /// Get the underling OS receive buffer length setting.
        /// </summary>
        /// <returns> underlying OS receive buffer length setting or null if not specified. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_RCVBUF_PARAM_NAME"/>
        public int? SocketRcvbufLength()
        {
            return _socketRcvbufLength;
        }

        /// <summary>
        /// Set the flow control initial receiver window length for this channel.
        /// </summary>
        /// <param name="receiverWindowLength"> initial receiver window length. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.RECEIVER_WINDOW_LENGTH_PARAM_NAME"/>
        public ChannelUriStringBuilder ReceiverWindowLength(int? receiverWindowLength)
        {
            _receiverWindowLength = receiverWindowLength;
            return this;
        }

        /// <summary>
        /// Set the flow control initial receiver window length for this channel from an existing <seealso cref="ChannelUri"/>,
        /// which may have a null value for this field.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.RECEIVER_WINDOW_LENGTH_PARAM_NAME"/>
        public ChannelUriStringBuilder ReceiverWindowLength(ChannelUri channelUri)
        {
            string valueStr = channelUri.Get(Aeron.Context.RECEIVER_WINDOW_LENGTH_PARAM_NAME);
            if (null == valueStr)
            {
                this._receiverWindowLength = null;
                return this;
            }
            else
            {
                long value = SystemUtil.ParseSize(Aeron.Context.RECEIVER_WINDOW_LENGTH_PARAM_NAME, valueStr);
                if (value > int.MaxValue)
                {
                    throw new InvalidOperationException("value exceeds maximum permitted: value=" + value);
                }

                return ReceiverWindowLength((int)value);
            }
        }

        /// <summary>
        /// Get the receiver window length to be used as the initial receiver window for flow control.
        /// </summary>
        /// <returns> receiver window length. </returns>
        /// <seealso cref="Aeron.Context.SOCKET_RCVBUF_PARAM_NAME"/>
        public int? ReceiverWindowLength()
        {
            return _receiverWindowLength;
        }

        /// <summary>
        /// Offset into a message to store the media receive timestamp. May also be the special value 'reserved' which means
        /// to store the timestamp in the reserved value field.
        /// </summary>
        /// <returns> current mediaReceiveTimestampOffset value either as string representation of an integer index or the
        /// special value 'reserved' </returns>
        public string MediaReceiveTimestampOffset()
        {
            return _mediaReceiveTimestampOffset;
        }

        /// <summary>
        /// Offset into a message to store the media receive timestamp. May also be the special value 'reserved' which means
        /// to store the timestamp in the reserved value field.
        /// </summary>
        /// <param name="timestampOffset"> to use as the offset. </param>
        /// <returns> this for a fluent API. </returns>
        /// <exception cref="ArgumentException"> if the string is not null and doesn't represent an int or the 'reserved' value. </exception>
        public ChannelUriStringBuilder MediaReceiveTimestampOffset(string timestampOffset)
        {
            if (null != timestampOffset && !Aeron.Context.RESERVED_OFFSET.Equals(timestampOffset))
            {
                try
                {
                    int.Parse(timestampOffset);
                }
                catch (FormatException)
                {
                    throw new ArgumentException("mediaReceiveTimestampOffset must be a number or the value '" +
                                                       Aeron.Context.RESERVED_OFFSET + "'");
                }
            }

            this._mediaReceiveTimestampOffset = timestampOffset;
            return this;
        }

        /// <summary>
        /// Offset into a message to store the media receive timestamp. May also be the special value 'reserved' which means
        /// to store the timestamp in the reserved value field.
        /// </summary>
        /// <param name="channelUri"> the existing URI to extract the mediaReceiveTimestampOffset from </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder MediaReceiveTimestampOffset(ChannelUri channelUri)
        {
            return MediaReceiveTimestampOffset(channelUri.Get(Aeron.Context.MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME));
        }

        /// <summary>
        /// Offset into a message to store the channel receive timestamp. May also be the special value 'reserved' which
        /// means to store the timestamp in the reserved value field.
        /// </summary>
        /// <returns> current channelReceiveTimestampOffset value either as string representation of an integer index or
        /// the special value 'reserved' </returns>
        public string ChannelReceiveTimestampOffset()
        {
            return _channelReceiveTimestampOffset;
        }

        /// <summary>
        /// Offset into a message to store the channel receive timestamp. May also be the special value 'reserved' which
        /// means to store the timestamp in the reserved value field.
        /// </summary>
        /// <param name="timestampOffset"> to use as the offset. </param>
        /// <returns> this for a fluent API. </returns>
        /// <exception cref="ArgumentException"> if the string doesn't represent an int or the 'reserved' value. </exception>
        public ChannelUriStringBuilder ChannelReceiveTimestampOffset(string timestampOffset)
        {
            if (null != timestampOffset && !Aeron.Context.RESERVED_OFFSET.Equals(timestampOffset))
            {
                try
                {
                    int.Parse(timestampOffset);
                }
                catch (FormatException)
                {
                    throw new ArgumentException("channelReceiveTimestampOffset must be a number or the value '" +
                                                       Aeron.Context.RESERVED_OFFSET + "'");
                }
            }

            this._channelReceiveTimestampOffset = timestampOffset;
            return this;
        }

        /// <summary>
        /// Offset into a message to store the channel receive timestamp. May also be the special value 'reserved' which
        /// means to store the timestamp in the reserved value field.
        /// </summary>
        /// <param name="channelUri"> the existing URI to extract the receiveTimestampOffset from. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder ChannelReceiveTimestampOffset(ChannelUri channelUri)
        {
            return ChannelReceiveTimestampOffset(channelUri.Get(Aeron.Context.CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME));
        }

        /// <summary>
        /// Offset into a message to store the channel send timestamp. May also be the special value 'reserved' which means
        /// to store the timestamp in the reserved value field.
        /// </summary>
        /// <returns> current sendTimestampOffset value either as string representation of an integer index or the special
        /// value 'reserved'. </returns>
        public string ChannelSendTimestampOffset()
        {
            return _channelSendTimestampOffset;
        }

        /// <summary>
        /// Offset into a message to store the channel send timestamp. May also be the special value 'reserved' which means
        /// to store the timestamp in the reserved value field.
        /// </summary>
        /// <param name="timestampOffset"> to use as the offset. </param>
        /// <exception cref="ArgumentException"></exception>
        /// <returns> this for a fluent API. </returns>
        /// <exception cref="ArgumentException"> if the string is not null doesn't represent an int or the 'reserved' value. </exception>
        public ChannelUriStringBuilder ChannelSendTimestampOffset(string timestampOffset)
        {
            if (null != timestampOffset && !Aeron.Context.RESERVED_OFFSET.Equals(timestampOffset))
            {
                try
                {
                    int.Parse(timestampOffset);
                }
                catch (FormatException)
                {
                    throw new ArgumentException("channelSendTimestampOffset must be a number or the value '" +
                                                Aeron.Context.RESERVED_OFFSET + "' found: " + timestampOffset);
                }
            }

            _channelSendTimestampOffset = timestampOffset;
            return this;
        }

        /// <summary>
        /// Offset into a message to store the channel send timestamp. May also be the special value 'reserved' which means
        /// to store the timestamp in the reserved value field.
        /// </summary>
        /// <param name="channelUri"> the existing URI to extract the channelSendTimestampOffset from. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder ChannelSendTimestampOffset(ChannelUri channelUri)
        {
            return ChannelSendTimestampOffset(channelUri.Get(Aeron.Context.CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME));
        }

        /// <summary>
        /// Build a channel URI String for the given parameters.
        /// </summary>
        /// <returns> a channel URI String for the given parameters. </returns>
        public string Build()
        {
            _sb.Length = 0;


            if (!string.IsNullOrEmpty(_prefix))
            {
                _sb.Append(_prefix).Append(':');
            }

            _sb.Append(ChannelUri.AERON_SCHEME).Append(':').Append(_media).Append('?');

            AppendParameter(_sb, Aeron.Context.TAGS_PARAM_NAME, _tags);
            AppendParameter(_sb, Aeron.Context.ENDPOINT_PARAM_NAME, _endpoint);
            AppendParameter(_sb, Aeron.Context.INTERFACE_PARAM_NAME, _networkInterface);
            AppendParameter(_sb, Aeron.Context.MDC_CONTROL_PARAM_NAME, _controlEndpoint);
            AppendParameter(_sb, Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME, _controlMode);
            AppendParameter(_sb, Aeron.Context.MTU_LENGTH_PARAM_NAME, _mtu);
            AppendParameter(_sb, Aeron.Context.TERM_LENGTH_PARAM_NAME, _termLength);
            AppendParameter(_sb, Aeron.Context.INITIAL_TERM_ID_PARAM_NAME, _initialTermId);
            AppendParameter(_sb, Aeron.Context.TERM_ID_PARAM_NAME, _termId);
            AppendParameter(_sb, Aeron.Context.TERM_OFFSET_PARAM_NAME, _termOffset);

            if (null != _sessionId)
            {
                AppendParameter(_sb, Aeron.Context.SESSION_ID_PARAM_NAME, PrefixTag(_isSessionIdTagged, _sessionId));
            }

            AppendParameter(_sb, Aeron.Context.TTL_PARAM_NAME, _ttl);
            AppendParameter(_sb, Aeron.Context.RELIABLE_STREAM_PARAM_NAME, _reliable);
            AppendParameter(_sb, Aeron.Context.LINGER_PARAM_NAME, _linger);
            AppendParameter(_sb, Aeron.Context.ALIAS_PARAM_NAME, _alias);
            AppendParameter(_sb, Aeron.Context.CONGESTION_CONTROL_PARAM_NAME, _cc);
            AppendParameter(_sb, Aeron.Context.FLOW_CONTROL_PARAM_NAME, _fc);
            AppendParameter(_sb, Aeron.Context.GROUP_TAG_PARAM_NAME, _groupTag);
            AppendParameter(_sb, Aeron.Context.SPARSE_PARAM_NAME, _sparse);
            AppendParameter(_sb, Aeron.Context.EOS_PARAM_NAME, _eos);
            AppendParameter(_sb, Aeron.Context.TETHER_PARAM_NAME, _tether);
            AppendParameter(_sb, Aeron.Context.GROUP_PARAM_NAME, _group);
            AppendParameter(_sb, Aeron.Context.REJOIN_PARAM_NAME, _rejoin);
            AppendParameter(_sb, Aeron.Context.SPIES_SIMULATE_CONNECTION_PARAM_NAME, _ssc);
            AppendParameter(_sb, Aeron.Context.SOCKET_SNDBUF_PARAM_NAME, _socketSndbufLength);
            AppendParameter(_sb, Aeron.Context.SOCKET_RCVBUF_PARAM_NAME, _socketRcvbufLength);
            AppendParameter(_sb, Aeron.Context.RECEIVER_WINDOW_LENGTH_PARAM_NAME, _receiverWindowLength);
            AppendParameter(_sb, Aeron.Context.MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME, _mediaReceiveTimestampOffset);
            AppendParameter(_sb, Aeron.Context.CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME, _channelReceiveTimestampOffset);
            AppendParameter(_sb, Aeron.Context.CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME, _channelSendTimestampOffset);


            char lastChar = _sb[_sb.Length - 1];
            if (lastChar == '|' || lastChar == '?')
            {
                _sb.Length = _sb.Length - 1;
            }

            return _sb.ToString();
        }

        private static void AppendParameter(StringBuilder sb, String paramName, object paramValue)
        {
            if (null != paramValue)
            {
                sb.Append(paramName).Append('=').Append(paramValue).Append('|');
            }
        }
        
        public override string ToString()
        {
            return Build();
        }

        private static string PrefixTag(bool isTagged, long? value)
        {
            return isTagged ? TAG_PREFIX + value : value.ToString();
        }
    }
}