using System;
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Type safe means of building a channel URI associated with a <seealso cref="Publication"/> or <seealso cref="Subscription"/>.
    /// </summary>
    /// <seealso cref="Aeron.AddPublication"/>
    /// <seealso cref="Aeron.AddSubscription(string,int)"/>
    /// <seealso cref="ChannelUri"/>
    public class ChannelUriStringBuilder
    {
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
        private string cc;
        private string fc;
        private bool? _reliable;
        private int? _ttl;
        private int? _mtu;
        private int? _termLength;
        private int? _initialTermId;
        private int? _termId;
        private int? _termOffset;
        private int? _sessionId;
        private long? groupTag;
        private long? _linger;
        private bool? _sparse;
        private bool? _eos;
        private bool? _tether;
        private bool? group;
        private bool? rejoin;
        private bool _isSessionIdTagged;

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
            cc = null;
            fc = null;
            _reliable = null;
            _ttl = null;
            _mtu = null;
            _termLength = null;
            _initialTermId = null;
            _termId = null;
            _termOffset = null;
            _sessionId = null;
            groupTag = null;
            _linger = null;
            _sparse = null;
            _eos = null;
            _tether = null;
            group = null;
            rejoin = null;
            _isSessionIdTagged = false;

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

            int count = 0;
            count += null == _initialTermId ? 0 : 1;
            count += null == _termId ? 0 : 1;
            count += null == _termOffset ? 0 : 1;

            if (count > 3)
            {
                if (count < 3)
                {
                    throw new ArgumentException(
                        "if any of then a complete set of 'initialTermId', 'termId', and 'termOffset' must be provided");
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
        /// Set the prefix for taking an addition action such as spying on an outgoing publication with "aeron-spy".
        /// </summary>
        /// <param name="prefix"> to be applied to the URI before the the scheme. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="ChannelUri.SPY_QUALIFIER"/>
        public ChannelUriStringBuilder Prefix(string prefix)
        {
            if (null != prefix && !prefix.Equals("") && !prefix.Equals(ChannelUri.SPY_QUALIFIER))
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
        /// <param name="controlEndpoint"> for joining a MDC control socket. </param>
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
        /// <param name="isReliable"> false if loss can be be gap filled. </param>
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
                return Ttl(Convert.ToInt32(ttlValue));
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

                return Mtu((int) value);
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
                    throw new InvalidOperationException("Term length more than max length of " +
                                                        LogBufferDescriptor.TERM_MAX_LENGTH + ": length=" +
                                                        _termLength);
                }

                return TermLength((int) value);
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
                return InitialTermId(Convert.ToInt32(initialTermIdValue));
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
                return TermId(Convert.ToInt32(termIdValue));
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
                return TermOffset(Convert.ToInt32(termOffsetValue));
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
        /// Set the sessionId value to be what is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        public ChannelUriStringBuilder SessionId(ChannelUri channelUri)
        {
            string sessionIdValue = channelUri.Get(Aeron.Context.SESSION_ID_PARAM_NAME);
            if (null == sessionIdValue)
            {
                _sessionId = null;
                return this;
            }
            else
            {
                return SessionId(Convert.ToInt32(sessionIdValue));
            }
        }

        /// <summary>
        /// Get the session id for a publication or restricted subscription.
        /// </summary>
        /// <returns> the session id for a publication or restricted subscription. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        public int? SessionId()
        {
            return _sessionId;
        }

        /// <summary>
        /// Set the time a network publication will linger in nanoseconds after being drained. This time is so that tail loss
        /// can be recovered.
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
        /// Get the time a network publication will linger in nanoseconds after being drained. This time is so that tail loss
        /// can be recovered.
        /// </summary>
        /// <returns> the linger time in nanoseconds a publication will wait around after being drained. </returns>
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
            this.group = group;
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
                group = null;
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
            return group;
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
        /// Is the value for <seealso cref="SessionId()"/> a tagged.
        /// </summary>
        /// <returns> whether the value for <seealso cref="SessionId()"/> a tag reference or not. </returns>
        /// <seealso cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <seealso cref="Aeron.Context.TAG_PREFIX"/>
        public bool IsSessionIdTagged()
        {
            return _isSessionIdTagged;
        }

        /// <summary>
        /// Set the alias for a URI. Alias's are not interpreted by Aeron and are to be used by the application
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
        /// Set the congestion control algorithm to be used on a channel.
        /// </summary>
        /// <param name="congestionControl"> for the URI. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.CONGESTION_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder CongestionControl(string congestionControl)
        {
            this.cc = congestionControl;
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
        /// Get the congestion control algorithm to be used on a channel.
        /// </summary>
        /// <returns> congestion control strategy for the channel. </returns>
        /// <seealso cref="Aeron.Context.CONGESTION_CONTROL_PARAM_NAME"/>
        public string CongestionControl()
        {
            return cc;
        }

        /// <summary>
        /// Set the flow control strategy to be used on a channel.
        /// </summary>
        /// <param name="flowControl"> for the URI. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.FLOW_CONTROL_PARAM_NAME"/>
        public ChannelUriStringBuilder FlowControl(string flowControl)
        {
            this.fc = flowControl;
            return this;
        }

        /// <summary>
        /// Set tagged flow control settings to be used on channel.  All specified values may be null and the default
        /// specified in the MediaDriver.Context will be used instead.
        /// </summary>
        /// <param name="groupTag">  receiver tag for this channel. </param>
        /// <param name="minGroupSize"> group size required to allow publications for this channel to be move to connected status. </param>
        /// <param name="timeout">      timeout receivers, default is ns, but allows suffixing of time units (e.g. 5s). </param>
        /// <returns> this for fluent API. </returns>
        public ChannelUriStringBuilder TaggedFlowControl(long? groupTag, int? minGroupSize, string timeout)
        {
            string flowControlValue = "tagged";

            if (null != groupTag || null != minGroupSize)
            {
                flowControlValue += ",g:";
            }

            if (null != groupTag)
            {
                flowControlValue += groupTag;
            }

            if (null != minGroupSize)
            {
                flowControlValue += "/";
                flowControlValue += minGroupSize;
            }

            if (null != timeout)
            {
                flowControlValue += ",t:";
                flowControlValue += timeout;
            }

            return FlowControl(flowControlValue);
        }


        /// <summary>
        /// Set min flow control settings to be used on channel.  All specified values may be null and the default
        /// specified in the MediaDriver.Context will be used instead.
        /// </summary>
        /// <param name="minGroupSize"> group size required to allow publications for this channel to be move to connected status. </param>
        /// <param name="timeout">      timeout receivers, default is ns, but allows suffixing of time units (e.g. 5s). </param>
        /// <returns> this for fluent API. </returns>
        public ChannelUriStringBuilder MinFlowControl(int? minGroupSize, string timeout)
        {
            string flowControlValue = "min";

            if (null != minGroupSize)
            {
                flowControlValue += ",g:/";
                flowControlValue += minGroupSize;
            }

            if (null != timeout)
            {
                flowControlValue += ",t:";
                flowControlValue += timeout;
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
        /// Get the flow control strategy to be used on a channel.
        /// </summary>
        /// <returns> flow control strategy for the channel. </returns>
        /// <seealso cref="Aeron.Context.FLOW_CONTROL_PARAM_NAME"/>
        public string FlowControl()
        {
            return fc;
        }

        /// <summary>
        /// Set the group tag (gtag) to be sent in SMs.
        /// </summary>
        /// <param name="groupTag"> to be sent in SMs </param>
        /// <returns> this for fluent API. </returns>
        /// <seealso cref="Aeron.Context.GROUP_TAG_PARAM_NAME"/>
        public ChannelUriStringBuilder GroupTag(long? groupTag)
        {
            this.groupTag = groupTag;
            return this;
        }

        /// <summary>
        /// Set the receiver tag to be value which is in the <seealso cref="ChannelUri"/> which may be null.
        /// </summary>
        /// <param name="channelUri"> to read the value from. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.GROUP_TAG_PARAM_NAME"/>
        public ChannelUriStringBuilder GroupTag(ChannelUri channelUri)
        {
            string groupTagValue = channelUri.Get(Aeron.Context.GROUP_TAG_PARAM_NAME);
            if (null == groupTagValue)
            {
                groupTag = null;
                return this;
            }
            else
            {
                return GroupTag(Convert.ToInt64(groupTagValue));
            }
        }

        /// <summary>
        /// Get the group tag (gtag) to be sent in SMs.
        /// </summary>
        /// <returns> receiver tag to be sent in SMs. </returns>
        /// <seealso cref="Aeron.Context.GROUP_TAG_PARAM_NAME"/>
        public long? GroupTag()
        {
            return groupTag;
        }

        /// <summary>
        /// Set the subscription semantics for if a stream should be rejoined after going unavailable.
        /// </summary>
        /// <param name="rejoin"> false if stream is not to be rejoined. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.REJOIN_PARAM_NAME"/>
        public ChannelUriStringBuilder Rejoin(bool? rejoin)
        {
            this.rejoin = rejoin;
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
                rejoin = null;
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
            return rejoin;
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
            if (position < 0 || 0 != (position & (FRAME_ALIGNMENT - 1)))
            {
                throw new ArgumentException("invalid position: " + position);
            }

            int bitsToShift = LogBufferDescriptor.PositionBitsToShift(termLength);

            _initialTermId = initialTermId;
            _termId = LogBufferDescriptor.ComputeTermIdFromPosition(position, bitsToShift, initialTermId);
            _termOffset = (int) (position & (termLength - 1));
            _termLength = termLength;

            return this;
        }

        /// <summary>
        /// Build a channel URI String for the given parameters.
        /// </summary>
        /// <returns> a channel URI String for the given parameters. </returns>
        public string Build()
        {
            _sb.Length = 0;

            if (null != _prefix && !"".Equals(_prefix))
            {
                _sb.Append(_prefix).Append(':');
            }

            _sb.Append(ChannelUri.AERON_SCHEME).Append(':').Append(_media).Append('?');

            if (null != _tags)
            {
                _sb.Append(Aeron.Context.TAGS_PARAM_NAME).Append('=').Append(_tags).Append('|');
            }

            if (null != _endpoint)
            {
                _sb.Append(Aeron.Context.ENDPOINT_PARAM_NAME).Append('=').Append(_endpoint).Append('|');
            }

            if (null != _networkInterface)
            {
                _sb.Append(Aeron.Context.INTERFACE_PARAM_NAME).Append('=').Append(_networkInterface).Append('|');
            }

            if (null != _controlEndpoint)
            {
                _sb.Append(Aeron.Context.MDC_CONTROL_PARAM_NAME).Append('=')
                    .Append(_controlEndpoint).Append('|');
            }

            if (null != _controlMode)
            {
                _sb.Append(Aeron.Context.MDC_CONTROL_MODE_PARAM_NAME).Append('=').Append(_controlMode).Append('|');
            }

            if (null != _mtu)
            {
                _sb.Append(Aeron.Context.MTU_LENGTH_PARAM_NAME).Append('=').Append(_mtu.Value).Append('|');
            }

            if (null != _termLength)
            {
                _sb.Append(Aeron.Context.TERM_LENGTH_PARAM_NAME).Append('=').Append(_termLength.Value).Append('|');
            }

            if (null != _initialTermId)
            {
                _sb.Append(Aeron.Context.INITIAL_TERM_ID_PARAM_NAME).Append('=').Append(_initialTermId.Value)
                    .Append('|');
            }

            if (null != _termId)
            {
                _sb.Append(Aeron.Context.TERM_ID_PARAM_NAME).Append('=').Append(_termId.Value).Append('|');
            }

            if (null != _termOffset)
            {
                _sb.Append(Aeron.Context.TERM_OFFSET_PARAM_NAME).Append('=').Append(_termOffset.Value).Append('|');
            }

            if (null != _sessionId)
            {
                _sb.Append(Aeron.Context.SESSION_ID_PARAM_NAME).Append('=')
                    .Append(PrefixTag(_isSessionIdTagged, _sessionId.Value)).Append('|');
            }

            if (null != _ttl)
            {
                _sb.Append(Aeron.Context.TTL_PARAM_NAME).Append('=').Append(_ttl.Value).Append('|');
            }

            if (null != _reliable)
            {
                _sb.Append(Aeron.Context.RELIABLE_STREAM_PARAM_NAME).Append('=').Append(_reliable).Append('|');
            }

            if (null != _linger)
            {
                _sb.Append(Aeron.Context.LINGER_PARAM_NAME).Append('=').Append(_linger.Value).Append('|');
            }

            if (null != _alias)
            {
                _sb.Append(Aeron.Context.ALIAS_PARAM_NAME).Append('=').Append(_alias).Append('|');
            }

            if (null != cc)
            {
                _sb.Append(Aeron.Context.CONGESTION_CONTROL_PARAM_NAME).Append('=').Append(cc).Append('|');
            }

            if (null != fc)
            {
                _sb.Append(Aeron.Context.FLOW_CONTROL_PARAM_NAME).Append('=').Append(fc).Append('|');
            }

            if (null != groupTag)
            {
                _sb.Append(Aeron.Context.GROUP_TAG_PARAM_NAME).Append('=').Append(groupTag).Append('|');
            }
            
            if (null != _sparse)
            {
                _sb.Append(Aeron.Context.SPARSE_PARAM_NAME).Append('=').Append(_sparse).Append('|');
            }

            if (null != _eos)
            {
                _sb.Append(Aeron.Context.EOS_PARAM_NAME).Append('=').Append(_eos).Append('|');
            }

            if (null != _tether)
            {
                _sb.Append(Aeron.Context.TETHER_PARAM_NAME).Append('=').Append(_tether).Append('|');
            }

            if (null != group)
            {
                _sb.Append(Aeron.Context.GROUP_PARAM_NAME).Append('=').Append(group).Append('|');
            }

            if (null != rejoin)
            {
                _sb.Append(Aeron.Context.REJOIN_PARAM_NAME).Append('=').Append(rejoin).Append('|');
            }
            
            char lastChar = _sb[_sb.Length - 1];
            if (lastChar == '|' || lastChar == '?')
            {
                _sb.Length = _sb.Length - 1;
            }

            return _sb.ToString();
        }

        private static string PrefixTag(bool isTagged, int value)
        {
            return isTagged ? TAG_PREFIX + value : value.ToString();
        }
    }
}