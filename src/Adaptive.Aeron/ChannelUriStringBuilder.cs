using System;
using System.Text;
using Adaptive.Aeron.LogBuffer;

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
        private bool? _reliable;
        private int? _ttl;
        private int? _mtu;
        private int? _termLength;
        private int? _initialTermId;
        private int? _termId;
        private int? _termOffset;
        private int? _sessionId;
        private int? _linger;
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
            _reliable = null;
            _ttl = null;
            _mtu = null;
            _termLength = null;
            _initialTermId = null;
            _termId = null;
            _termOffset = null;
            _sessionId = null;
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
                throw new InvalidOperationException("Either 'endpoint' or 'control' must be specified for UDP.");
            }

            int count = 0;
            count += null == _initialTermId ? 0 : 1;
            count += null == _termId ? 0 : 1;
            count += null == _termOffset ? 0 : 1;

            if (count > 0 && count < 3)
            {
                throw new InvalidOperationException(
                    "If any of then a complete set of 'initialTermId', 'termId', and 'termOffset' must be provided");
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
                throw new ArgumentException("Invalid prefix: " + prefix);
            }

            _prefix = prefix;
            return this;
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
                    throw new ArgumentException("Invalid media: " + media);
            }

            _media = media;
            return this;
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
                throw new ArgumentException("Invalid control mode: " + controlMode);
            }

            _controlMode = controlMode;
            return this;
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

                if ((mtu & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
                {
                    throw new ArgumentException("MTU not a multiple of FRAME_ALIGNMENT: mtu=" + mtu);
                }
            }

            _mtu = mtu;
            return this;
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
                    throw new ArgumentException("Term offset not in range 0-1g: " + termOffset);
                }

                if (0 != (termOffset & (FrameDescriptor.FRAME_ALIGNMENT - 1)))
                {
                    throw new ArgumentException("Term offset not multiple of FRAME_ALIGNMENT: " + termOffset);
                }
            }

            _termOffset = termOffset;
            return this;
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
        /// Get the session id for a publication or restricted subscription.
        /// </summary>
        /// <returns> the session id for a publication or restricted subscription. </returns>
        /// <seealso cref="Aeron.Context.SESSION_ID_PARAM_NAME"/>
        public int? SessionId()
        {
            return _sessionId;
        }

        /// <summary>
        /// Set the time a publication will linger in nanoseconds after being drained. This time is so that tail loss
        /// can be recovered.
        /// </summary>
        /// <param name="lingerNs"> time for the publication after it is drained. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context.LINGER_PARAM_NAME"></seealso>
        public ChannelUriStringBuilder Linger(int? lingerNs)
        {
            if (null != lingerNs && lingerNs < 0)
            {
                throw new ArgumentException("Linger value cannot be negative: " + lingerNs);
            }

            _linger = lingerNs;
            return this;
        }

        /// <summary>
        /// Get the time a publication will linger in nanoseconds after being drained. This time is so that tail loss
        /// can be recovered.
        /// </summary>
        /// <returns> the linger time in nanoseconds a publication will wait around after being drained. </returns>
        /// <seealso cref="Aeron.Context.LINGER_PARAM_NAME"></seealso>
        public int? Linger()
        {
            return _linger;
        }

        /// <summary>
        /// Set the tags for a channel, and/or publication or subscription.
        /// </summary>
        /// <param name="tags"> for the channel, publication or subscription. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="Aeron.Context#TAGS_PARAM_NAME"/>
        public ChannelUriStringBuilder Tags(string tags)
        {
            _tags = tags;
            return this;
        }

        /// <summary>
        /// Get the tags for a channel, and/or publication or subscription.
        /// </summary>
        /// <returns> the tags for a channel, publication or subscription. </returns>
        /// <seealso cref="Aeron.Context#TAGS_PARAM_NAME"/>
        public string Tags()
        {
            return _tags;
        }

        /// <summary>
        /// Toggle the value for <seealso cref="SessionId()"/> being tagged or not.
        /// </summary>
        /// <param name="isSessionIdTagged"> for session id </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUriStringBuilder IsSessionIdTagged(bool isSessionIdTagged)
        {
            _isSessionIdTagged = isSessionIdTagged;
            return this;
        }

        /// <summary>
        /// Is the value for <seealso cref="SessionId()"/> a tagged.
        /// </summary>
        /// <returns> whether the value for <seealso cref="SessionId()"/> a tag reference or not. </returns>
        public bool IsSessionIdTagged()
        {
            return _isSessionIdTagged;
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

            if (null != _reliable)
            {
                _sb.Append(Aeron.Context.RELIABLE_STREAM_PARAM_NAME).Append('=').Append(_reliable).Append('|');
            }

            if (null != _ttl)
            {
                _sb.Append(Aeron.Context.TTL_PARAM_NAME).Append('=').Append(_ttl.Value).Append('|');
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
                _sb.Append(Aeron.Context.SESSION_ID_PARAM_NAME).Append('=').Append(PrefixTag(_isSessionIdTagged, _sessionId.Value)).Append('|');
            }

            if (null != _linger)
            {
                _sb.Append(Aeron.Context.LINGER_PARAM_NAME).Append('=').Append(_linger.Value).Append('|');
            }

            char lastChar = _sb[_sb.Length - 1];
            if (lastChar == '|' || lastChar == '?')
            {
                _sb.Length = _sb.Length - 1;
            }

            return _sb.ToString();
        }

        /// <summary>
        /// Call <seealso cref="Convert.ToInt32(String)"/> only if the value param is not null. Else pass null on.
        /// </summary>
        /// <param name="value"> to check for null and convert if not null. </param>
        /// <returns> null if value param is null or result of <seealso cref="Convert.ToInt32(String)"/>. </returns>
        public static int? IntegerValueOf(string value)
        {
            return null == value ? (int?) null : Convert.ToInt32(value);
        }

        private static string PrefixTag(bool isTagged, int value)
        {
            return isTagged ? TAG_PREFIX + value : value.ToString();
        }
    }
}