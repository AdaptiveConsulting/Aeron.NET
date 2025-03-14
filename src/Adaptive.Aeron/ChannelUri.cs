using System;
using System.Collections.Generic;
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Collections;
using static Adaptive.Aeron.Aeron.Context;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Parser for Aeron channel URIs. The format is:
    /// <pre>
    /// aeron-uri = "aeron:" media [ "?" param *( "|" param ) ]
    /// media     = *( "[^?:]" )
    /// param     = key "=" value
    /// key       = *( "[^=]" )
    /// value     = *( "[^|]" )
    /// </pre>
    /// <para>
    /// Multiple params with the same key are allowed, the last value specified takes precedence.
    /// </para>
    /// </summary>
    /// <seealso cref="ChannelUriStringBuilder"/>
    public sealed class ChannelUri
    {
        private enum State
        {
            MEDIA,
            PARAMS_KEY,
            PARAMS_VALUE
        }

        /// <summary>
        /// URI Scheme for Aeron channels and destinations.
        /// </summary>
        public const string AERON_SCHEME = "aeron";

        /// <summary>
        /// Qualifier for spy subscriptions which spy on outgoing network destined traffic efficiently.
        /// </summary>
        public const string SPY_QUALIFIER = "aeron-spy";

        /// <summary>
        /// Invalid tag value returned when calling <seealso cref="GetTag(string)"/> and the channel is not tagged.
        /// </summary>
        public const long INVALID_TAG = Aeron.NULL_VALUE;
        
        /// <summary>
        /// Max length in characters for the URI string.
        /// </summary>
        public const int MAX_URI_LENGTH = 4095;

        private const int CHANNEL_TAG_INDEX = 0;
        private const int ENTITY_TAG_INDEX = 1;

        private static readonly string AERON_PREFIX = AERON_SCHEME + ":";

        private string _prefix;
        private string _media;
        private readonly Map<string, string> _params;
        private readonly string[] _tags;

        /// <summary>
        /// Construct with the components provided to avoid parsing.
        /// </summary>
        /// <param name="prefix"> empty if no prefix is required otherwise expected to be 'aeron-spy' </param>
        /// <param name="media">  for the channel which is typically "udp" or "ipc". </param>
        /// <param name="params"> for the query string as key value pairs. </param>
        public ChannelUri(string prefix, string media, Map<string, string> @params)
        {
            _prefix = prefix;
            _media = media;
            _params = @params;
            _tags = SplitTags(_params.Get(TAGS_PARAM_NAME));
        }

        /// <summary>
        /// The prefix for the channel.
        /// </summary>
        /// <returns> the prefix for the channel. </returns>
        public string Prefix()
        {
            return _prefix;
        }

        /// <summary>
        /// Change the prefix from what has been parsed.
        /// </summary>
        /// <param name="prefix"> to replace the existing prefix. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUri Prefix(string prefix)
        {
            _prefix = prefix;
            return this;
        }

        /// <summary>
        /// The media over which the channel operates.
        /// </summary>
        /// <returns> the media over which the channel operates. </returns>
        public string Media()
        {
            return _media;
        }

        /// <summary>
        /// Set the media over which the channel operates.
        /// </summary>
        /// <param name="media"> to replace the parsed value. </param>
        /// <returns> this for a fluent API. </returns>
        public ChannelUri Media(string media)
        {
            ValidateMedia(media);
            _media = media;
            return this;
        }

        /// <summary>
        /// Is the channel <seealso cref="Media()"/> equal to <seealso cref="Aeron.Context.UDP_MEDIA"/>.
        /// </summary>
        /// <returns> true the channel <seealso cref="Media()"/> equals <seealso cref="Aeron.Context.UDP_MEDIA"/>. </returns>
        public bool IsUdp => UDP_MEDIA.Equals(_media);

        /// <summary>
        /// Is the channel <seealso cref="Media()"/> equal to <seealso cref="Aeron.Context.IPC_MEDIA"/>.
        /// </summary>
        /// <returns> true the channel <seealso cref="Media()"/> equals <seealso cref="Aeron.Context.IPC_MEDIA"/>. </returns>
        public bool IsIpc => IPC_MEDIA.Equals(_media);

        /// <summary>
        /// The scheme for the URI. Must be "aeron".
        /// </summary>
        /// <returns> the scheme for the URI. </returns>
        public string Scheme()
        {
            return AERON_SCHEME;
        }

        /// <summary>
        /// Get a value for a given parameter key.
        /// </summary>
        /// <param name="key"> to lookup. </param>
        /// <returns> the value if set for the key otherwise null. </returns>
        public string Get(string key)
        {
            return _params.Get(key);
        }

        /// <summary>
        /// Get the value for a given parameter key or the default value provided if the key does not exist.
        /// </summary>
        /// <param name="key">          to lookup. </param>
        /// <param name="defaultValue"> to be returned if no key match is found. </param>
        /// <returns> the value if set for the key otherwise the default value provided. </returns>
        public string Get(string key, string defaultValue)
        {
            string value = _params.Get(key);
            if (null != value)
            {
                return value;
            }

            return defaultValue;
        }

        /// <summary>
        /// Put a key and value pair in the map of params.
        /// </summary>
        /// <param name="key">   of the param to be put. </param>
        /// <param name="value"> of the param to be put. </param>
        /// <returns> the existing value otherwise null. </returns>
        public string Put(string key, string value)
        {
            return _params.Put(key, value);
        }

        /// <summary>
        /// Remove a key pair in the map of params.
        /// </summary>
        /// <param name="key"> of the param to be removed. </param>
        /// <returns> the previous value of the param or null. </returns>
        public string Remove(string key)
        {
            return _params.Remove(key);
        }

        /// <summary>
        /// Does the URI contain a value for the given key.
        /// </summary>
        /// <param name="key"> to be lookup. </param>
        /// <returns> true if the key has a value otherwise false. </returns>
        /// <see cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <see cref="Aeron.Context.TAG_PREFIX"/>
        public bool ContainsKey(string key)
        {
            return _params.ContainsKey(key);
        }

        /// <summary>
        /// Get the channel tag, if it exists, that refers to another channel.
        /// </summary>
        /// <returns> channel tag if it exists or null if not in this URI. </returns>
        public string ChannelTag()
        {
            return (null != _tags && _tags.Length > CHANNEL_TAG_INDEX) ? _tags[CHANNEL_TAG_INDEX] : null;
        }

        /// <summary>
        /// Get the entity tag, if it exists, that refers to an entity such as subscription or publication.
        /// </summary>
        /// <returns> entity tag if it exists or null if not in this URI. </returns>
        /// <see cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <see cref="Aeron.Context.TAG_PREFIX"/>
        public string EntityTag()
        {
            return _tags.Length > ENTITY_TAG_INDEX ? _tags[ENTITY_TAG_INDEX] : null;
        }

        private bool Equals(ChannelUri other)
        {
            return _prefix == other._prefix && _media == other._media && Equals(_params, other._params) &&
                   Equals(_tags, other._tags);
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj is ChannelUri other))
            {
                return false;
            }

            return Equals(_prefix, other._prefix) &&
                   Equals(_media, other._media) &&
                   Equals(_params, other._params) &&
                   Equals(_tags, other._tags);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (_prefix != null ? _prefix.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (_media != null ? _media.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (_params != null ? _params.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (_tags != null ? _tags.GetHashCode() : 0);
                return hashCode;
            }
        }

        /// <summary>
        /// Generate a String representation of the URI that is valid for an Aeron channel.
        /// </summary>
        /// <returns> a String representation of the URI that is valid for an Aeron channel. </returns>
        public override string ToString()
        {
            StringBuilder sb;
            if (ReferenceEquals(_prefix, null) || string.IsNullOrEmpty(_prefix))
            {
                sb = new StringBuilder((_params.Count * 20) + 10);
            }
            else
            {
                sb = new StringBuilder(_params.Count * 20 + 20);
                sb.Append(_prefix);

                if (!_prefix.EndsWith(":"))
                {
                    sb.Append(":");
                }
            }

            sb.Append(AERON_PREFIX).Append(_media);

            if (_params.Count > 0)
            {
                sb.Append('?');

                foreach (KeyValuePair<string, string> entry in _params)
                {
                    sb.Append(entry.Key).Append('=').Append(entry.Value).Append('|');
                }

                sb.Length = sb.Length - 1;
            }

            return sb.ToString();
        }

        /// <summary>
        /// Initialise a channel for restarting a publication at a given position.
        /// </summary>
        /// <param name="position">      at which the publication should be started. </param>
        /// <param name="initialTermId"> what which the stream would start. </param>
        /// <param name="termLength">    for the stream. </param>
        public void InitialPosition(long position, int initialTermId, int termLength)
        {
            if (position < 0 || 0 != (position & (FrameDescriptor.FRAME_ALIGNMENT - 1)))
            {
                throw new ArgumentException("invalid position: " + position);
            }

            int bitsToShift = LogBufferDescriptor.PositionBitsToShift(termLength);
            int termId = LogBufferDescriptor.ComputeTermIdFromPosition(position, bitsToShift, initialTermId);
            int termOffset = (int)(position & (termLength - 1));

            Put(INITIAL_TERM_ID_PARAM_NAME, Convert.ToString(initialTermId));
            Put(TERM_ID_PARAM_NAME, Convert.ToString(termId));
            Put(TERM_OFFSET_PARAM_NAME, Convert.ToString(termOffset));
            Put(TERM_LENGTH_PARAM_NAME, Convert.ToString(termLength));
        }


        /// <summary>
        /// Parse a <seealso cref="string"/> which contains an Aeron URI.
        /// </summary>
        /// <param name="uri"> to be parsed. </param>
        /// <returns> a new <seealso cref="ChannelUri"/> representing the URI string. </returns>
        public static ChannelUri Parse(string uri)
        {
            int length = uri.Length;
            if (length > MAX_URI_LENGTH)
            {
                throw new ArgumentException("URI length (" + length + ") exceeds max supported length (" +
                                            MAX_URI_LENGTH + "): " + uri.Substring(0, MAX_URI_LENGTH));
            }

            
            int position = 0;
            string prefix;
            if (StartsWith(uri, 0, SPY_PREFIX))
            {
                prefix = SPY_QUALIFIER;
                position = SPY_PREFIX.Length;
            }
            else
            {
                prefix = "";
            }

            if (!StartsWith(uri, position, AERON_PREFIX))
            {
                throw new ArgumentException("Aeron URIs must start with 'aeron:', found: " + uri);
            }
            else
            {
                position += AERON_PREFIX.Length;
            }

            var builder = new StringBuilder();
            var @params = new Map<string, string>();
            string media = null;
            string key = null;

            State state = State.MEDIA;
            for (int i = position; i < length; i++)
            {
                char c = uri[i];
                switch (state)
                {
                    case State.MEDIA:
                        switch (c)
                        {
                            case '?':
                                media = builder.ToString();
                                builder.Length = 0;
                                state = State.PARAMS_KEY;
                                break;

                            case ':':
                            case '|':
                            case '=':
                                throw new ArgumentException("encountered '" + c +
                                                            "' within media definition at index " + i + " in " + uri);

                            default:
                                builder.Append(c);
                                break;
                        }

                        break;

                    case State.PARAMS_KEY:
                        if (c == '=')
                        {
                            if (0 == builder.Length)
                            {
                                throw new ArgumentException("empty key not allowed at index " + i + " in " + uri);
                            }

                            key = builder.ToString();
                            builder.Length = 0;
                            state = State.PARAMS_VALUE;
                        }
                        else
                        {
                            if (c == '|')
                            {
                                throw new ArgumentException("invalid end of key at index " + i + " in " + uri);
                            }

                            builder.Append(c);
                        }

                        break;

                    case State.PARAMS_VALUE:
                        if (c == '|')
                        {
                            @params.Put(key, builder.ToString());
                            builder.Length = 0;
                            state = State.PARAMS_KEY;
                        }
                        else
                        {
                            builder.Append(c);
                        }

                        break;

                    default:
                        throw new ArgumentException("unexpected state=" + state + " in " + uri);
                }
            }

            switch (state)
            {
                case State.MEDIA:
                    media = builder.ToString();
                    ValidateMedia(media);
                    break;

                case State.PARAMS_VALUE:
                    @params.Put(key, builder.ToString());
                    break;

                default:
                    throw new ArgumentException("no more input found, state=" + state + " in " + uri);
            }

            return new ChannelUri(prefix, media, @params);
        }

        /// <summary>
        /// Add a sessionId to a given channel.
        /// </summary>
        /// <param name="channel">   to add sessionId to. </param>
        /// <param name="sessionId"> to add to channel. </param>
        /// <returns> new string that represents channel with sessionId added. </returns>
        public static string AddSessionId(string channel, int sessionId)
        {
            ChannelUri channelUri = Parse(channel);
            channelUri.Put(SESSION_ID_PARAM_NAME, Convert.ToString(sessionId));

            return channelUri.ToString();
        }
        
        /// <summary>
        /// Add alias to the uri if none exists.
        /// </summary>
        /// <param name="uri">   to add alias to. </param>
        /// <param name="alias"> to add to the uri. </param>
        /// <returns> original uri if alias is empty or one is already defined, otherwise new uri with an alias. </returns>
        public static string AddAliasIfAbsent(string uri, string alias)
        {
            if (!string.IsNullOrEmpty(alias))
            {
                ChannelUri channelUri = ChannelUri.Parse(uri);
                if (!channelUri.ContainsKey(ALIAS_PARAM_NAME))
                {
                    channelUri.Put(ALIAS_PARAM_NAME, alias);
                    return channelUri.ToString();
                }
            }
            return uri;
        }

        /// <summary>
        /// Is the param value tagged? (starts with the "tag:" prefix).
        /// </summary>
        /// <param name="paramValue"> to check if tagged. </param>
        /// <returns> true if tagged or false if not. </returns>
        /// <see cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <see cref="Aeron.Context.TAG_PREFIX"/>
        public static bool IsTagged(string paramValue)
        {
            return StartsWith(paramValue, 0, TAG_PREFIX);
        }

        /// <summary>
        /// Get the value of the tag from a given parameter value.
        /// </summary>
        /// <param name="paramValue"> to extract the tag value from. </param>
        /// <returns> the value of the tag or <seealso cref="INVALID_TAG"/> if not tagged. </returns>
        /// <see cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <see cref="Aeron.Context.TAG_PREFIX"/>
        public static long GetTag(string paramValue)
        {
            return IsTagged(paramValue) ? long.Parse(paramValue.Substring(4, paramValue.Length - 4)) : INVALID_TAG;
        }

        /// <summary>
        /// Create a channel URI for a destination, i.e. a channel that uses {@code media} and {@code interface} parameters
        /// of the original channel and adds specified {@code endpoint} to it. For example given the input channel is
        /// {@code aeron:udp?mtu=1440|ttl=0|endpoint=localhost:8090|term-length=128k|interface=eth0} and the endpoint is
        /// {@code 192.168.0.14} the output of this method will be {@code aeron:udp?endpoint=192.168.0.14|interface=eth0}.
        /// </summary>
        /// <param name="channel">  for which the destination is being added. </param>
        /// <param name="endpoint"> for the target destination. </param>
        /// <returns> new channel URI for a destination. </returns>
        public static string CreateDestinationUri(string channel, string endpoint)
        {
            ChannelUri channelUri = ChannelUri.Parse(channel);
            string uri = AERON_PREFIX + channelUri.Media() + "?" + ENDPOINT_PARAM_NAME + "=" + endpoint;
            string networkInterface = channelUri.Get(INTERFACE_PARAM_NAME);

            if (null != networkInterface)
            {
                return uri + "|" + INTERFACE_PARAM_NAME + "=" + networkInterface;
            }

            return uri;
        }

        /// <summary>
        /// Uses the supplied endpoint to resolve any wildcard ports. If the existing endpoint has a value of "0" for then
        /// the port of this endpoint will be used instead. If the endpoint is not specified in this uri, then the whole
        /// supplied endpoint is used. If the endpoint exists and has a non-wildcard port, then the existing endpoint is
        /// retained.
        /// </summary>
        /// <param name="resolvedEndpoint"> The endpoint to supply a resolved endpoint port. </param>
        /// <exception cref="ArgumentException"> if the supplied resolvedEndpoint does not have a port or the port is zero. </exception>
        /// <exception cref="ArgumentNullException"> if the supplied resolvedEndpoint is null </exception>
        public void ReplaceEndpointWildcardPort(string resolvedEndpoint)
        {
            if (null == resolvedEndpoint)
            {
                throw new ArgumentNullException(nameof(resolvedEndpoint), "resolvedEndpoint is null");
            }

            int portSeparatorIndex = resolvedEndpoint.LastIndexOf(':');
            if (-1 == portSeparatorIndex)
            {
                throw new ArgumentException("No port specified on resolvedEndpoint=" + resolvedEndpoint);
            }

            if (resolvedEndpoint.EndsWith(":0", StringComparison.Ordinal))
            {
                throw new ArgumentException("Wildcard port specified on resolvedEndpoint=" + resolvedEndpoint);
            }

            string existingEndpoint = Get(ENDPOINT_PARAM_NAME);
            if (null == existingEndpoint)
            {
                Put(ENDPOINT_PARAM_NAME, resolvedEndpoint);
            }
            else if (existingEndpoint.EndsWith(":0", StringComparison.Ordinal))
            {
                string endpoint = existingEndpoint.Substring(0, existingEndpoint.Length - 2) +
                                  resolvedEndpoint.Substring(resolvedEndpoint.LastIndexOf(':'));
                Put(ENDPOINT_PARAM_NAME, endpoint);
            }
        }
        
        /// <summary>
        /// Call consumer for each parameter defined in the URI.
        /// </summary>
        /// <param name="consumer"> to be invoked for each parameter. </param>
        public void ForEachParameter(Action<string, string> consumer)
        {
            _params.ForEach(consumer);
        }

        /// <summary>
        /// Determines if this channel has specified <code>control-mode=response</code>.
        /// </summary>
        /// <returns> true if this channel has specified <code>control-mode=response</code>. </returns>
        public bool HasControlModeResponse()
        {
            return CONTROL_MODE_RESPONSE.Equals(Get(MDC_CONTROL_MODE_PARAM_NAME));
        }

        /// <summary>
        /// Determines if the supplied channel has specified <code>control-mode=response</code>.
        /// </summary>
        /// <param name="channelUri"> to check if the control mode is response </param>
        /// <returns> true if the supplied channel has specified <code>control-mode=response</code>. </returns>
        public static bool IsControlModeResponse(string channelUri)
        {
            return Parse(channelUri).HasControlModeResponse();
        }


        private static void ValidateMedia(string media)
        {
            if (IPC_MEDIA.Equals(media) || UDP_MEDIA.Equals(media))
            {
                return;
            }

            throw new ArgumentException("unknown media: " + media);
        }


        private static bool StartsWith(string input, int position, string prefix)
        {
            if (input.Length - position < prefix.Length)
            {
                return false;
            }

            for (int i = 0; i < prefix.Length; i++)
            {
                if (input[position + i] != prefix[i])
                {
                    return false;
                }
            }

            return true;
        }

        private static string[] SplitTags(string tagsValue)
        {
            string[] tags = ArrayUtil.EMPTY_STRING_ARRAY;

            if (null != tagsValue)
            {
                int tagCount = CountTags(tagsValue);
                if (tagCount == 1)
                {
                    tags = new[] { tagsValue };
                }
                else
                {
                    int tagStartPosition = 0;
                    int tagIndex = 0;
                    tags = new string[tagCount];

                    for (int i = 0, length = tagsValue.Length; i < length; i++)
                    {
                        if (tagsValue[i] == ',')
                        {
                            tags[tagIndex++] = tagsValue.Substring(tagStartPosition, i - tagStartPosition);
                            tagStartPosition = i + 1;

                            if (tagIndex >= (tagCount - 1))
                            {
                                tags[tagIndex] = tagsValue.Substring(tagStartPosition, length - tagStartPosition);
                            }
                        }
                    }
                }
            }

            return tags;
        }

        private static int CountTags(string tags)
        {
            int count = 1;

            for (int i = 0, length = tags.Length; i < length; i++)
            {
                if (tags[i] == ',')
                {
                    ++count;
                }
            }

            return count;
        }
    }
}