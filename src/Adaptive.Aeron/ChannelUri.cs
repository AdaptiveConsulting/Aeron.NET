using System;
using System.Collections.Generic;
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Collections;

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
    public class ChannelUri
    {
        private enum State
        {
            MEDIA,
            PARAMS_KEY,
            PARAMS_VALUE
        }

        /// <summary>
        /// URI Scheme for Aeron channels.
        /// </summary>
        public const string AERON_SCHEME = "aeron";

        /// <summary>
        /// Qualifier for spy subscriptions which spy on outgoing network destined traffic efficiently.
        /// </summary>
        public const string SPY_QUALIFIER = "aeron-spy";

        public const long INVALID_TAG = Aeron.NULL_VALUE;

        private const int CHANNEL_TAG_INDEX = 0;
        private const int ENTITY_TAG_INDEX = 1;

        private static readonly string AERON_PREFIX = AERON_SCHEME + ":";

        private string _prefix;
        private string _media;
        private readonly IDictionary<string, string> _params;
        private readonly string[] _tags;

        /// <summary>
        /// Construct with the components provided to avoid parsing.
        /// </summary>
        /// <param name="prefix"> empty if no prefix is required otherwise expected to be 'aeron-spy' </param>
        /// <param name="media">  for the channel which is typically "udp" or "ipc". </param>
        /// <param name="params"> for the query string as key value pairs. </param>
        public ChannelUri(string prefix, string media, IDictionary<string, string> @params)
        {
            _prefix = prefix;
            _media = media;
            _params = @params;
            _tags = SplitTags(_params.GetOrDefault(Aeron.Context.TAGS_PARAM_NAME));
        }

        /// <summary>
        /// Construct with the components provided to avoid parsing.
        /// </summary>
        /// <param name="media">  for the channel which is typically "udp" or "ipc". </param>
        /// <param name="params"> for the query string as key value pairs. </param>
        public ChannelUri(string media, IDictionary<string, string> @params) : this("", media, @params)
        {
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
            _media = media;
            return this;
        }

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
            return _params[key];
        }

        /// <summary>
        /// Get the value for a given parameter key or the default value provided if the key does not exist.
        /// </summary>
        /// <param name="key">          to lookup. </param>
        /// <param name="defaultValue"> to be returned if no key match is found. </param>
        /// <returns> the value if set for the key otherwise the default value provided. </returns>
        public string Get(string key, string defaultValue)
        {
            string value = _params[key];
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
            return _params[key] = value;
        }
        
        /// <summary>
        /// Remove a key pair in the map of params.
        /// </summary>
        /// <param name="key"> of the param to be removed. </param>
        /// <returns> the previous value of the param or null. </returns>
        public string Remove(string key)
        {
            String ret = null;
            
            if (_params.ContainsKey(key))
            {
                ret = _params[key];
            }
            
            _params.Remove(key);
            return ret;
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
        /// Get the channel tag, if it exists, that refers to an another channel.
        /// </summary>
        /// <returns> channel tag if it exists or null if not in this URI. </returns>
        public string ChannelTag()
        {
            return  (null != _tags && _tags.Length > CHANNEL_TAG_INDEX) ? _tags[CHANNEL_TAG_INDEX] : null;
        }

        /// <summary>
        /// Get the entity tag, if it exists, that refers to an entity such as subscription or publication.
        /// </summary>
        /// <returns> entity tag if it exists or null if not in this URI. </returns>
        /// <see cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <see cref="Aeron.Context.TAG_PREFIX"/>
        public string EntityTag()
        {
            return (null != _tags && _tags.Length > ENTITY_TAG_INDEX) ? _tags[ENTITY_TAG_INDEX] : null;
        }

        /// <summary>
        /// Generate a String representation of the URI that is valid for an Aeron channel.
        /// </summary>
        /// <returns> a String representation of the URI that is valid for an Aeron channel. </returns>
        public override string ToString()
        {
            StringBuilder sb;
            if (ReferenceEquals(_prefix, null) || "".Equals(_prefix))
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

            Put(Aeron.Context.INITIAL_TERM_ID_PARAM_NAME, Convert.ToString(initialTermId));
            Put(Aeron.Context.TERM_ID_PARAM_NAME, Convert.ToString(termId));
            Put(Aeron.Context.TERM_OFFSET_PARAM_NAME, Convert.ToString(termOffset));
            Put(Aeron.Context.TERM_LENGTH_PARAM_NAME, Convert.ToString(termLength));
        }

        
        /// <summary>
        /// Parse a <seealso cref="string"/> which contains an Aeron URI.
        /// </summary>
        /// <param name="cs"> to be parsed. </param>
        /// <returns> a new <seealso cref="ChannelUri"/> representing the URI string. </returns>
        public static ChannelUri Parse(string cs)
        {
            int position = 0;
            string prefix;
            if (StartsWith(cs, 0, Aeron.Context.SPY_PREFIX))
            {
                prefix = SPY_QUALIFIER;
                position = Aeron.Context.SPY_PREFIX.Length;
            }
            else
            {
                prefix = "";
            }

            if (!StartsWith(cs, position, AERON_PREFIX))
            {
                throw new System.ArgumentException("Aeron URIs must start with 'aeron:', found: '" + cs + "'");
            }
            else
            {
                position += AERON_PREFIX.Length;
            }

            var builder = new StringBuilder();
            var @params = new Dictionary<string, string>();
            string media = null;
            string key = null;

            State state = State.MEDIA;
            for (int i = position; i < cs.Length; i++)
            {
                char c = cs[i];

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
                                throw new ArgumentException("encountered ':' within media definition");

                            default:
                                builder.Append(c);
                                break;
                        }

                        break;

                    case State.PARAMS_KEY:
                        if (c == '=')
                        {
                            key = builder.ToString();
                            builder.Length = 0;
                            state = State.PARAMS_VALUE;
                        }
                        else
                        {
                            builder.Append(c);
                        }
                        break;

                    case State.PARAMS_VALUE:
                        if (c == '|')
                        {
                            @params[key] = builder.ToString();
                            builder.Length = 0;
                            state = State.PARAMS_KEY;
                        }
                        else
                        {
                            builder.Append(c);
                        }

                        break;

                    default:
                        throw new InvalidOperationException("unexpected state=" + state);
                }
            }

            switch (state)
            {
                case State.MEDIA:
                    media = builder.ToString();
                    break;

                case State.PARAMS_VALUE:
                    @params[key] = builder.ToString();
                    break;

                default:
                    throw new ArgumentException("no more input found, but was in state: " + state);
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
            channelUri.Put(Aeron.Context.SESSION_ID_PARAM_NAME, Convert.ToString(sessionId));

            return channelUri.ToString();
        }
        
        /// <summary>
        /// Is the param value tagged? (starts with the "tag:" prefix)
        /// </summary>
        /// <param name="paramValue"> to check if tagged. </param>
        /// <returns> true if tagged or false if not. </returns>
        /// <see cref="Aeron.Context.TAGS_PARAM_NAME"/>
        /// <see cref="Aeron.Context.TAG_PREFIX"/>
        public static bool IsTagged(string paramValue)
        {
            return StartsWith(paramValue, 0, Aeron.Context.TAG_PREFIX);
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
                    tags = new[]{tagsValue};
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