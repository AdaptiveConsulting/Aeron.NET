/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;
using static Adaptive.Aeron.Aeron;
using static Adaptive.Aeron.LogBuffer.ControlledFragmentHandlerAction;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Poller for the egress from a cluster to capture administration message details.
    /// </summary>
    public class EgressPoller : IControlledFragmentHandler
    {
        private readonly int _fragmentLimit;
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
        private readonly ChallengeDecoder _challengeDecoder = new ChallengeDecoder();
        private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly SessionMessageHeaderDecoder _sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
        private readonly ControlledFragmentAssembler _fragmentAssembler;
        private readonly Subscription _subscription;

        private Image _egressImage;
        private long _clusterSessionId = NULL_VALUE;
        private long _correlationId = NULL_VALUE;
        private long _leadershipTermId = NULL_VALUE;
        private int _leaderMemberId = NULL_VALUE;
        private int _templateId = NULL_VALUE;
        private int _version = 0;
        private long _leaderHeartbeatTimeoutNs = NULL_VALUE;
        private bool _isPollComplete = false;
        private EventCode _eventCode;
        private string _detail = "";
        private byte[] _encodedChallenge;

        /// <summary>
        /// Construct a poller on the egress subscription.
        /// </summary>
        /// <param name="subscription">  for egress from the cluster. </param>
        /// <param name="fragmentLimit"> for each poll operation. </param>
        public EgressPoller(Subscription subscription, int fragmentLimit)
        {
            _fragmentAssembler = new ControlledFragmentAssembler(this);

            _subscription = subscription;
            _fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling events.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling events. </returns>
        public Subscription Subscription()
        {
            return _subscription;
        }

        /// <summary>
        /// <seealso cref="Image"/> for the egress response from the cluster which can be used for connection
        /// tracking.
        /// </summary>
        /// <returns> <seealso cref="Image"/> for the egress response from the cluster which can be used for
        /// connection tracking. </returns>
        public Image EgressImage()
        {
            return _egressImage;
        }

        /// <summary>
        /// Get the template id of the last received event.
        /// </summary>
        /// <returns> the template id of the last received event. </returns>
        public int TemplateId()
        {
            return _templateId;
        }

        /// <summary>
        /// Cluster session id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing.
        /// </summary>
        /// <returns> cluster session id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>
        /// if not returned. </returns>
        public long ClusterSessionId()
        {
            return _clusterSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned
        /// nothing.
        /// </summary>
        /// <returns> correlation id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>
        /// if not returned. </returns>
        public long CorrelationId()
        {
            return _correlationId;
        }

        /// <summary>
        /// Leadership term id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing.
        /// </summary>
        /// <returns> leadership term id of the last polled event or
        /// <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not returned. </returns>
        public long LeadershipTermId()
        {
            return _leadershipTermId;
        }

        /// <summary>
        /// Leader cluster member id of the last polled event or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing.
        /// </summary>
        /// <returns> leader cluster member id of the last polled event or
        /// <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public int LeaderMemberId()
        {
            return _leaderMemberId;
        }

        /// <summary>
        /// Get the event code returned from the last session event.
        /// </summary>
        /// <returns> the event code returned from the last session event. </returns>
        public EventCode EventCode()
        {
            return _eventCode;
        }

        /// <summary>
        /// Version response from the server in semantic version form.
        /// </summary>
        /// <returns> response from the server in semantic version form. </returns>
        public int Version()
        {
            return _version;
        }

        /// <summary>
        /// Leader heartbeat timeout of the last polled event or <seealso cref="NULL_VALUE"/> if not available.
        /// </summary>
        /// <returns> leader heartbeat timeout of the last polled event or <seealso cref="NULL_VALUE"/> if not
        /// available. </returns>
        public long LeaderHeartbeatTimeoutNs()
        {
            return _leaderHeartbeatTimeoutNs;
        }

        /// <summary>
        /// Get the detail returned from the last session event.
        /// </summary>
        /// <returns> the detail returned from the last session event. </returns>
        public string Detail()
        {
            return _detail;
        }

        /// <summary>
        /// Get the challenge data in the last challenge.
        /// </summary>
        /// <returns> the challenge data in the last challenge or null if last message was not a challenge. </returns>
        public byte[] EncodedChallenge()
        {
            return _encodedChallenge;
        }

        /// <summary>
        /// Has the last polling action received a complete event?
        /// </summary>
        /// <returns> true if the last polling action received a complete event. </returns>
        public bool IsPollComplete()
        {
            return _isPollComplete;
        }

        /// <summary>
        /// Was last message a challenge or not?
        /// </summary>
        /// <returns> true if last message was a challenge or false if not. </returns>
        public bool IsChallenged()
        {
            return ChallengeDecoder.TEMPLATE_ID == _templateId;
        }

        /// <summary>
        /// Reset last captured value and poll the egress subscription for output.
        /// </summary>
        /// <returns> number of fragments consumed. </returns>
        public int Poll()
        {
            if (_isPollComplete)
            {
                _clusterSessionId = NULL_VALUE;
                _correlationId = NULL_VALUE;
                _leadershipTermId = NULL_VALUE;
                _leaderMemberId = NULL_VALUE;
                _templateId = NULL_VALUE;
                _version = 0;
                _leaderHeartbeatTimeoutNs = NULL_VALUE;
                _eventCode = Codecs.EventCode.NULL_VALUE;
                _detail = "";
                _encodedChallenge = null;
                _isPollComplete = false;
            }

            return _subscription.ControlledPoll(_fragmentAssembler, _fragmentLimit);
        }

        /// <inheritdoc />
        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (_isPollComplete)
            {
                return ABORT;
            }

            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                return CONTINUE;
            }

            _templateId = _messageHeaderDecoder.TemplateId();
            switch (_templateId)
            {
                case SessionMessageHeaderDecoder.TEMPLATE_ID:
                    _sessionMessageHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _leadershipTermId = _sessionMessageHeaderDecoder.LeadershipTermId();
                    _clusterSessionId = _sessionMessageHeaderDecoder.ClusterSessionId();
                    _isPollComplete = true;
                    return BREAK;

                case SessionEventDecoder.TEMPLATE_ID:
                    _sessionEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _clusterSessionId = _sessionEventDecoder.ClusterSessionId();
                    _correlationId = _sessionEventDecoder.CorrelationId();
                    _leadershipTermId = _sessionEventDecoder.LeadershipTermId();
                    _leaderMemberId = _sessionEventDecoder.LeaderMemberId();
                    _eventCode = _sessionEventDecoder.Code();
                    _version = _sessionEventDecoder.Version();
                    _leaderHeartbeatTimeoutNs = LeaderHeartbeatTimeoutNsInternal(_sessionEventDecoder);
                    _detail = _sessionEventDecoder.Detail();
                    _isPollComplete = true;
                    _egressImage = (Image)header.Context;
                    return BREAK;

                case NewLeaderEventDecoder.TEMPLATE_ID:
                    _newLeaderEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _clusterSessionId = _newLeaderEventDecoder.ClusterSessionId();
                    _leadershipTermId = _newLeaderEventDecoder.LeadershipTermId();
                    _leaderMemberId = _newLeaderEventDecoder.LeaderMemberId();
                    _detail = _newLeaderEventDecoder.IngressEndpoints();
                    _isPollComplete = true;
                    _egressImage = (Image)header.Context;
                    return BREAK;

                case ChallengeDecoder.TEMPLATE_ID:
                    _challengeDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _encodedChallenge = new byte[_challengeDecoder.EncodedChallengeLength()];
                    _challengeDecoder.GetEncodedChallenge(
                        _encodedChallenge,
                        0,
                        _challengeDecoder.EncodedChallengeLength()
                    );

                    _clusterSessionId = _challengeDecoder.ClusterSessionId();
                    _correlationId = _challengeDecoder.CorrelationId();
                    _isPollComplete = true;
                    return BREAK;
            }

            return CONTINUE;
        }

        private static long LeaderHeartbeatTimeoutNsInternal(SessionEventDecoder sessionEventDecoder)
        {
            long leaderHeartbeatTimeoutNs = sessionEventDecoder.LeaderHeartbeatTimeoutNs();

            if (leaderHeartbeatTimeoutNs == SessionEventDecoder.LeaderHeartbeatTimeoutNsNullValue())
            {
                return NULL_VALUE;
            }

            return leaderHeartbeatTimeoutNs;
        }
    }
}
