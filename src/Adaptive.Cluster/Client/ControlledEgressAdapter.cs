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
using static Adaptive.Cluster.Client.AeronCluster;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Adapter for dispatching egress messages from a cluster to a <seealso cref="IControlledEgressListener"/> .
    /// </summary>
    public sealed class ControlledEgressAdapter : IControlledFragmentHandler
    {
        private readonly long _clusterSessionId;
        private readonly int _fragmentLimit;
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionEventDecoder _sessionEventDecoder = new SessionEventDecoder();
        private readonly NewLeaderEventDecoder _newLeaderEventDecoder = new NewLeaderEventDecoder();
        private readonly AdminResponseDecoder _adminResponseDecoder = new AdminResponseDecoder();
        private readonly SessionMessageHeaderDecoder _sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
        private ControlledFragmentAssembler _fragmentAssembler;
        private readonly IControlledEgressListener _listener;
        private readonly IControlledEgressListenerExtension _listenerExtension;
        private readonly Subscription _subscription;

        /// <summary>
        /// Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
        /// <seealso cref="IControlledEgressListener"/>.
        /// </summary>
        /// <param name="listener">         to dispatch events to. </param>
        /// <param name="clusterSessionId"> for the egress. </param>
        /// <param name="subscription">     over the egress stream. </param>
        /// <param name="fragmentLimit">    to poll on each <seealso cref="Poll()"/> operation. </param>
        public ControlledEgressAdapter(
            IControlledEgressListener listener,
            long clusterSessionId,
            Subscription subscription,
            int fragmentLimit
        )
            : this(listener, null, clusterSessionId, subscription, fragmentLimit)
        {
            _fragmentAssembler = new ControlledFragmentAssembler(this);
        }

        /// <summary>
        /// Construct an adapter for cluster egress which consumes from the subscription and dispatches to the
        /// <seealso cref="IControlledEgressListener"/> or extension messages to
        /// <seealso cref="IControlledEgressListenerExtension"/>.
        /// </summary>
        /// <param name="listener">          to dispatch events to. </param>
        /// <param name="listenerExtension"> to dispatch extension messages to </param>
        /// <param name="clusterSessionId">  for the egress. </param>
        /// <param name="subscription">      over the egress stream. </param>
        /// <param name="fragmentLimit">     to poll on each <seealso cref="Poll()"/> operation. </param>
        public ControlledEgressAdapter(
            IControlledEgressListener listener,
            IControlledEgressListenerExtension listenerExtension,
            long clusterSessionId,
            Subscription subscription,
            int fragmentLimit
        )
        {
            this._clusterSessionId = clusterSessionId;
            this._fragmentLimit = fragmentLimit;
            this._listener = listener;
            this._listenerExtension = listenerExtension;
            this._subscription = subscription;
        }

        /// <summary>
        /// Poll the egress subscription and dispatch assembled events to the
        /// <seealso cref="IControlledEgressListener"/>.
        /// </summary>
        /// <returns> the number of fragments consumed. </returns>
        public int Poll()
        {
            return _subscription.ControlledPoll(_fragmentAssembler, _fragmentLimit);
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        // Upstream: io.aeron.cluster.client.ControlledEgressAdapter#onFragment is @SuppressWarnings("MethodLength").
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S138:Functions should not have too many lines",
            Justification = "Upstream Java parity; method is itself @SuppressWarnings(\"MethodLength\")."
        )]
        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);

            int templateId = _messageHeaderDecoder.TemplateId();
            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                if (_listenerExtension != null)
                {
                    return _listenerExtension.OnExtensionMessage(
                        _messageHeaderDecoder.BlockLength(),
                        templateId,
                        schemaId,
                        _messageHeaderDecoder.Version(),
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        length - MessageHeaderDecoder.ENCODED_LENGTH
                    );
                }

                throw new ClusterException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId
                );
            }

            switch (templateId)
            {
                case SessionMessageHeaderDecoder.TEMPLATE_ID:
                {
                    _sessionMessageHeaderDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    long sessionId = _sessionMessageHeaderDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        return _listener.OnMessage(
                            sessionId,
                            _sessionMessageHeaderDecoder.Timestamp(),
                            buffer,
                            offset + SESSION_HEADER_LENGTH,
                            length - SESSION_HEADER_LENGTH,
                            header
                        );
                    }

                    break;
                }

                case SessionEventDecoder.TEMPLATE_ID:
                {
                    _sessionEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    long sessionId = _sessionEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _listener.OnSessionEvent(
                            _sessionEventDecoder.CorrelationId(),
                            sessionId,
                            _sessionEventDecoder.LeadershipTermId(),
                            _sessionEventDecoder.LeaderMemberId(),
                            _sessionEventDecoder.Code(),
                            _sessionEventDecoder.Detail()
                        );
                    }

                    break;
                }

                case NewLeaderEventDecoder.TEMPLATE_ID:
                {
                    _newLeaderEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    long sessionId = _newLeaderEventDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        _listener.OnNewLeader(
                            sessionId,
                            _newLeaderEventDecoder.LeadershipTermId(),
                            _newLeaderEventDecoder.LeaderMemberId(),
                            _newLeaderEventDecoder.IngressEndpoints()
                        );
                    }

                    break;
                }

                case AdminResponseDecoder.TEMPLATE_ID:
                {
                    _adminResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    long sessionId = _adminResponseDecoder.ClusterSessionId();
                    if (sessionId == _clusterSessionId)
                    {
                        long correlationId = _adminResponseDecoder.CorrelationId();
                        AdminRequestType requestType = _adminResponseDecoder.RequestType();
                        AdminResponseCode responseCode = _adminResponseDecoder.ResponseCode();
                        string message = _adminResponseDecoder.Message();
                        int payloadOffset =
                            _adminResponseDecoder.Offset()
                            + AdminResponseDecoder.BLOCK_LENGTH
                            + AdminResponseDecoder.MessageHeaderLength()
                            + message.Length
                            + AdminResponseDecoder.PayloadHeaderLength();
                        int payloadLength = _adminResponseDecoder.PayloadLength();
                        _listener.OnAdminResponse(
                            sessionId,
                            correlationId,
                            requestType,
                            responseCode,
                            message,
                            buffer,
                            payloadOffset,
                            payloadLength
                        );
                    }

                    break;
                }

                default:
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}
