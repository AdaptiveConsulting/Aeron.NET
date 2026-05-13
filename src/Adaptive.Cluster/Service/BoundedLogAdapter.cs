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

using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Adapter for reading a log with a upper bound applied beyond which the consumer cannot progress.
    /// </summary>
    internal sealed class BoundedLogAdapter : IControlledFragmentHandler, IDisposable
    {
        private readonly int _fragmentLimit;
        private long _maxLogPosition;
        private Image _image;
        private readonly ClusteredServiceAgent _agent;
        private readonly BufferBuilder _builder = new BufferBuilder();
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SessionMessageHeaderDecoder _sessionHeaderDecoder = new SessionMessageHeaderDecoder();
        private readonly TimerEventDecoder _timerEventDecoder = new TimerEventDecoder();
        private readonly SessionOpenEventDecoder _openEventDecoder = new SessionOpenEventDecoder();
        private readonly SessionCloseEventDecoder _closeEventDecoder = new SessionCloseEventDecoder();
        private readonly ClusterActionRequestDecoder _actionRequestDecoder = new ClusterActionRequestDecoder();
        private readonly NewLeadershipTermEventDecoder _newLeadershipTermEventDecoder =
            new NewLeadershipTermEventDecoder();

        internal BoundedLogAdapter(ClusteredServiceAgent agent, int fragmentLimit)
        {
            this._agent = agent;
            this._fragmentLimit = fragmentLimit;
        }

        public void Dispose()
        {
            if (null != _image)
            {
                _image.Subscription?.Dispose();
                _image = null;
            }
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            ControlledFragmentHandlerAction action = ControlledFragmentHandlerAction.CONTINUE;
            byte flags = header.Flags;

            if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
            {
                action = OnMessage(buffer, offset, length, header);
            }
            else if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
            {
                _builder
                    .Reset()
                    .CaptureHeader(header)
                    .Append(buffer, offset, length)
                    .NextTermOffset(BitUtil.Align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT));
            }
            else if (offset == _builder.NextTermOffset())
            {
                int limit = _builder.Limit();

                _builder.Append(buffer, offset, length);

                if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                {
                    action = OnMessage(_builder.Buffer(), 0, _builder.Limit(), _builder.CompleteHeader(header));

                    if (ControlledFragmentHandlerAction.ABORT == action)
                    {
                        _builder.Limit(limit);
                    }
                    else
                    {
                        _builder.Reset();
                    }
                }
                else
                {
                    _builder.NextTermOffset(BitUtil.Align(offset + length + HEADER_LENGTH, FRAME_ALIGNMENT));
                }
            }
            else
            {
                _builder.Reset();
            }

            return action;
        }

        internal void MaxLogPosition(long position)
        {
            _maxLogPosition = position;
        }

        public bool IsDone()
        {
            return _image.Position >= _maxLogPosition || _image.IsEndOfStream || _image.Closed;
        }

        internal void Image(Image image)
        {
            this._image = image;
        }

        internal Image Image()
        {
            return _image;
        }

        public int Poll(long limit)
        {
            return _image.BoundedControlledPoll(this, limit, _fragmentLimit);
        }

        // Upstream: io.aeron.cluster.service.BoundedLogAdapter#onFragment is @SuppressWarnings("MethodLength").
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S138:Functions should not have too many lines",
            Justification = "Upstream Java parity; method is itself @SuppressWarnings(\"MethodLength\")."
        )]
        private ControlledFragmentHandlerAction OnMessage(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);
            int templateId = _messageHeaderDecoder.TemplateId();

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId
                );
            }

            if (templateId == SessionMessageHeaderEncoder.TEMPLATE_ID)
            {
                _sessionHeaderDecoder.Wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    _messageHeaderDecoder.BlockLength(),
                    _messageHeaderDecoder.Version()
                );

                _agent.OnSessionMessage(
                    header.Position,
                    _sessionHeaderDecoder.ClusterSessionId(),
                    _sessionHeaderDecoder.Timestamp(),
                    buffer,
                    offset + AeronCluster.SESSION_HEADER_LENGTH,
                    length - AeronCluster.SESSION_HEADER_LENGTH,
                    header
                );

                return ControlledFragmentHandlerAction.CONTINUE;
            }

            switch (templateId)
            {
                case TimerEventDecoder.TEMPLATE_ID:
                    _timerEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _agent.OnTimerEvent(
                        header.Position,
                        _timerEventDecoder.CorrelationId(),
                        _timerEventDecoder.Timestamp()
                    );
                    break;

                case SessionOpenEventDecoder.TEMPLATE_ID:
                    _openEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    string responseChannel = _openEventDecoder.ResponseChannel();
                    byte[] encodedPrincipal = new byte[_openEventDecoder.EncodedPrincipalLength()];
                    _openEventDecoder.GetEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.Length);

                    _agent.OnSessionOpen(
                        _openEventDecoder.LeadershipTermId(),
                        header.Position,
                        _openEventDecoder.ClusterSessionId(),
                        _openEventDecoder.Timestamp(),
                        _openEventDecoder.ResponseStreamId(),
                        responseChannel,
                        encodedPrincipal
                    );
                    break;

                case SessionCloseEventDecoder.TEMPLATE_ID:
                    _closeEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _agent.OnSessionClose(
                        _closeEventDecoder.LeadershipTermId(),
                        header.Position,
                        _closeEventDecoder.ClusterSessionId(),
                        _closeEventDecoder.Timestamp(),
                        _closeEventDecoder.CloseReason()
                    );
                    break;

                case ClusterActionRequestDecoder.TEMPLATE_ID:
                    _actionRequestDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    var flags =
                        ClusterActionRequestDecoder.FlagsNullValue() != _actionRequestDecoder.Flags()
                            ? _actionRequestDecoder.Flags()
                            : ClusteredServiceContainer.CLUSTER_ACTION_FLAGS_DEFAULT;

                    _agent.OnServiceAction(
                        _actionRequestDecoder.LeadershipTermId(),
                        _actionRequestDecoder.LogPosition(),
                        _actionRequestDecoder.Timestamp(),
                        _actionRequestDecoder.Action(),
                        flags
                    );
                    break;

                case NewLeadershipTermEventDecoder.TEMPLATE_ID:
                    _newLeadershipTermEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    var clusterTimeUnit =
                        _newLeadershipTermEventDecoder.TimeUnit() == ClusterTimeUnit.NULL_VALUE
                            ? ClusterTimeUnit.MILLIS
                            : _newLeadershipTermEventDecoder.TimeUnit();

                    _agent.OnNewLeadershipTermEvent(
                        _newLeadershipTermEventDecoder.LeadershipTermId(),
                        _newLeadershipTermEventDecoder.LogPosition(),
                        _newLeadershipTermEventDecoder.Timestamp(),
                        _newLeadershipTermEventDecoder.TermBaseLogPosition(),
                        _newLeadershipTermEventDecoder.LeaderMemberId(),
                        _newLeadershipTermEventDecoder.LogSessionId(),
                        clusterTimeUnit,
                        _newLeadershipTermEventDecoder.AppVersion()
                    );
                    break;

                case MembershipChangeEventDecoder.TEMPLATE_ID:
                    // Removed Dynamic Join.
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}
