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

namespace Adaptive.Cluster.Service
{
    sealed class ServiceAdapter : IDisposable
    {
        private const int FragmentLimit = 1;

        private readonly Subscription _subscription;
        private readonly ClusteredServiceAgent _clusteredServiceAgent;
        private readonly FragmentAssembler _fragmentAssembler;

        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly JoinLogDecoder _joinLogDecoder = new JoinLogDecoder();
        private readonly RequestServiceAckDecoder _requestServiceAckDecoder = new RequestServiceAckDecoder();
        private readonly ServiceTerminationPositionDecoder _serviceTerminationPositionDecoder =
            new ServiceTerminationPositionDecoder();

        public ServiceAdapter(Subscription subscription, ClusteredServiceAgent clusteredServiceAgent)
        {
            this._subscription = subscription;
            this._clusteredServiceAgent = clusteredServiceAgent;
            this._fragmentAssembler = new FragmentAssembler(OnFragment);
        }

        public void Dispose()
        {
            _subscription?.Dispose();
        }

        internal int Poll()
        {
            return _subscription.Poll(_fragmentAssembler, FragmentLimit);
        }

        private void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ClusterException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId
                );
            }

            switch (_messageHeaderDecoder.TemplateId())
            {
                case JoinLogDecoder.TEMPLATE_ID:
                    _joinLogDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _clusteredServiceAgent.OnJoinLog(
                        _joinLogDecoder.LogPosition(),
                        _joinLogDecoder.MaxLogPosition(),
                        _joinLogDecoder.MemberId(),
                        _joinLogDecoder.LogSessionId(),
                        _joinLogDecoder.LogStreamId(),
                        _joinLogDecoder.IsStartup() == BooleanType.TRUE,
                        (ClusterRole)_joinLogDecoder.Role(),
                        _joinLogDecoder.LogChannel()
                    );
                    break;

                case ServiceTerminationPositionDecoder.TEMPLATE_ID:
                    _serviceTerminationPositionDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _clusteredServiceAgent.OnServiceTerminationPosition(
                        _serviceTerminationPositionDecoder.LogPosition()
                    );
                    break;

                case RequestServiceAckDecoder.TEMPLATE_ID:
                    _requestServiceAckDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _clusteredServiceAgent.OnRequestServiceAck(_requestServiceAckDecoder.LogPosition());
                    break;
            }
        }
    }
}
