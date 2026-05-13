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
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using static Adaptive.Cluster.Service.ClusteredServiceContainer.Configuration;

namespace Adaptive.Cluster.Service
{
    internal class ServiceSnapshotLoader : IControlledFragmentHandler
    {
        private const int FragmentLimit = 10;

        private bool _inSnapshot = false;
        private bool _isDone = false;
        private int _appVersion;
        private ClusterTimeUnit _timeUnit;

        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly SnapshotMarkerDecoder _snapshotMarkerDecoder = new SnapshotMarkerDecoder();
        private readonly ClientSessionDecoder _clientSessionDecoder = new ClientSessionDecoder();
        private readonly ImageControlledFragmentAssembler _fragmentAssembler;
        private readonly Image _image;
        private readonly ClusteredServiceAgent _agent;

        internal ServiceSnapshotLoader(Image image, ClusteredServiceAgent agent)
        {
            this._fragmentAssembler = new ImageControlledFragmentAssembler(this);
            this._image = image;
            this._agent = agent;
        }

        internal bool IsDone()
        {
            return _isDone;
        }

        internal int AppVersion()
        {
            return _appVersion;
        }

        internal ClusterTimeUnit TimeUnit()
        {
            return _timeUnit;
        }

        internal int Poll()
        {
            return _image.ControlledPoll(_fragmentAssembler, FragmentLimit);
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (MessageHeaderDecoder.SCHEMA_ID != schemaId)
            {
                throw new ClusterException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId
                );
            }

            switch (_messageHeaderDecoder.TemplateId())
            {
                case SnapshotMarkerDecoder.TEMPLATE_ID:
                    _snapshotMarkerDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    long typeId = _snapshotMarkerDecoder.TypeId();
                    if (SNAPSHOT_TYPE_ID != typeId)
                    {
                        throw new ClusterException("unexpected snapshot type: " + typeId);
                    }

                    switch (_snapshotMarkerDecoder.Mark())
                    {
                        case SnapshotMark.BEGIN:
                            if (_inSnapshot)
                            {
                                throw new ClusterException("already in snapshot");
                            }

                            _inSnapshot = true;
                            _appVersion = _snapshotMarkerDecoder.AppVersion();
                            _timeUnit =
                                _snapshotMarkerDecoder.TimeUnit() == ClusterTimeUnit.NULL_VALUE
                                    ? ClusterTimeUnit.MILLIS
                                    : _snapshotMarkerDecoder.TimeUnit();

                            return ControlledFragmentHandlerAction.CONTINUE;

                        case SnapshotMark.END:
                            if (!_inSnapshot)
                            {
                                throw new ClusterException("missing begin snapshot");
                            }

                            _isDone = true;
                            return ControlledFragmentHandlerAction.BREAK;

                        case SnapshotMark.SECTION:
                        case SnapshotMark.NULL_VALUE:
                            break;
                    }

                    break;

                case ClientSessionDecoder.TEMPLATE_ID:
                    _clientSessionDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    string responseChannel = _clientSessionDecoder.ResponseChannel();
                    byte[] encodedPrincipal = new byte[_clientSessionDecoder.EncodedPrincipalLength()];
                    _clientSessionDecoder.GetEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.Length);

                    _agent.AddSession(
                        _clientSessionDecoder.ClusterSessionId(),
                        _clientSessionDecoder.ResponseStreamId(),
                        responseChannel,
                        encodedPrincipal
                    );
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}
