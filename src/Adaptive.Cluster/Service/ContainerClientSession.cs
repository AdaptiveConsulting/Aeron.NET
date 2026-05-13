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
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;

namespace Adaptive.Cluster.Service
{
    public class ContainerClientSession : IClientSession
    {
        private readonly long _id;
        private readonly int _responseStreamId;
        private readonly string _responseChannel;
        private readonly byte[] _encodedPrincipal;

        private readonly ClusteredServiceAgent _clusteredServiceAgent;
        private Publication _responsePublication;
        private bool _isClosing;

        internal ContainerClientSession(
            long sessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal,
            ClusteredServiceAgent clusteredServiceAgent
        )
        {
            this._id = sessionId;
            this._responseStreamId = responseStreamId;
            this._responseChannel = responseChannel;
            this._encodedPrincipal = encodedPrincipal;
            this._clusteredServiceAgent = clusteredServiceAgent;
        }

        public long Id => _id;

        public int ResponseStreamId => _responseStreamId;

        public string ResponseChannel => _responseChannel;

        public byte[] EncodedPrincipal => _encodedPrincipal;

        public void Close()
        {
            if (null != _clusteredServiceAgent.GetClientSession(_id))
            {
                _clusteredServiceAgent.CloseClientSession(_id);
            }
        }

        public bool IsClosing => _isClosing;

        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            return _clusteredServiceAgent.Offer(_id, _responsePublication, buffer, offset, length);
        }

        public long Offer(DirectBufferVector[] vectors)
        {
            return _clusteredServiceAgent.Offer(_id, _responsePublication, vectors);
        }

        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            return _clusteredServiceAgent.TryClaim(_id, _responsePublication, length, bufferClaim);
        }

        internal void Connect(Aeron.Aeron aeron)
        {
            try
            {
                if (null == _responsePublication)
                {
                    _responsePublication = aeron.AddPublication(_responseChannel, _responseStreamId);
                }
            }
            catch (RegistrationException ex)
            {
                _clusteredServiceAgent.HandleError(
                    new ClusterException("failed to connect session response publication: " + ex.Message, Category.WARN)
                );
            }
        }

        internal void MarkClosing()
        {
            this._isClosing = true;
        }

        internal void ResetClosing()
        {
            _isClosing = false;
        }

        internal void Disconnect(ErrorHandler errorHandler)
        {
            CloseHelper.Dispose(errorHandler, _responsePublication);
            _responsePublication = null;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return
                "ClientSession{" +
                "id=" + _id +
                ", responseStreamId=" + _responseStreamId +
                ", responseChannel='" + _responseChannel + '\'' +
                ", encodedPrincipal=" + _encodedPrincipal +
                ", responsePublication=" + _responsePublication +
                ", isClosing=" + _isClosing +
                '}';
        }
    }
}
