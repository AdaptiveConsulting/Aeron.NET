/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Adaptive.Aeron.Command;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.RingBuffer;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Separates the concern of communicating with the client conductor away from the rest of the client.
    /// 
    /// Writes commands into the client conductor buffer.
    /// 
    /// Note: this class is not thread safe and is expecting to be called under the {@link ClientConductor} main lock.
    /// </summary>
    public class DriverProxy
    {
        /// <summary>
        /// Maximum capacity of the write buffer
        /// </summary>
        public const int MSG_BUFFER_CAPACITY = 2048;

        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(BufferUtil.AllocateDirectAligned(MSG_BUFFER_CAPACITY,BitUtil.CACHE_LINE_LENGTH * 2)); // todo expandable
        private readonly PublicationMessageFlyweight _publicationMessage = new PublicationMessageFlyweight();
        private readonly SubscriptionMessageFlyweight _subscriptionMessage = new SubscriptionMessageFlyweight();
        private readonly RemoveMessageFlyweight _removeMessage = new RemoveMessageFlyweight();
        private readonly CorrelatedMessageFlyweight _correlatedMessage = new CorrelatedMessageFlyweight();
        private readonly DestinationMessageFlyweight _destinationMessage = new DestinationMessageFlyweight();
        private readonly CounterMessageFlyweight _counterMessage = new CounterMessageFlyweight();
        private readonly IRingBuffer _toDriverCommandBuffer;

        public DriverProxy(IRingBuffer toDriverCommandBuffer, long clientId)
        {
            if (toDriverCommandBuffer == null) throw new ArgumentNullException(nameof(toDriverCommandBuffer));

            _toDriverCommandBuffer = toDriverCommandBuffer;

            _publicationMessage.Wrap(_buffer, 0);
            _subscriptionMessage.Wrap(_buffer, 0);

            _correlatedMessage.Wrap(_buffer, 0);
            _removeMessage.Wrap(_buffer, 0);
            _destinationMessage.Wrap(_buffer, 0);
            _counterMessage.Wrap(_buffer, 0);

            _correlatedMessage.ClientId(clientId);
        }

        public long TimeOfLastDriverKeepaliveMs()
        {
            return _toDriverCommandBuffer.ConsumerHeartbeatTime();
        }

        public long AddPublication(string channel, int streamId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _publicationMessage.CorrelationId(correlationId);

            _publicationMessage
                .StreamId(streamId)
                .Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_PUBLICATION, _buffer, 0,
                _publicationMessage.Length()))
            {
                throw new AeronException("Could not write add publication command");
            }

            return correlationId;
        }

        public long AddExclusivePublication(string channel, int streamId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _publicationMessage.CorrelationId(correlationId);

            _publicationMessage
                .StreamId(streamId)
                .Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_EXCLUSIVE_PUBLICATION, _buffer, 0,
                _publicationMessage.Length()))
            {
                throw new AeronException("Could not write add exclusive publication command");
            }

            return correlationId;
        }

        public long RemovePublication(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _removeMessage.RegistrationId(registrationId).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_PUBLICATION, _buffer, 0,
                RemoveMessageFlyweight.Length()))
            {
                throw new AeronException("Could not write remove publication command");
            }

            return correlationId;
        }

        public long AddSubscription(string channel, int streamId)
        {
            const long registrationId = Aeron.NULL_VALUE;
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _subscriptionMessage.CorrelationId(correlationId);

            _subscriptionMessage.RegistrationCorrelationId(registrationId).StreamId(streamId).Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_SUBSCRIPTION, _buffer, 0,
                _subscriptionMessage.Length()))
            {
                throw new AeronException("Could not write add subscription command");
            }

            return correlationId;
        }

        public long RemoveSubscription(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _removeMessage.RegistrationId(registrationId).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_SUBSCRIPTION, _buffer, 0,
                RemoveMessageFlyweight.Length()))
            {
                throw new AeronException("Could not write remove subscription message");
            }

            return correlationId;
        }

        public void SendClientKeepalive()
        {
            _correlatedMessage.CorrelationId(0);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.CLIENT_KEEPALIVE, _buffer, 0,
                CorrelatedMessageFlyweight.LENGTH))
            {
                throw new AeronException("Could not send client keepalive command");
            }
        }

        public long AddDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _destinationMessage.RegistrationCorrelationId(registrationId).Channel(endpointChannel)
                .CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_DESTINATION, _buffer, 0,
                _destinationMessage.Length()))
            {
                throw new AeronException("Could not write destination command");
            }

            return correlationId;
        }

        public long RemoveDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _destinationMessage.RegistrationCorrelationId(registrationId).Channel(endpointChannel)
                .CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_DESTINATION, _buffer, 0,
                _destinationMessage.Length()))
            {
                throw new AeronException("Could not write destination command");
            }

            return correlationId;
        }

        public long AddRcvDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _destinationMessage.RegistrationCorrelationId(registrationId).Channel(endpointChannel)
                .CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_RCV_DESTINATION, _buffer, 0,
                _destinationMessage.Length()))
            {
                throw new AeronException("Could not write rcv destination command");
            }

            return correlationId;
        }

        public long RemoveRcvDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _destinationMessage.RegistrationCorrelationId(registrationId).Channel(endpointChannel)
                .CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_RCV_DESTINATION, _buffer, 0,
                _destinationMessage.Length()))
            {
                throw new AeronException("Could not write rcv destination command");
            }

            return correlationId;
        }


        public long AddCounter(int typeId, IDirectBuffer keyBuffer, int keyOffset, int keyLength,
            IDirectBuffer labelBuffer, int labelOffset, int labelLength)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _counterMessage.TypeId(typeId).KeyBuffer(keyBuffer, keyOffset, keyLength)
                .LabelBuffer(labelBuffer, labelOffset, labelLength).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_COUNTER, _buffer, 0, _counterMessage.Length()))
            {
                throw new AeronException("Could not write add counter command");
            }

            return correlationId;
        }

        public long AddCounter(int typeId, string label)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _counterMessage.TypeId(typeId).KeyBuffer(null, 0, 0).Label(label).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_COUNTER, _buffer, 0, _counterMessage.Length()))
            {
                throw new AeronException("Could not write add counter command");
            }

            return correlationId;
        }

        public long RemoveCounter(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _removeMessage.RegistrationId(registrationId).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_COUNTER, _buffer, 0,
                RemoveMessageFlyweight.Length()))
            {
                throw new AeronException("Could not write remove counter command");
            }

            return correlationId;
        }

        public void ClientClose()
        {
            _correlatedMessage.CorrelationId(_toDriverCommandBuffer.NextCorrelationId());
            _toDriverCommandBuffer.Write(ControlProtocolEvents.CLIENT_CLOSE, _buffer, 0, CorrelatedMessageFlyweight.LENGTH);
        }
    }
}