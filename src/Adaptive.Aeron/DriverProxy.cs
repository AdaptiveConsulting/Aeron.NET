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

using Adaptive.Aeron.Command;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.RingBuffer;
using static Adaptive.Aeron.Command.ControlProtocolEvents;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Separates the concern of communicating with the client conductor away from the rest of the client.
    /// 
    /// For writing commands into the client conductor buffer.
    /// 
    /// Note: this class is not thread safe and is expecting to be called within <see cref="Aeron.Context.ClientLock(Adaptive.Agrona.Concurrent.ILock)"/>.
    /// </summary>
    public class DriverProxy
    {
        private readonly long _clientId;
        private readonly PublicationMessageFlyweight _publicationMessage = new PublicationMessageFlyweight();
        private readonly SubscriptionMessageFlyweight _subscriptionMessage = new SubscriptionMessageFlyweight();
        private readonly RemoveCounterFlyweight removeCounter = new RemoveCounterFlyweight();
        private readonly RemovePublicationFlyweight removePublication = new RemovePublicationFlyweight();
        private readonly RemoveSubscriptionFlyweight removeSubscription = new RemoveSubscriptionFlyweight();
        private readonly DestinationMessageFlyweight _destinationMessage = new DestinationMessageFlyweight();

        private readonly DestinationByIdMessageFlyweight
            _destinationByIdMessage = new DestinationByIdMessageFlyweight();

        private readonly CounterMessageFlyweight _counterMessage = new CounterMessageFlyweight();

        private readonly StaticCounterMessageFlyweight _staticCounterMessageFlyweight =
            new StaticCounterMessageFlyweight();

        private readonly RejectImageFlyweight _rejectImage = new RejectImageFlyweight();
        private readonly IRingBuffer _toDriverCommandBuffer;

        public DriverProxy(IRingBuffer toDriverCommandBuffer, long clientId)
        {
            _toDriverCommandBuffer = toDriverCommandBuffer;
            _clientId = clientId;
        }

        /// <summary>
        /// Time of the last heartbeat to indicate the driver is alive.
        /// </summary>
        /// <returns> time of the last heartbeat to indicate the driver is alive. </returns>
        public long TimeOfLastDriverKeepaliveMs()
        {
            return _toDriverCommandBuffer.ConsumerHeartbeatTime();
        }

        /// <summary>
        /// Instruct the driver to add a concurrent publication.
        /// </summary>
        /// <param name="channel">  uri in string format. </param>
        /// <param name="streamId"> within the channel. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddPublication(string channel, int streamId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = PublicationMessageFlyweight.ComputeLength(channel.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_PUBLICATION, length);
            if (index < 0)
            {
                throw new AeronException("could not write add publication command");
            }

            _publicationMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .StreamId(streamId)
                .Channel(channel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Instruct the driver to add a non-concurrent, i.e. exclusive, publication.
        /// </summary>
        /// <param name="channel">  uri in string format. </param>
        /// <param name="streamId"> within the channel. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddExclusivePublication(string channel, int streamId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = PublicationMessageFlyweight.ComputeLength(channel.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_EXCLUSIVE_PUBLICATION, length);
            if (index < 0)
            {
                throw new AeronException("could not write add exclusive publication command");
            }

            _publicationMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .StreamId(streamId)
                .Channel(channel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Instruct the driver to remove a publication by its registration id.
        /// </summary>
        /// <param name="registrationId"> for the publication to be removed. </param>
        /// <param name="revoke"> whether the publication is being revoked.</param>
        /// <returns> the correlation id for the command. </returns>
        public long RemovePublication(long registrationId, bool revoke)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int index = _toDriverCommandBuffer.TryClaim(REMOVE_PUBLICATION,
                RemovePublicationFlyweight.Length());
            if (index < 0)
            {
                throw new AeronException("could not write remove publication command");
            }

            removePublication
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .Revoke(revoke)
                .RegistrationId(registrationId)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Instruct the driver to add a subscription.
        /// </summary>
        /// <param name="channel">  uri in string format. </param>
        /// <param name="streamId"> within the channel. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddSubscription(string channel, int streamId)
        {
            long registrationId = Aeron.NULL_VALUE;
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = SubscriptionMessageFlyweight.ComputeLength(channel.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_SUBSCRIPTION, length);
            if (index < 0)
            {
                throw new AeronException("could not write add subscription command");
            }

            _subscriptionMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationCorrelationId(registrationId)
                .StreamId(streamId)
                .Channel(channel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Instruct the driver to remove a subscription by its registration id.
        /// </summary>
        /// <param name="registrationId"> for the subscription to be removed. </param>
        /// <returns> the correlation id for the command. </returns>
        public long RemoveSubscription(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int index = _toDriverCommandBuffer.TryClaim(REMOVE_SUBSCRIPTION,
                RemoveSubscriptionFlyweight.Length());
            if (index < 0)
            {
                throw new AeronException("could not write remove subscription command");
            }

            removeSubscription
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationId(registrationId)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Add a destination to the send channel of an existing MDC Publication.
        /// </summary>
        /// <param name="registrationId">  of the Publication. </param>
        /// <param name="endpointChannel"> for the destination. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = DestinationMessageFlyweight.ComputeLength(endpointChannel.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_DESTINATION, length);
            if (index < 0)
            {
                throw new AeronException("could not write add destination command");
            }

            _destinationMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationCorrelationId(registrationId)
                .Channel(endpointChannel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Remove a destination from the send channel of an existing MDC Publication.
        /// </summary>
        /// <param name="registrationId">  of the Publication. </param>
        /// <param name="endpointChannel"> used for the <seealso cref="AddDestination(long, string)"/> command. </param>
        /// <returns> the correlation id for the command. </returns>
        public long RemoveDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = DestinationMessageFlyweight.ComputeLength(endpointChannel.Length);
            int index = _toDriverCommandBuffer.TryClaim(REMOVE_DESTINATION, length);
            if (index < 0)
            {
                throw new AeronException("could not write remove destination command");
            }

            _destinationMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationCorrelationId(registrationId)
                .Channel(endpointChannel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Remove a destination from the send channel of an existing MDC Publication.
        /// </summary>
        /// <param name="publicationRegistrationId">  of the Publication. </param>
        /// <param name="destinationRegistrationId"> used for the <seealso cref="AddDestination(long, string)"/> command. </param>
        /// <returns> the correlation id for the command. </returns>
        public long RemoveDestination(long publicationRegistrationId, long destinationRegistrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int index = _toDriverCommandBuffer.TryClaim(REMOVE_DESTINATION_BY_ID,
                DestinationByIdMessageFlyweight.MESSAGE_LENGTH);
            if (index < 0)
            {
                throw new AeronException("could not write remove destination command");
            }

            _destinationByIdMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .ResourceRegistrationId(publicationRegistrationId)
                .DestinationRegistrationId(destinationRegistrationId)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Add a destination to the receive channel endpoint of an existing MDS Subscription.
        /// </summary>
        /// <param name="registrationId">  of the Subscription. </param>
        /// <param name="endpointChannel"> for the destination. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddRcvDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = DestinationMessageFlyweight.ComputeLength(endpointChannel.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_RCV_DESTINATION, length);
            if (index < 0)
            {
                throw new AeronException("could not write add rcv destination command");
            }

            _destinationMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationCorrelationId(registrationId)
                .Channel(endpointChannel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Remove a destination from the receive channel endpoint of an existing MDS Subscription.
        /// </summary>
        /// <param name="registrationId">  of the Subscription. </param>
        /// <param name="endpointChannel"> used for the <seealso cref="AddRcvDestination(long, string)"/> command. </param>
        /// <returns> the correlation id for the command. </returns>
        public long RemoveRcvDestination(long registrationId, string endpointChannel)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = DestinationMessageFlyweight.ComputeLength(endpointChannel.Length);
            int index = _toDriverCommandBuffer.TryClaim(REMOVE_RCV_DESTINATION, length);
            if (index < 0)
            {
                throw new AeronException("could not write remove rcv destination command");
            }

            _destinationMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationCorrelationId(registrationId)
                .Channel(endpointChannel)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Add a new counter with a type id plus the label and key are provided in buffers.
        /// </summary>
        /// <param name="typeId">      for associating with the counter. </param>
        /// <param name="keyBuffer">   containing the metadata key. </param>
        /// <param name="keyOffset">   offset at which the key begins. </param>
        /// <param name="keyLength">   length in bytes for the key. </param>
        /// <param name="labelBuffer"> containing the label. </param>
        /// <param name="labelOffset"> offset at which the label begins. </param>
        /// <param name="labelLength"> length in bytes for the label. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddCounter(int typeId, IDirectBuffer keyBuffer, int keyOffset, int keyLength,
            IDirectBuffer labelBuffer, int labelOffset, int labelLength)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = CounterMessageFlyweight.ComputeLength(keyLength, labelLength);
            int index = _toDriverCommandBuffer.TryClaim(ADD_COUNTER, length);
            if (index < 0)
            {
                throw new AeronException("could not write add counter command");
            }

            _counterMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .KeyBuffer(keyBuffer, keyOffset, keyLength)
                .LabelBuffer(labelBuffer, labelOffset, labelLength)
                .TypeId(typeId)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Add a new counter with a type id and label, the key will be blank.
        /// </summary>
        /// <param name="typeId"> for associating with the counter. </param>
        /// <param name="label">  that is human-readable for the counter. </param>
        /// <returns> the correlation id for the command. </returns>
        public long AddCounter(int typeId, string label)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = CounterMessageFlyweight.ComputeLength(0, label.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_COUNTER, length);
            if (index < 0)
            {
                throw new AeronException("could not write add counter command");
            }

            _counterMessage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .KeyBuffer(null, 0, 0)
                .Label(label)
                .TypeId(typeId)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Instruct the media driver to remove an existing counter by its registration id.
        /// </summary>
        /// <param name="registrationId"> of counter to remove. </param>
        /// <returns> the correlation id for the command. </returns>
        public long RemoveCounter(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int index = _toDriverCommandBuffer.TryClaim(REMOVE_COUNTER,
                RemoveCounterFlyweight.Length());
            if (index < 0)
            {
                throw new AeronException("could not write remove counter command");
            }

            removeCounter
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .RegistrationId(registrationId)
                .ClientId(_clientId)
                .CorrelationId(correlationId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        /// <summary>
        /// Notify the media driver that this client is closing.
        /// </summary>
        public void ClientClose()
        {
            int index = _toDriverCommandBuffer.TryClaim(CLIENT_CLOSE,
                CorrelatedMessageFlyweight.LENGTH);
            if (index > 0)
            {
                (new CorrelatedMessageFlyweight())
                    .Wrap(_toDriverCommandBuffer.Buffer(), index)
                    .ClientId(_clientId)
                    .CorrelationId(Aeron.NULL_VALUE);

                _toDriverCommandBuffer.Commit(index);
            }
        }

        /// <summary>
        /// Instruct the media driver to terminate.
        /// </summary>
        /// <param name="tokenBuffer"> containing the authentication token. </param>
        /// <param name="tokenOffset"> at which the token begins. </param>
        /// <param name="tokenLength"> in bytes. </param>
        /// <returns> true is successfully sent. </returns>
        public bool TerminateDriver(IDirectBuffer tokenBuffer, int tokenOffset, int tokenLength)
        {
            int length = TerminateDriverFlyweight.ComputeLength(tokenLength);
            int index = _toDriverCommandBuffer.TryClaim(TERMINATE_DRIVER, length);
            if (index > 0)
            {
                (new TerminateDriverFlyweight())
                    .Wrap(_toDriverCommandBuffer.Buffer(), index)
                    .TokenBuffer(tokenBuffer, tokenOffset, tokenLength)
                    .ClientId(_clientId)
                    .CorrelationId(Aeron.NULL_VALUE);

                _toDriverCommandBuffer.Commit(index);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Reject a specific image.
        /// </summary>
        /// <param name="imageCorrelationId"> of the image to be invalidated </param>
        /// <param name="position">      of the image when invalidation occurred </param>
        /// <param name="reason">        user supplied reason for invalidation, reported back to publication </param>
        /// <returns>              the correlationId of the request for invalidation. </returns>
        public long RejectImage(long imageCorrelationId, long position, string reason)
        {
            int length = RejectImageFlyweight.ComputeLength(reason);
            int index = _toDriverCommandBuffer.TryClaim(REJECT_IMAGE, length);

            if (index < 0)
            {
                throw new AeronException("could not write reject image command");
            }

            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _rejectImage
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .ClientId(_clientId)
                .CorrelationId(correlationId)
                .ImageCorrelationId(imageCorrelationId)
                .Position(position)
                .Reason(reason);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }


        public override string ToString()
        {
            return "DriverProxy{" +
                   "clientId=" + _clientId +
                   '}';
        }

        internal long AddStaticCounter(int typeId, IDirectBuffer keyBuffer, int keyOffset, int keyLength,
            IDirectBuffer labelBuffer, int labelOffset, int labelLength, long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = StaticCounterMessageFlyweight.ComputeLength(keyLength, labelLength);
            int index = _toDriverCommandBuffer.TryClaim(ADD_STATIC_COUNTER, length);
            if (index < 0)
            {
                throw new AeronException("could not write add counter command");
            }

            _staticCounterMessageFlyweight
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .KeyBuffer(keyBuffer, keyOffset, keyLength)
                .LabelBuffer(labelBuffer, labelOffset, labelLength)
                .TypeId(typeId)
                .RegistrationId(registrationId)
                .CorrelationId(correlationId)
                .ClientId(_clientId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }

        internal long AddStaticCounter(int typeId, string label, long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            int length = StaticCounterMessageFlyweight.ComputeLength(0, label.Length);
            int index = _toDriverCommandBuffer.TryClaim(ADD_STATIC_COUNTER, length);
            if (index < 0)
            {
                throw new AeronException("could not write add counter command");
            }

            _staticCounterMessageFlyweight
                .Wrap(_toDriverCommandBuffer.Buffer(), index)
                .KeyBuffer(null, 0, 0)
                .Label(label)
                .TypeId(typeId)
                .RegistrationId(registrationId)
                .CorrelationId(correlationId)
                .ClientId(_clientId);

            _toDriverCommandBuffer.Commit(index);

            return correlationId;
        }
    }
}