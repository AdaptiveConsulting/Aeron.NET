using System;
using Adaptive.Aeron.Command;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.RingBuffer;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Separates the concern of communicating with the client conductor away from the rest of the client.
    /// 
    /// Writes messages into the client conductor buffer.
    /// </summary>
    public class DriverProxy
    {
        /// <summary>
        /// Maximum capacity of the write buffer </summary>
        public const int MSG_BUFFER_CAPACITY = 4096;

        private readonly long _clientId;
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(new byte[MSG_BUFFER_CAPACITY]);
        private readonly PublicationMessageFlyweight _publicationMessage = new PublicationMessageFlyweight();
        private readonly SubscriptionMessageFlyweight _subscriptionMessage = new SubscriptionMessageFlyweight();

        private readonly RemoveMessageFlyweight _removeMessage = new RemoveMessageFlyweight();
        // the heartbeats come from the client conductor thread, so keep the flyweights and buffer separate
        private readonly UnsafeBuffer _keepaliveBuffer = new UnsafeBuffer(new byte[MSG_BUFFER_CAPACITY]);

        private readonly CorrelatedMessageFlyweight _correlatedMessage = new CorrelatedMessageFlyweight();
        private readonly IRingBuffer _toDriverCommandBuffer;

        public DriverProxy(IRingBuffer toDriverCommandBuffer)
        {
            if (toDriverCommandBuffer == null) throw new ArgumentNullException(nameof(toDriverCommandBuffer));

            _toDriverCommandBuffer = toDriverCommandBuffer;

            _publicationMessage.Wrap(_buffer, 0);
            _subscriptionMessage.Wrap(_buffer, 0);

            _correlatedMessage.Wrap(_keepaliveBuffer, 0);
            _removeMessage.Wrap(_buffer, 0);

            _clientId = toDriverCommandBuffer.NextCorrelationId();
        }

        public long TimeOfLastDriverKeepalive()
        {
            return _toDriverCommandBuffer.ConsumerHeartbeatTime();
        }

        public virtual long AddPublication(string channel, int streamId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _publicationMessage.ClientId(_clientId).CorrelationId(correlationId);

            _publicationMessage.StreamId(streamId).Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_PUBLICATION, _buffer, 0, _publicationMessage.Length()))
            {
                throw new InvalidOperationException("could not write publication message");
            }

            return correlationId;
        }

        public virtual long RemovePublication(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            _removeMessage.CorrelationId(correlationId);
            _removeMessage.RegistrationId(registrationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_PUBLICATION, _buffer, 0, RemoveMessageFlyweight.Length()))
            {
                throw new InvalidOperationException("could not write publication remove message");
            }

            return correlationId;
        }

        public virtual long AddSubscription(string channel, int streamId)
        {
            const long registrationId = -1;
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _subscriptionMessage.ClientId(_clientId).CorrelationId(correlationId);

            _subscriptionMessage.RegistrationCorrelationId(registrationId).StreamId(streamId).Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_SUBSCRIPTION, _buffer, 0, _subscriptionMessage.Length()))
            {
                throw new InvalidOperationException("could not write subscription message");
            }

            return correlationId;
        }

        public virtual long RemoveSubscription(long registrationId)
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();
            _removeMessage.CorrelationId(correlationId);
            _removeMessage.RegistrationId(registrationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_SUBSCRIPTION, _buffer, 0, RemoveMessageFlyweight.Length()))
            {
                throw new InvalidOperationException("could not write subscription remove message");
            }

            return correlationId;
        }

        public void SendClientKeepalive()
        {
            _correlatedMessage.ClientId(_clientId).CorrelationId(0);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.CLIENT_KEEPALIVE, _keepaliveBuffer, 0, CorrelatedMessageFlyweight.LENGTH))
            {
                throw new InvalidOperationException("could not write keepalive message");
            }
        }
    }
}