using System;
using Adaptive.Aeron.Command;
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
        /// Maximum capacity of the write buffer </summary>
        public const int MSG_BUFFER_CAPACITY = 1024;

        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(BufferUtil.AllocateDirectAligned(MSG_BUFFER_CAPACITY,BitUtil.CACHE_LINE_LENGTH * 2));
        private readonly PublicationMessageFlyweight _publicationMessage = new PublicationMessageFlyweight();
        private readonly SubscriptionMessageFlyweight _subscriptionMessage = new SubscriptionMessageFlyweight();
        private readonly RemoveMessageFlyweight _removeMessage = new RemoveMessageFlyweight();
        private readonly CorrelatedMessageFlyweight _correlatedMessage = new CorrelatedMessageFlyweight();
        private readonly IRingBuffer _toDriverCommandBuffer;

        public DriverProxy(IRingBuffer toDriverCommandBuffer)
        {
            if (toDriverCommandBuffer == null) throw new ArgumentNullException(nameof(toDriverCommandBuffer));

            _toDriverCommandBuffer = toDriverCommandBuffer;

            _publicationMessage.Wrap(_buffer, 0);
            _subscriptionMessage.Wrap(_buffer, 0);

            _correlatedMessage.Wrap(_buffer, 0);
            _removeMessage.Wrap(_buffer, 0);

            var clientId = toDriverCommandBuffer.NextCorrelationId();
            _correlatedMessage.ClientId(clientId);
        }

        public long TimeOfLastDriverKeepalive()
        {
            return _toDriverCommandBuffer.ConsumerHeartbeatTime();
        }

#if DEBUG
        public virtual long AddPublication(string channel, int streamId)
#else
        public long AddPublication(string channel, int streamId)
#endif
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _publicationMessage.CorrelationId(correlationId);

            _publicationMessage.StreamId(streamId).Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_PUBLICATION, _buffer, 0, _publicationMessage.Length()))
            {
                throw new InvalidOperationException("Could not write add publication command");
            }

            return correlationId;
        }

#if DEBUG
        public virtual long RemovePublication(long registrationId)
#else
        public long RemovePublication(long registrationId)
#endif
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _removeMessage.RegistrationId(registrationId).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_PUBLICATION, _buffer, 0, RemoveMessageFlyweight.Length()))
            {
                throw new InvalidOperationException("Could not write remove publication command");
            }

            return correlationId;
        }

#if DEBUG
        public virtual long AddSubscription(string channel, int streamId)
#else
        public long AddSubscription(string channel, int streamId)
#endif
        {
            const long registrationId = -1;
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _subscriptionMessage.CorrelationId(correlationId);

            _subscriptionMessage.RegistrationCorrelationId(registrationId).StreamId(streamId).Channel(channel);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.ADD_SUBSCRIPTION, _buffer, 0, _subscriptionMessage.Length()))
            {
                throw new InvalidOperationException("Could not write add subscription command");
            }

            return correlationId;
        }

#if DEBUG
        public virtual long RemoveSubscription(long registrationId)
#else
        public long RemoveSubscription(long registrationId)
#endif
        {
            long correlationId = _toDriverCommandBuffer.NextCorrelationId();

            _removeMessage.RegistrationId(registrationId).CorrelationId(correlationId);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.REMOVE_SUBSCRIPTION, _buffer, 0, RemoveMessageFlyweight.Length()))
            {
                throw new InvalidOperationException("Could not write remove subscription message");
            }

            return correlationId;
        }

        public void SendClientKeepalive()
        {
            _correlatedMessage.CorrelationId(0);

            if (!_toDriverCommandBuffer.Write(ControlProtocolEvents.CLIENT_KEEPALIVE, _buffer, 0, CorrelatedMessageFlyweight.LENGTH))
            {
                throw new InvalidOperationException("Could not send client keepalive command");
            }
        }
    }
}