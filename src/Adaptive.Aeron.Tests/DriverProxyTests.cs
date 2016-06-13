using System;
using Adaptive.Aeron.Command;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.RingBuffer;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class DriverProxyTest
    {
        [SetUp]
        public void Setup()
        {
            conductorBuffer = new ManyToOneRingBuffer(new UnsafeBuffer(new byte[RingBufferDescriptor.TrailerLength + 1024]));
            conductor = new DriverProxy(conductorBuffer);
        }
        
        public const string CHANNEL = "aeron:udp?interface=localhost:40123|endpoint=localhost:40124";

        private const int STREAM_ID = 1;
        private const long CORRELATION_ID = 3;
        private IRingBuffer conductorBuffer;
        private DriverProxy conductor;

        [Test]
        public void ThreadSendsAddChannelMessage()
        {
            ThreadSendsChannelMessage(() => conductor.AddPublication(CHANNEL, STREAM_ID), ControlProtocolEvents.ADD_PUBLICATION);
        }

        [Test]
        public void ThreadSendsRemoveChannelMessage()
        {
            conductor.RemovePublication(CORRELATION_ID);
            AssertReadsOneMessage((msgTypeId, buffer, index, length) =>
            {
                RemoveMessageFlyweight message = new RemoveMessageFlyweight();
                message.Wrap(buffer, index);
                Assert.That(msgTypeId, Is.EqualTo(ControlProtocolEvents.REMOVE_PUBLICATION));
                Assert.That(message.RegistrationId(), Is.EqualTo(CORRELATION_ID));
            });
        }

        private void ThreadSendsChannelMessage(Action sendMessage, int expectedMsgTypeId)
        {
            sendMessage();

            AssertReadsOneMessage((msgTypeId, buffer, index, length) =>
            {
                PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
                publicationMessage.Wrap(buffer, index);
                Assert.That(msgTypeId, Is.EqualTo(expectedMsgTypeId));
                Assert.That(publicationMessage.Channel(), Is.EqualTo(CHANNEL));
                Assert.That(publicationMessage.StreamId(), Is.EqualTo(STREAM_ID));
            });
        }

        [Test]
        public void ThreadSendsRemoveSubscriberMessage()
        {
            conductor.RemoveSubscription(CORRELATION_ID);

            AssertReadsOneMessage((msgTypeId, buffer, index, length) =>
            {
                RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
                removeMessage.Wrap(buffer, index);
                Assert.That(msgTypeId, Is.EqualTo(ControlProtocolEvents.REMOVE_SUBSCRIPTION));
                Assert.That(removeMessage.RegistrationId(), Is.EqualTo(CORRELATION_ID));
            });
        }

        private void AssertReadsOneMessage(MessageHandler handler)
        {
            int messageCount = conductorBuffer.Read(handler);
            Assert.That(messageCount, Is.EqualTo(1));
        }
    }

}