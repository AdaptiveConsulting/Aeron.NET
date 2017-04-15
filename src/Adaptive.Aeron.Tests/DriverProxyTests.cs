/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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