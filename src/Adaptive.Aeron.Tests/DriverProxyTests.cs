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
using Adaptive.Agrona;
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
            _conductorBuffer = new ManyToOneRingBuffer(
                new UnsafeBuffer(BufferUtil.AllocateDirect(RingBufferDescriptor.TrailerLength + 1024))
            );
            _conductor = new DriverProxy(_conductorBuffer, ClientId);
        }

        public const string Channel = "aeron:udp?interface=localhost:40123|endpoint=localhost:40124";

        private const int StreamId = 1001;
        private const long CorrelationId = 3;
        private const long ClientId = 7;
        private IRingBuffer _conductorBuffer;
        private DriverProxy _conductor;

        [Test]
        public void ThreadSendsAddChannelMessage()
        {
            ThreadSendsChannelMessage(
                () => _conductor.AddPublication(Channel, StreamId),
                ControlProtocolEvents.ADD_PUBLICATION
            );
        }

        [Test]
        public void ThreadSendsRemoveChannelMessage()
        {
            _conductor.RemovePublication(CorrelationId, false);
            AssertReadsOneMessage(
                (msgTypeId, buffer, index, length) =>
                {
                    RemovePublicationFlyweight message = new RemovePublicationFlyweight();
                    message.Wrap(buffer, index);
                    Assert.AreEqual(ControlProtocolEvents.REMOVE_PUBLICATION, msgTypeId);
                    Assert.AreEqual(CorrelationId, message.RegistrationId());
                }
            );
        }

        private void ThreadSendsChannelMessage(Action sendMessage, int expectedMsgTypeId)
        {
            sendMessage();

            AssertReadsOneMessage(
                (msgTypeId, buffer, index, length) =>
                {
                    PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
                    publicationMessage.Wrap(buffer, index);
                    Assert.AreEqual(expectedMsgTypeId, msgTypeId);
                    Assert.AreEqual(Channel, publicationMessage.Channel());
                    Assert.AreEqual(StreamId, publicationMessage.StreamId());
                }
            );
        }

        [Test]
        public void ThreadSendsRemoveSubscriberMessage()
        {
            _conductor.RemoveSubscription(CorrelationId);

            AssertReadsOneMessage(
                (msgTypeId, buffer, index, length) =>
                {
                    RemoveSubscriptionFlyweight removeMessage = new RemoveSubscriptionFlyweight();
                    removeMessage.Wrap(buffer, index);
                    Assert.AreEqual(ControlProtocolEvents.REMOVE_SUBSCRIPTION, msgTypeId);
                    Assert.AreEqual(CorrelationId, removeMessage.RegistrationId());
                }
            );
        }

        private void AssertReadsOneMessage(MessageHandler handler)
        {
            int messageCount = _conductorBuffer.Read(handler);
            Assert.AreEqual(1, messageCount);
        }
    }
}
