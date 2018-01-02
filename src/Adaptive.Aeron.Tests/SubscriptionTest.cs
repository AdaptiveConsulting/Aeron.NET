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

using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class SubscriptionTest
    {
        private const string CHANNEL = "aeron:udp?endpoint=localhost:40124";
        private const int STREAM_ID_1 = 2;
        private const long SUBSCRIPTION_CORRELATION_ID = 100;
        private const int READ_BUFFER_CAPACITY = 1024;
        private static readonly byte FLAGS = FrameDescriptor.UNFRAGMENTED;
        private static readonly int FRAGMENT_COUNT_LIMIT = int.MaxValue;
        private const int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

        private UnsafeBuffer AtomicReadBuffer;
        private ClientConductor Conductor;
        private FragmentHandler FragmentHandler;
        private Image ImageOneMock;
        private Header Header;
        private Image ImageTwoMock;
        private AvailableImageHandler AvailableImageHandler;
        private UnavailableImageHandler UnavailableImageHandler;

        private Subscription Subscription;

        [SetUp]
        public void Setup()
        {
            ImageOneMock = A.Fake<Image>();
            ImageTwoMock = A.Fake<Image>();
            
            A.CallTo(() => ImageOneMock.CorrelationId).Returns(1);
            A.CallTo(() => ImageTwoMock.CorrelationId).Returns(2);

            AtomicReadBuffer = new UnsafeBuffer(new byte[READ_BUFFER_CAPACITY]);
            Conductor = A.Fake<ClientConductor>();
            FragmentHandler = A.Fake<FragmentHandler>();
            Header = A.Fake<Header>();
            AvailableImageHandler = A.Fake<AvailableImageHandler>();
            UnavailableImageHandler = A.Fake<UnavailableImageHandler>();

            A.CallTo(() => Header.Flags).Returns(FLAGS);

            Subscription = new Subscription(
                Conductor,
                CHANNEL,
                STREAM_ID_1,
                SUBSCRIPTION_CORRELATION_ID,
                AvailableImageHandler,
                UnavailableImageHandler);
            
            A.CallTo(() => Conductor.ReleaseSubscription(Subscription)).Invokes(() => Subscription.InternalClose());
        }

        [Test]
        public void ShouldEnsureTheSubscriptionIsOpenWhenPolling()
        {
            Subscription.Dispose();
            Assert.True(Subscription.Closed);

            A.CallTo(() => Conductor.ReleaseSubscription(Subscription)).MustHaveHappened();
        }

        [Test]
        public void ShouldReadNothingWhenNoImages()
        {
            Assert.AreEqual(Subscription.Poll(FragmentHandler, 1), 0);
        }

        [Test]
        public void ShouldReadNothingWhenThereIsNoData()
        {
            Subscription.AddImage(ImageOneMock);


            A.CallTo(() => ImageOneMock.Poll(A<FragmentHandler>._, A<int>._)).Returns(0);

            Assert.AreEqual(Subscription.Poll(FragmentHandler, 1), 0);
        }

        [Test]
        public void ShouldReadData()
        {
            Subscription.AddImage(ImageOneMock);

            A.CallTo(() => ImageOneMock.Poll(A<FragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (FragmentHandler) o.Arguments[0];
                handler(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            Assert.AreEqual(Subscription.Poll(FragmentHandler, FRAGMENT_COUNT_LIMIT), 1);

            A.CallTo(() => FragmentHandler(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public void ShouldReadDataFromMultipleSources()
        {
            Subscription.AddImage(ImageOneMock);
            Subscription.AddImage(ImageTwoMock);

            A.CallTo(() => ImageOneMock.Poll(A<FragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (FragmentHandler) o.Arguments[0];
                handler(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            A.CallTo(() => ImageTwoMock.Poll(A<FragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (FragmentHandler) o.Arguments[0];
                handler(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            Assert.AreEqual(Subscription.Poll(FragmentHandler, FRAGMENT_COUNT_LIMIT), 2);
        }
    }
}