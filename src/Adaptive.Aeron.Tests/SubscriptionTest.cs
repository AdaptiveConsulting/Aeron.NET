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

using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class SubscriptionTest
    {
        private const string CHANNEL = "aeron:udp?endpoint=localhost:40124";
        private const int STREAM_ID_1 = 1002;
        private const int INITIAL_TERM_ID = 7;
        private const long SUBSCRIPTION_CORRELATION_ID = 100;
        private const int READ_BUFFER_CAPACITY = 1024;
        private static readonly int FRAGMENT_COUNT_LIMIT = int.MaxValue;
        private const int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

        private UnsafeBuffer AtomicReadBuffer;
        private ClientConductor Conductor;
        private IFragmentHandler FragmentHandler;
        private Image ImageOneMock;
        private Image ImageTwoMock;
        private Header Header = new Header(
            INITIAL_TERM_ID, LogBufferDescriptor.PositionBitsToShift(LogBufferDescriptor.TERM_MIN_LENGTH));
        private AvailableImageHandler AvailableImageHandler;
        private UnavailableImageHandler UnavailableImageHandler;

        private readonly UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[1024]);
        private CountersManager countersManager = Tests.NewCountersManager(16 * 1024);

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
            FragmentHandler = A.Fake<IFragmentHandler>();
            AvailableImageHandler = A.Fake<AvailableImageHandler>();
            UnavailableImageHandler = A.Fake<UnavailableImageHandler>();
            
            A.CallTo(() => Conductor.CountersReader()).Returns(countersManager);
            
            Subscription = new Subscription(
                Conductor,
                CHANNEL,
                STREAM_ID_1,
                SUBSCRIPTION_CORRELATION_ID,
                AvailableImageHandler,
                UnavailableImageHandler);
            
            A.CallTo(() => Conductor.RemoveSubscription(Subscription)).Invokes(() => Subscription.InternalClose(Aeron.NULL_VALUE));
        }

        [Test]
        public void ShouldEnsureTheSubscriptionIsOpenWhenPolling()
        {
            Subscription.Dispose();
            Assert.True(Subscription.IsClosed);

            A.CallTo(() => Conductor.RemoveSubscription(Subscription)).MustHaveHappened();
        }

        [Test]
        public void ShouldReadNothingWhenNoImages()
        {
            Assert.AreEqual(0, Subscription.Poll(FragmentHandler, 1));
        }

        [Test]
        public void ShouldReadNothingWhenThereIsNoData()
        {
            Subscription.AddImage(ImageOneMock);


            A.CallTo(() => ImageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).Returns(0);

            Assert.AreEqual(0, Subscription.Poll(FragmentHandler, 1));
        }

        [Test]
        public void ShouldReadData()
        {
            Subscription.AddImage(ImageOneMock);

            A.CallTo(() => ImageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (IFragmentHandler) o.Arguments[0];
                handler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            Assert.AreEqual(1, Subscription.Poll(FragmentHandler, FRAGMENT_COUNT_LIMIT));

            A.CallTo(() => FragmentHandler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public void ShouldReadDataFromMultipleSources()
        {
            Subscription.AddImage(ImageOneMock);
            Subscription.AddImage(ImageTwoMock);

            A.CallTo(() => ImageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (IFragmentHandler) o.Arguments[0];
                handler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            A.CallTo(() => ImageTwoMock.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (IFragmentHandler) o.Arguments[0];
                handler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            Assert.AreEqual(2, Subscription.Poll(FragmentHandler, FRAGMENT_COUNT_LIMIT));
        }
    }
}