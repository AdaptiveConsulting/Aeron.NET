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
        private const string Channel = "aeron:udp?endpoint=localhost:40124";
        private const int StreamId1 = 1002;
        private const int InitialTermId = 7;
        private const long SubscriptionCorrelationId = 100;
        private const int ReadBufferCapacity = 1024;
        private static readonly int FragmentCountLimit = int.MaxValue;
        private const int HeaderLength = DataHeaderFlyweight.HEADER_LENGTH;

        private UnsafeBuffer _atomicReadBuffer;
        private ClientConductor _conductor;
        private IFragmentHandler _fragmentHandler;
        private Image _imageOneMock;
        private Image _imageTwoMock;
        private Header _header = new Header(
            InitialTermId,
            LogBufferDescriptor.PositionBitsToShift(LogBufferDescriptor.TERM_MIN_LENGTH)
        );
        private AvailableImageHandler _availableImageHandler;
        private UnavailableImageHandler _unavailableImageHandler;

        private readonly UnsafeBuffer _tempBuffer = new UnsafeBuffer(new byte[1024]);
        private CountersManager _countersManager = Tests.NewCountersManager(16 * 1024);

        private Subscription _subscription;

        [SetUp]
        public void Setup()
        {
            _imageOneMock = A.Fake<Image>();
            _imageTwoMock = A.Fake<Image>();

            A.CallTo(() => _imageOneMock.CorrelationId).Returns(1);
            A.CallTo(() => _imageTwoMock.CorrelationId).Returns(2);

            _atomicReadBuffer = new UnsafeBuffer(new byte[ReadBufferCapacity]);
            _conductor = A.Fake<ClientConductor>();
            _fragmentHandler = A.Fake<IFragmentHandler>();
            _availableImageHandler = A.Fake<AvailableImageHandler>();
            _unavailableImageHandler = A.Fake<UnavailableImageHandler>();

            A.CallTo(() => _conductor.CountersReader()).Returns(_countersManager);

            _subscription = new Subscription(
                _conductor,
                Channel,
                StreamId1,
                SubscriptionCorrelationId,
                _availableImageHandler,
                _unavailableImageHandler
            );

            A.CallTo(() => _conductor.RemoveSubscription(_subscription))
                .Invokes(() => _subscription.InternalClose(Aeron.NULL_VALUE));
        }

        [Test]
        public void ShouldEnsureTheSubscriptionIsOpenWhenPolling()
        {
            _subscription.Dispose();
            Assert.True(_subscription.IsClosed);

            A.CallTo(() => _conductor.RemoveSubscription(_subscription)).MustHaveHappened();
        }

        [Test]
        public void ShouldReadNothingWhenNoImages()
        {
            Assert.AreEqual(0, _subscription.Poll(_fragmentHandler, 1));
        }

        [Test]
        public void ShouldReadNothingWhenThereIsNoData()
        {
            _subscription.AddImage(_imageOneMock);

            A.CallTo(() => _imageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).Returns(0);

            Assert.AreEqual(0, _subscription.Poll(_fragmentHandler, 1));
        }

        [Test]
        public void ShouldReadData()
        {
            _subscription.AddImage(_imageOneMock);

            A.CallTo(() => _imageOneMock.Poll(A<IFragmentHandler>._, A<int>._))
                .ReturnsLazily(o =>
                {
                    var handler = (IFragmentHandler)o.Arguments[0];
                    handler.OnFragment(_atomicReadBuffer, HeaderLength, ReadBufferCapacity - HeaderLength, _header);
                    return 1;
                });

            Assert.AreEqual(1, _subscription.Poll(_fragmentHandler, FragmentCountLimit));

            A.CallTo(() =>
                    _fragmentHandler.OnFragment(
                        _atomicReadBuffer,
                        HeaderLength,
                        ReadBufferCapacity - HeaderLength,
                        A<Header>._
                    )
                )
                .MustHaveHappened();
        }

        [Test]
        public void ShouldReadDataFromMultipleSources()
        {
            _subscription.AddImage(_imageOneMock);
            _subscription.AddImage(_imageTwoMock);

            A.CallTo(() => _imageOneMock.Poll(A<IFragmentHandler>._, A<int>._))
                .ReturnsLazily(o =>
                {
                    var handler = (IFragmentHandler)o.Arguments[0];
                    handler.OnFragment(_atomicReadBuffer, HeaderLength, ReadBufferCapacity - HeaderLength, _header);
                    return 1;
                });

            A.CallTo(() => _imageTwoMock.Poll(A<IFragmentHandler>._, A<int>._))
                .ReturnsLazily(o =>
                {
                    var handler = (IFragmentHandler)o.Arguments[0];
                    handler.OnFragment(_atomicReadBuffer, HeaderLength, ReadBufferCapacity - HeaderLength, _header);
                    return 1;
                });

            Assert.AreEqual(2, _subscription.Poll(_fragmentHandler, FragmentCountLimit));
        }
    }
}
