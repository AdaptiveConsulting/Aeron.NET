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

using System;
using Adaptive.Aeron.Command;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using Adaptive.Agrona.Util;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class ClientConductorTest
    {
        private static readonly int TermBufferLength = LogBufferDescriptor.TERM_MIN_LENGTH;

        private const int SessionId1 = 13;
        private const int SessionId2 = 15;

        private const string Channel = "aeron:udp?endpoint=localhost:40124";
        private const int StreamId1 = 1002;
        private const int StreamId2 = 1004;
        private const int SendBufferCapacity = 1024;
        private const int CounterBufferLength = 512 * 1024;

        private const long CorrelationId = 2000;
        private const long CorrelationId2 = 2002;
        private const long CloseCorrelationId = 2001;
        private const long UnknownCorrelationId = 3000;

        private static readonly long KeepAliveInterval = NanoUtil.FromMilliseconds(500);
        private const long AwaitTimeout = 100;
        private const long InterServiceTimeoutMs = 1000;

        private const int SubscriptionPositionId = 2;
        private const int SubscriptionPositionRegistrationId = 4001;

        private const string SourceInfo = "127.0.0.1:40789";

        private PublicationBuffersReadyFlyweight _publicationReady;
        private SubscriptionReadyFlyweight _subscriptionReady;
        private OperationSucceededFlyweight _operationSuccess;
        private ErrorResponseFlyweight _errorResponse;
        private ClientTimeoutFlyweight _clientTimeout;

        private UnsafeBuffer _publicationReadyBuffer;
        private UnsafeBuffer _subscriptionReadyBuffer;
        private UnsafeBuffer _operationSuccessBuffer;
        private UnsafeBuffer _errorMessageBuffer;
        private UnsafeBuffer _clientTimeoutBuffer;

        private CopyBroadcastReceiver _mockToClientReceiver;

        private UnsafeBuffer _counterValuesBuffer;
        private UnsafeBuffer _counterMetaDataBuffer;

        private readonly TestEpochClock _epochClock = new TestEpochClock();
        private readonly TestNanoClock _nanoClock = new TestNanoClock();
        private IErrorHandler _mockClientErrorHandler;

        private ClientConductor _conductor;
        private DriverProxy _driverProxy;
        private AvailableImageHandler _mockAvailableImageHandler;
        private UnavailableImageHandler _mockUnavailableImageHandler;
        private Action _mockCloseHandler;
        private ILogBuffersFactory _logBuffersFactory;
        private ILock _mockClientLock = A.Fake<ILock>();
        private Aeron _mockAeron;

        private bool _suppressPrintError = false;

        private class PrintingErrorHandler : IErrorHandler
        {
            private readonly ClientConductorTest _test;

            public PrintingErrorHandler(ClientConductorTest test)
            {
                _test = test;
            }

            public void OnError(Exception throwable)
            {
                if (!_test._suppressPrintError)
                {
                    Console.WriteLine(throwable.ToString());
                    Console.Write(throwable.StackTrace);
                }
            }
        }

        [SetUp]
        public void SetUp()
        {
            _mockClientErrorHandler = A.Fake<IErrorHandler>(options =>
                options.Wrapping(new PrintingErrorHandler(this))
            );

            _publicationReady = new PublicationBuffersReadyFlyweight();
            _subscriptionReady = new SubscriptionReadyFlyweight();
            _operationSuccess = new OperationSucceededFlyweight();
            _errorResponse = new ErrorResponseFlyweight();
            _clientTimeout = new ClientTimeoutFlyweight();

            _publicationReadyBuffer = new UnsafeBuffer(new byte[SendBufferCapacity]);
            _subscriptionReadyBuffer = new UnsafeBuffer(new byte[SendBufferCapacity]);
            _operationSuccessBuffer = new UnsafeBuffer(new byte[SendBufferCapacity]);
            _errorMessageBuffer = new UnsafeBuffer(new byte[SendBufferCapacity]);
            _clientTimeoutBuffer = new UnsafeBuffer(new byte[SendBufferCapacity]);

            _counterValuesBuffer = new UnsafeBuffer(new byte[CounterBufferLength]);
            _counterMetaDataBuffer = new UnsafeBuffer(new byte[CounterBufferLength]);

            _mockToClientReceiver = A.Fake<CopyBroadcastReceiver>();

            _mockAvailableImageHandler = A.Fake<AvailableImageHandler>();
            _mockUnavailableImageHandler = A.Fake<UnavailableImageHandler>();
            _mockCloseHandler = A.Fake<Action>();

            _logBuffersFactory = A.Fake<ILogBuffersFactory>();

            _driverProxy = A.Fake<DriverProxy>();

            _mockAeron = A.Fake<Aeron>();

            A.CallTo(() => _mockClientLock.TryLock()).Returns(true);

            A.CallTo(() => _driverProxy.AddPublication(Channel, StreamId1)).Returns(CorrelationId);
            A.CallTo(() => _driverProxy.AddPublication(Channel, StreamId2)).Returns(CorrelationId2);
            A.CallTo(() => _driverProxy.RemovePublication(CorrelationId, false)).Returns(CloseCorrelationId);
            A.CallTo(() => _driverProxy.AddSubscription(A<string>._, A<int>._)).Returns(CorrelationId);
            A.CallTo(() => _driverProxy.RemoveSubscription(CorrelationId)).Returns(CloseCorrelationId);

            Aeron.Context ctx = new Aeron.Context()
                .ClientLock(_mockClientLock)
                .EpochClock(_epochClock)
                .NanoClock(_nanoClock)
                .AwaitingIdleStrategy(new NoOpIdleStrategy())
                .ToClientBuffer(_mockToClientReceiver)
                .DriverProxy(_driverProxy)
                .LogBuffersFactory(_logBuffersFactory)
                .ErrorHandler(_mockClientErrorHandler)
                .AvailableImageHandler(_mockAvailableImageHandler)
                .UnavailableImageHandler(_mockUnavailableImageHandler)
                .CloseHandler(_mockCloseHandler)
                .KeepAliveIntervalNs(KeepAliveInterval)
                .DriverTimeoutMs(AwaitTimeout)
                .InterServiceTimeoutNs(InterServiceTimeoutMs * 1000000)
                .CountersValuesBuffer(_counterValuesBuffer)
                .CountersMetaDataBuffer(_counterMetaDataBuffer);

            _conductor = new ClientConductor(ctx, _mockAeron);

            _publicationReady.Wrap(_publicationReadyBuffer, 0);
            _subscriptionReady.Wrap(_subscriptionReadyBuffer, 0);
            _operationSuccess.Wrap(_operationSuccessBuffer, 0);
            _errorResponse.Wrap(_errorMessageBuffer, 0);
            _clientTimeout.Wrap(_clientTimeoutBuffer, 0);

            _publicationReady.CorrelationId(CorrelationId);
            _publicationReady.RegistrationId(CorrelationId);
            _publicationReady.SessionId(SessionId1);
            _publicationReady.StreamId(StreamId1);
            _publicationReady.LogFileName(SessionId1 + "-log");

            _operationSuccess.CorrelationId(CloseCorrelationId);

            var termBuffersSession1 = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
            var termBuffersSession2 = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                termBuffersSession1[i] = new UnsafeBuffer(new byte[TermBufferLength]);
                termBuffersSession2[i] = new UnsafeBuffer(new byte[TermBufferLength]);
            }

            UnsafeBuffer logMetaDataSession1 = new UnsafeBuffer(new byte[TermBufferLength]);
            UnsafeBuffer logMetaDataSession2 = new UnsafeBuffer(new byte[TermBufferLength]);

            IMutableDirectBuffer header1 = DataHeaderFlyweight.CreateDefaultHeader(SessionId1, StreamId1, 0);
            IMutableDirectBuffer header2 = DataHeaderFlyweight.CreateDefaultHeader(SessionId2, StreamId2, 0);

            LogBufferDescriptor.StoreDefaultFrameHeader(logMetaDataSession1, header1);
            LogBufferDescriptor.StoreDefaultFrameHeader(logMetaDataSession2, header2);

            var logBuffersSession1 = A.Fake<LogBuffers>();
            var logBuffersSession2 = A.Fake<LogBuffers>();

            A.CallTo(() => _logBuffersFactory.Map(SessionId1 + "-log")).Returns(logBuffersSession1);
            A.CallTo(() => _logBuffersFactory.Map(SessionId2 + "-log")).Returns(logBuffersSession2);

            A.CallTo(() => logBuffersSession1.DuplicateTermBuffers()).Returns(termBuffersSession1);
            A.CallTo(() => logBuffersSession2.DuplicateTermBuffers()).Returns(termBuffersSession2);

            A.CallTo(() => logBuffersSession1.MetaDataBuffer()).Returns(logMetaDataSession1);
            A.CallTo(() => logBuffersSession2.MetaDataBuffer()).Returns(logMetaDataSession2);

            A.CallTo(() => logBuffersSession1.TermLength()).Returns(TermBufferLength);
            A.CallTo(() => logBuffersSession2.TermLength()).Returns(TermBufferLength);
        }

        // --------------------------------
        // Publication related interactions
        // --------------------------------

        [Test]
        public void AddPublicationShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer => _publicationReady.Length()
            );

            _conductor.AddPublication(Channel, StreamId1);

            A.CallTo(() => _driverProxy.AddPublication(Channel, StreamId1)).MustHaveHappened();
        }

        [Test]
        public void AddPublicationShouldMapLogFile()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer => _publicationReady.Length()
            );

            _conductor.AddPublication(Channel, StreamId1);

            A.CallTo(() => _logBuffersFactory.Map(SessionId1 + "-log")).MustHaveHappened();
        }

        [Test]
        [Timeout(5000)]
        public void AddPublicationShouldTimeoutWithoutReadyMessage()
        {
            Assert.Throws(typeof(DriverTimeoutException), () => _conductor.AddPublication(Channel, StreamId1));
        }

        [Test]
        public void ClosingPublicationShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer => _publicationReady.Length()
            );

            var publication = _conductor.AddPublication(Channel, StreamId1);

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_OPERATION_SUCCESS,
                _operationSuccessBuffer,
                buffer => OperationSucceededFlyweight.LENGTH
            );

            publication.Dispose();

            A.CallTo(() => _driverProxy.RemovePublication(CorrelationId, false)).MustHaveHappened();
        }

        [Test]
        public void ClosingPublicationShouldPurgeCache()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer => _publicationReady.Length()
            );

            var firstPublication = _conductor.AddPublication(Channel, StreamId1);

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_OPERATION_SUCCESS,
                _operationSuccessBuffer,
                buffer => OperationSucceededFlyweight.LENGTH
            );

            firstPublication.Dispose();

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer => _publicationReady.Length()
            );

            var secondPublication = _conductor.AddPublication(Channel, StreamId1);

            Assert.AreNotSame(firstPublication, secondPublication);
        }

        [Test]
        public void ShouldFailToAddPublicationOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_ERROR,
                _errorMessageBuffer,
                buffer =>
                {
                    _errorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                    _errorResponse.ErrorMessage("invalid channel");
                    _errorResponse.OffendingCommandCorrelationId(CorrelationId);
                    return _errorResponse.Length();
                }
            );

            Assert.Throws(typeof(RegistrationException), () => _conductor.AddPublication(Channel, StreamId1));
        }

        [Test]
        public void ClosingPublicationDoesNotRemoveOtherPublications()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer => _publicationReady.Length()
            );

            var publication = _conductor.AddPublication(Channel, StreamId1);

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer =>
                {
                    _publicationReady.StreamId(StreamId2);
                    _publicationReady.SessionId(SessionId2);
                    _publicationReady.LogFileName(SessionId2 + "-log");
                    _publicationReady.CorrelationId(CorrelationId2);
                    _publicationReady.RegistrationId(CorrelationId2);
                    return _publicationReady.Length();
                }
            );

            _conductor.AddPublication(Channel, StreamId2);

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_OPERATION_SUCCESS,
                _operationSuccessBuffer,
                buffer => OperationSucceededFlyweight.LENGTH
            );

            publication.Dispose();

            A.CallTo(() => _driverProxy.RemovePublication(CorrelationId, false)).MustHaveHappened();
            A.CallTo(() => _driverProxy.RemovePublication(CorrelationId2, false)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldNotMapBuffersForUnknownCorrelationId()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer =>
                {
                    _publicationReady.CorrelationId(UnknownCorrelationId);
                    _publicationReady.RegistrationId(UnknownCorrelationId);
                    return _publicationReady.Length();
                }
            );

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_PUBLICATION_READY,
                _publicationReadyBuffer,
                buffer =>
                {
                    _publicationReady.CorrelationId(CorrelationId);
                    return _publicationReady.Length();
                }
            );

            var publication = _conductor.AddPublication(Channel, StreamId1);
            _conductor.DoWork();

            A.CallTo(() => _logBuffersFactory.Map(A<string>._)).MustHaveHappened(1, Times.Exactly);
            Assert.AreEqual(publication.RegistrationId, CorrelationId);
        }

        // ---------------------------------
        // Subscription related interactions
        // ---------------------------------

        [Test]
        public void AddSubscriptionShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_SUBSCRIPTION_READY,
                _subscriptionReadyBuffer,
                buffer =>
                {
                    _subscriptionReady.CorrelationId(CorrelationId);
                    return SubscriptionReadyFlyweight.LENGTH;
                }
            );

            _conductor.AddSubscription(Channel, StreamId1);

            A.CallTo(() => _driverProxy.AddSubscription(Channel, StreamId1)).MustHaveHappened();
        }

        [Test]
        public void ClosingSubscriptionShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_SUBSCRIPTION_READY,
                _subscriptionReadyBuffer,
                buffer =>
                {
                    _subscriptionReady.CorrelationId(CorrelationId);
                    return SubscriptionReadyFlyweight.LENGTH;
                }
            );

            var subscription = _conductor.AddSubscription(Channel, StreamId1);

            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_OPERATION_SUCCESS,
                _operationSuccessBuffer,
                buffer =>
                {
                    _operationSuccess.CorrelationId(CloseCorrelationId);
                    return OperationSucceededFlyweight.LENGTH;
                }
            );

            subscription.Dispose();

            A.CallTo(() => _driverProxy.RemoveSubscription(CorrelationId)).MustHaveHappened();
        }

        [Test]
        [Timeout(5000)]
        public void AddSubscriptionShouldTimeoutWithoutOperationSuccessful()
        {
            Assert.Throws(typeof(DriverTimeoutException), () => _conductor.AddSubscription(Channel, StreamId1));
        }

        [Test]
        public void ShouldFailToAddSubscriptionOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_ERROR,
                _errorMessageBuffer,
                buffer =>
                {
                    _errorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                    _errorResponse.ErrorMessage("invalid channel");
                    _errorResponse.OffendingCommandCorrelationId(CorrelationId);
                    return _errorResponse.Length();
                }
            );

            Assert.Throws(typeof(RegistrationException), () => _conductor.AddSubscription(Channel, StreamId1));
        }

        [Test]
        public void ClientNotifiedOfNewImageShouldMapLogFile()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_SUBSCRIPTION_READY,
                _subscriptionReadyBuffer,
                buffer =>
                {
                    _subscriptionReady.CorrelationId(CorrelationId);
                    return SubscriptionReadyFlyweight.LENGTH;
                }
            );

            Subscription subscription = _conductor.AddSubscription(Channel, StreamId1);

            _conductor.OnAvailableImage(
                CorrelationId,
                SessionId1,
                subscription.RegistrationId,
                SubscriptionPositionId,
                SessionId1 + "-log",
                SourceInfo
            );

            A.CallTo(() => _logBuffersFactory.Map(SessionId1 + "-log")).MustHaveHappened();
        }

        [Test]
        public void ClientNotifiedOfNewAndInactiveImages()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_SUBSCRIPTION_READY,
                _subscriptionReadyBuffer,
                buffer =>
                {
                    _subscriptionReady.CorrelationId(CorrelationId);
                    return SubscriptionReadyFlyweight.LENGTH;
                }
            );

            var subscription = _conductor.AddSubscription(Channel, StreamId1);

            _conductor.OnAvailableImage(
                CorrelationId,
                SessionId1,
                subscription.RegistrationId,
                SubscriptionPositionId,
                SessionId1 + "-log",
                SourceInfo
            );

            Assert.False(subscription.HasNoImages);
            Assert.True(subscription.IsConnected);

            A.CallTo(() => _mockAvailableImageHandler(A<Image>._)).MustHaveHappened();

            _conductor.OnUnavailableImage(CorrelationId, subscription.RegistrationId);

            A.CallTo(() => _mockUnavailableImageHandler(A<Image>._)).MustHaveHappened();

            Assert.True(subscription.HasNoImages);
            Assert.False(subscription.IsConnected);
        }

        [Test]
        public void ShouldIgnoreUnknownNewImage()
        {
            _conductor.OnAvailableImage(
                CorrelationId2,
                SessionId2,
                SubscriptionPositionRegistrationId,
                SubscriptionPositionId,
                SessionId2 + "-log",
                SourceInfo
            );

            A.CallTo(() => _logBuffersFactory.Map(A<string>._)).MustNotHaveHappened();
            A.CallTo(() => _mockAvailableImageHandler(A<Image>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldIgnoreUnknownInactiveImage()
        {
            _conductor.OnUnavailableImage(CorrelationId2, SubscriptionPositionRegistrationId);

            A.CallTo(() => _logBuffersFactory.Map(A<string>._)).MustNotHaveHappened();
            A.CallTo(() => _mockAvailableImageHandler(A<Image>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldTimeoutInterServiceIfTooLongBetweenDoWorkCalls()
        {
            _suppressPrintError = true;

            _conductor.DoWork();
            _nanoClock.AdvanceMillis(InterServiceTimeoutMs);
            _nanoClock.AdvancedNanos(1);
            _conductor.DoWork();

            A.CallTo(() => _mockClientErrorHandler.OnError(A<ConductorServiceTimeoutException>._)).MustHaveHappened();

            Assert.True(_conductor.IsTerminating());
        }

        [Test]
        public void ShouldTerminateAndErrorOnClientTimeoutFromDriver()
        {
            _suppressPrintError = true;

            _conductor.OnClientTimeout();

            A.CallTo(() => _mockClientErrorHandler.OnError(A<Exception>._)).MustHaveHappened();

            bool threwException = false;
            try
            {
                _conductor.DoWork();
            }
            catch (AgentTerminationException)
            {
                threwException = true;
            }

            Assert.True(threwException);
            Assert.True(_conductor.IsTerminating());

            _conductor.OnClose();
            A.CallTo(() => _mockCloseHandler.Invoke()).MustHaveHappened();
        }

        [Test]
        public void ShouldNotCloseAndErrorOnClientTimeoutForAnotherClientIdFromDriver()
        {
            WhenReceiveBroadcastOnMessage(
                ControlProtocolEvents.ON_CLIENT_TIMEOUT,
                _clientTimeoutBuffer,
                buffer =>
                {
                    _clientTimeout.ClientId(_conductor.DriverListenerAdapter().ClientId + 1);
                    return ClientTimeoutFlyweight.LENGTH;
                }
            );

            _conductor.DoWork();

            A.CallTo(() => _mockClientErrorHandler.OnError(A<Exception>._)).MustNotHaveHappened();

            Assert.False(_conductor.IsClosed());
        }

        private void WhenReceiveBroadcastOnMessage(
            int msgTypeId,
            IMutableDirectBuffer buffer,
            Func<IMutableDirectBuffer, int> filler
        )
        {
            A.CallTo(() => _mockToClientReceiver.Receive(A<MessageHandler>._))
                .Invokes(() =>
                {
                    var length = filler(buffer);
                    _conductor.DriverListenerAdapter().OnMessage(msgTypeId, buffer, 0, length);
                });
        }
    }

    class TestEpochClock : IEpochClock
    {
        private int _time;

        public long Time()
        {
            return _time += 10;
        }
    }

    class TestNanoClock : INanoClock
    {
        private long _time;

        public void AdvanceMillis(long durationMs)
        {
            _time += (durationMs * 10000000);
        }

        public void AdvancedNanos(long durationNs)
        {
            _time += durationNs;
        }

        public long NanoTime()
        {
            return _time += 10000000;
        }
    }
}
