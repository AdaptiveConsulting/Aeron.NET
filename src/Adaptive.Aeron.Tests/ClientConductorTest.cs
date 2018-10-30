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
        private static readonly int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;

        protected internal const int SESSION_ID_1 = 13;
        protected internal const int SESSION_ID_2 = 15;

        private const string CHANNEL = "aeron:udp?endpoint=localhost:40124";
        private const int STREAM_ID_1 = 2;
        private const int STREAM_ID_2 = 4;
        private const int SEND_BUFFER_CAPACITY = 1024;
        private const int COUNTER_BUFFER_LENGTH = 1024;

        private const long CORRELATION_ID = 2000;
        private const long CORRELATION_ID_2 = 2002;
        private const long CLOSE_CORRELATION_ID = 2001;
        private const long UNKNOWN_CORRELATION_ID = 3000;

        private static readonly long KEEP_ALIVE_INTERVAL = NanoUtil.FromMilliseconds(500);
        private const long AWAIT_TIMEOUT = 100;
        private const long INTER_SERVICE_TIMEOUT_MS = 1000;
        
        private const int SUBSCRIPTION_POSITION_ID = 2;
        private const int SUBSCRIPTION_POSITION_REGISTRATION_ID = 4001;

        private const string SOURCE_INFO = "127.0.0.1:40789";

        private PublicationBuffersReadyFlyweight PublicationReady;
        private SubscriptionReadyFlyweight SubscriptionReady;
        private OperationSucceededFlyweight OperationSuccess;
        private ErrorResponseFlyweight ErrorResponse;

        private UnsafeBuffer PublicationReadyBuffer;
        private UnsafeBuffer SubscriptionReadyBuffer;
        private UnsafeBuffer OperationSuccessBuffer;
        private UnsafeBuffer ErrorMessageBuffer;

        private CopyBroadcastReceiver MockToClientReceiver;

        private UnsafeBuffer CounterValuesBuffer;

        private readonly TestEpochClock EpochClock = new TestEpochClock();
        private readonly TestNanoClock NanoClock = new TestNanoClock();
        private ErrorHandler MockClientErrorHandler;

        private DriverProxy DriverProxy;
        private ClientConductor Conductor;
        private AvailableImageHandler MockAvailableImageHandler;
        private UnavailableImageHandler MockUnavailableImageHandler;
        private ILogBuffersFactory LogBuffersFactory;
        private ILock mockClientLock = A.Fake<ILock>();
        private bool SuppressPrintError = false;


        [SetUp]
        public void SetUp()
        {
            MockClientErrorHandler = A.Fake<ErrorHandler>(options => options.Wrapping(throwable =>
            {
                if (!SuppressPrintError)
                {
                    Console.WriteLine(throwable.ToString());
                    Console.Write(throwable.StackTrace);
                }
            }));

            PublicationReady = new PublicationBuffersReadyFlyweight();
            SubscriptionReady = new SubscriptionReadyFlyweight();
            OperationSuccess = new OperationSucceededFlyweight();
            ErrorResponse = new ErrorResponseFlyweight();

            PublicationReadyBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            SubscriptionReadyBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            OperationSuccessBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            ErrorMessageBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            
            CounterValuesBuffer = new UnsafeBuffer(new byte[COUNTER_BUFFER_LENGTH]);
            MockToClientReceiver = A.Fake<CopyBroadcastReceiver>();

            MockAvailableImageHandler = A.Fake<AvailableImageHandler>();
            MockUnavailableImageHandler = A.Fake<UnavailableImageHandler>();

            LogBuffersFactory = A.Fake<ILogBuffersFactory>();
            
            DriverProxy = A.Fake<DriverProxy>();
            
            A.CallTo(() => mockClientLock.TryLock()).Returns(true);

            A.CallTo(() => DriverProxy.AddPublication(CHANNEL, STREAM_ID_1)).Returns(CORRELATION_ID);
            A.CallTo(() => DriverProxy.AddPublication(CHANNEL, STREAM_ID_2)).Returns(CORRELATION_ID_2);
            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).Returns(CLOSE_CORRELATION_ID);
            A.CallTo(() => DriverProxy.AddSubscription(A<string>._, A<int>._)).Returns(CORRELATION_ID);
            A.CallTo(() => DriverProxy.RemoveSubscription(CORRELATION_ID)).Returns(CLOSE_CORRELATION_ID);

            Aeron.Context ctx = new Aeron.Context()
                .ClientLock(mockClientLock)
                .EpochClock(EpochClock)
                .NanoClock(NanoClock)
                .ToClientBuffer(MockToClientReceiver)
                .DriverProxy(DriverProxy)
                .LogBuffersFactory(LogBuffersFactory)
                .ErrorHandler(MockClientErrorHandler)
                .AvailableImageHandler(MockAvailableImageHandler)
                .UnavailableImageHandler(MockUnavailableImageHandler)
                .KeepAliveInterval(KEEP_ALIVE_INTERVAL)
                .DriverTimeoutMs(AWAIT_TIMEOUT)
                .InterServiceTimeout(INTER_SERVICE_TIMEOUT_MS * 1000000)
                .CountersValuesBuffer(CounterValuesBuffer);


            Conductor = new ClientConductor(ctx);

            PublicationReady.Wrap(PublicationReadyBuffer, 0);
            SubscriptionReady.Wrap(SubscriptionReadyBuffer, 0);
            OperationSuccess.Wrap(OperationSuccessBuffer, 0);
            ErrorResponse.Wrap(ErrorMessageBuffer, 0);

            PublicationReady.CorrelationId(CORRELATION_ID);
            PublicationReady.RegistrationId(CORRELATION_ID);
            PublicationReady.SessionId(SESSION_ID_1);
            PublicationReady.StreamId(STREAM_ID_1);
            PublicationReady.LogFileName(SESSION_ID_1 + "-log");
            
            OperationSuccess.CorrelationId(CLOSE_CORRELATION_ID);

            var termBuffersSession1 = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
            var termBuffersSession2 = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
               termBuffersSession1[i] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
               termBuffersSession2[i] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
            }

            UnsafeBuffer logMetaDataSession1 = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
            UnsafeBuffer logMetaDataSession2 = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);

            IMutableDirectBuffer header1 = DataHeaderFlyweight.CreateDefaultHeader(SESSION_ID_1, STREAM_ID_1, 0);
            IMutableDirectBuffer header2 = DataHeaderFlyweight.CreateDefaultHeader(SESSION_ID_2, STREAM_ID_2, 0);

            LogBufferDescriptor.StoreDefaultFrameHeader(logMetaDataSession1, header1);
            LogBufferDescriptor.StoreDefaultFrameHeader(logMetaDataSession2, header2);

            var logBuffersSession1 = A.Fake<LogBuffers>();
            var logBuffersSession2 = A.Fake<LogBuffers>();

            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_1 + "-log")).Returns(logBuffersSession1);
            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_2 + "-log")).Returns(logBuffersSession2);
            
            A.CallTo(() => logBuffersSession1.DuplicateTermBuffers()).Returns(termBuffersSession1);
            A.CallTo(() => logBuffersSession2.DuplicateTermBuffers()).Returns(termBuffersSession2);

            A.CallTo(() => logBuffersSession1.MetaDataBuffer()).Returns(logMetaDataSession1);
            A.CallTo(() => logBuffersSession2.MetaDataBuffer()).Returns(logMetaDataSession2);
            
            A.CallTo(() => logBuffersSession1.TermLength()).Returns(TERM_BUFFER_LENGTH);
            A.CallTo(() => logBuffersSession2.TermLength()).Returns(TERM_BUFFER_LENGTH);
        }

        // --------------------------------
        // Publication related interactions
        // --------------------------------

        [Test]
        public void AddPublicationShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            A.CallTo(() => DriverProxy.AddPublication(CHANNEL, STREAM_ID_1)).MustHaveHappened();
        }

        [Test]
        public void AddPublicationShouldMapLogFile()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_1 + "-log")).MustHaveHappened();
        }

// Timeout not implemented in the .NetStandard version of NUnit yet
#if NETFULL
        [Test]
        [Timeout(5000)]
        [ExpectedException(typeof(DriverTimeoutException))]
        public void AddPublicationShouldTimeoutWithoutReadyMessage()
        {
            Conductor.AddPublication(CHANNEL, STREAM_ID_1);
        }
#endif

        [Test]
        public void ClosingPublicationShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, OperationSuccessBuffer, buffer => OperationSucceededFlyweight.LENGTH);

            publication.Dispose();

            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).MustHaveHappened();
        }

        [Test]
        public void ClosingPublicationShouldPurgeCache()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            var firstPublication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, OperationSuccessBuffer, buffer => OperationSucceededFlyweight.LENGTH);

            firstPublication.Dispose();

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            var secondPublication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            Assert.AreNotSame(firstPublication, secondPublication);
        }

        [Test]
        [ExpectedException(typeof(RegistrationException))]
        public void ShouldFailToClosePublicationOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_ERROR, ErrorMessageBuffer, buffer =>
            {
                ErrorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                ErrorResponse.ErrorMessage("channel unknown");
                ErrorResponse.OffendingCommandCorrelationId(CLOSE_CORRELATION_ID);
                return ErrorResponse.Length();
            });

            publication.Dispose();
        }


        [Test]
        [ExpectedException(typeof(RegistrationException))]
        public void ShouldFailToAddPublicationOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_ERROR, ErrorMessageBuffer, buffer =>
            {
                ErrorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                ErrorResponse.ErrorMessage("invalid channel");
                ErrorResponse.OffendingCommandCorrelationId(CORRELATION_ID);
                return ErrorResponse.Length();
            });

            Conductor.AddPublication(CHANNEL, STREAM_ID_1);
        }

        [Test]
        public void ClosingPublicationDoesNotRemoveOtherPublications()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer =>
            {
                PublicationReady.StreamId(STREAM_ID_2);
                PublicationReady.SessionId(SESSION_ID_2);
                PublicationReady.LogFileName(SESSION_ID_2 + "-log");
                PublicationReady.CorrelationId(CORRELATION_ID_2);
                PublicationReady.RegistrationId(CORRELATION_ID_2);
                return PublicationReady.Length();
            });

            Conductor.AddPublication(CHANNEL, STREAM_ID_2);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, OperationSuccessBuffer, buffer => OperationSucceededFlyweight.LENGTH);

            publication.Dispose();

            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).MustHaveHappened();
            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID_2)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldNotMapBuffersForUnknownCorrelationId()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer =>
            {
                PublicationReady.CorrelationId(UNKNOWN_CORRELATION_ID);
                PublicationReady.RegistrationId(UNKNOWN_CORRELATION_ID);
                return PublicationReady.Length();
            });

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, buffer =>
            {
                PublicationReady.CorrelationId(CORRELATION_ID);
                return PublicationReady.Length();
            });

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);
            Conductor.DoWork();

            A.CallTo(() => LogBuffersFactory.Map(A<string>._)).MustHaveHappened(Repeated.Exactly.Once);
            Assert.AreEqual(publication.RegistrationId, CORRELATION_ID);
        }

        // ---------------------------------
        // Subscription related interactions
        // ---------------------------------

        [Test]
        public void AddSubscriptionShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_SUBSCRIPTION_READY, SubscriptionReadyBuffer, buffer =>
            {
                SubscriptionReady.CorrelationId(CORRELATION_ID);
                return SubscriptionReadyFlyweight.LENGTH;
            });

            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            A.CallTo(() => DriverProxy.AddSubscription(CHANNEL, STREAM_ID_1)).MustHaveHappened();
        }

        [Test]
        public void ClosingSubscriptionShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_SUBSCRIPTION_READY, SubscriptionReadyBuffer, buffer =>
            {
                SubscriptionReady.CorrelationId(CORRELATION_ID);
                return SubscriptionReadyFlyweight.LENGTH;
            });

            var subscription = Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, OperationSuccessBuffer, buffer =>
            {
                OperationSuccess.CorrelationId(CLOSE_CORRELATION_ID);
                return OperationSucceededFlyweight.LENGTH;
            });

            subscription.Dispose();

            A.CallTo(() => DriverProxy.RemoveSubscription(CORRELATION_ID)).MustHaveHappened();
        }

 // Timeout not implemented in the .NetStandard version of NUnit yet
#if NETFULL
        [Test]
        [Timeout(5000)]
        [ExpectedException(typeof(DriverTimeoutException))]
        public void AddSubscriptionShouldTimeoutWithoutOperationSuccessful()
        {
            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);
        }
#endif
        [Test]
        [ExpectedException(typeof(RegistrationException))]
        public void ShouldFailToAddSubscriptionOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_ERROR, ErrorMessageBuffer, buffer =>
            {
                ErrorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                ErrorResponse.ErrorMessage("invalid channel");
                ErrorResponse.OffendingCommandCorrelationId(CORRELATION_ID);
                return ErrorResponse.Length();
            });

            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);
        }

        [Test]
        public void ClientNotifiedOfNewImageShouldMapLogFile()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_SUBSCRIPTION_READY, SubscriptionReadyBuffer, buffer =>
            {
                SubscriptionReady.CorrelationId(CORRELATION_ID);
                return SubscriptionReadyFlyweight.LENGTH;
            });

            Subscription subscription = Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            Conductor.OnAvailableImage(
                CORRELATION_ID, 
                STREAM_ID_1, 
                SESSION_ID_1, 
                subscription.RegistrationId, 
                SUBSCRIPTION_POSITION_ID, 
                SESSION_ID_1 + "-log", 
                SOURCE_INFO);

            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_1 + "-log")).MustHaveHappened();
        }

        [Test]
        public void ClientNotifiedOfNewAndInactiveImages()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_SUBSCRIPTION_READY, SubscriptionReadyBuffer, buffer =>
            {
                SubscriptionReady.CorrelationId(CORRELATION_ID);
                return SubscriptionReadyFlyweight.LENGTH;
            });

            var subscription = Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            Conductor.OnAvailableImage(
                CORRELATION_ID, 
                STREAM_ID_1, 
                SESSION_ID_1, 
                subscription.RegistrationId, 
                SUBSCRIPTION_POSITION_ID, 
                SESSION_ID_1 + "-log", 
                SOURCE_INFO);

            Assert.False(subscription.HasNoImages);
            Assert.True(subscription.IsConnected);

            A.CallTo(() => MockAvailableImageHandler(A<Image>._)).MustHaveHappened();

            Conductor.OnUnavailableImage(CORRELATION_ID, subscription.RegistrationId, STREAM_ID_1);

            A.CallTo(() => MockUnavailableImageHandler(A<Image>._)).MustHaveHappened();

            Assert.True(subscription.HasNoImages);
            Assert.False(subscription.IsConnected);
        }

        [Test]
        public void ShouldIgnoreUnknownNewImage()
        {
            Conductor.OnAvailableImage(
                CORRELATION_ID_2, 
                STREAM_ID_2, 
                SESSION_ID_2, 
                SUBSCRIPTION_POSITION_REGISTRATION_ID, 
                SUBSCRIPTION_POSITION_ID, 
                SESSION_ID_2 + "-log", 
                SOURCE_INFO);

            A.CallTo(() => LogBuffersFactory.Map(A<string>._)).MustNotHaveHappened();
            A.CallTo(() => MockAvailableImageHandler(A<Image>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldIgnoreUnknownInactiveImage()
        {
            Conductor.OnUnavailableImage(CORRELATION_ID_2, SUBSCRIPTION_POSITION_REGISTRATION_ID, STREAM_ID_2);

            A.CallTo(() => LogBuffersFactory.Map(A<string>._)).MustNotHaveHappened();
            A.CallTo(() => MockAvailableImageHandler(A<Image>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldTimeoutInterServiceIfTooLongBetweenDoWorkCalls()
        {
            SuppressPrintError = true;

            Conductor.DoWork();
            NanoClock.AdvanceMillis(INTER_SERVICE_TIMEOUT_MS);
            NanoClock.AdvancedNanos(1);
            Conductor.DoWork();

            A.CallTo(() => MockClientErrorHandler(A<ConductorServiceTimeoutException>._)).MustHaveHappened();

            Assert.True(Conductor.IsClosed());
        }

        private void WhenReceiveBroadcastOnMessage(int msgTypeId, IMutableDirectBuffer buffer, Func<IMutableDirectBuffer, int> filler)
        {
            A.CallTo(() => MockToClientReceiver.Receive(A<MessageHandler>._)).Invokes(() =>
            {
                var length = filler(buffer);
                Conductor.DriverListenerAdapter().OnMessage(msgTypeId, buffer, 0, length);
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