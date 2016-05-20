using System;
using System.Collections.Generic;
using System.Threading;
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
        private static readonly int NUM_BUFFERS = (LogBufferDescriptor.PARTITION_COUNT*2) + 1;

        protected internal const int SESSION_ID_1 = 13;
        protected internal const int SESSION_ID_2 = 15;

        private const int COUNTER_BUFFER_LENGTH = 1024;
        private const string CHANNEL = "udp://localhost:40124";
        private const int STREAM_ID_1 = 2;
        private const int STREAM_ID_2 = 4;
        private const int SEND_BUFFER_CAPACITY = 1024;

        private const long CORRELATION_ID = 2000;
        private const long CORRELATION_ID_2 = 2002;
        private const long CLOSE_CORRELATION_ID = 2001;
        private const long UNKNOWN_CORRELATION_ID = 3000;

        private static readonly long KEEP_ALIVE_INTERVAL = NanoUtil.FromMilliseconds(500);
        private const long AWAIT_TIMEOUT = 100;
        private const long INTER_SERVICE_TIMEOUT_MS = 100;
        private const long PUBLICATION_CONNECTION_TIMEOUT_MS = 5000;

        private const string SOURCE_INFO = "127.0.0.1:40789";

        private PublicationBuffersReadyFlyweight PublicationReady;
        private CorrelatedMessageFlyweight CorrelatedMessage;
        private ErrorResponseFlyweight ErrorResponse;

        private UnsafeBuffer PublicationReadyBuffer;
        private UnsafeBuffer CorrelatedMessageBuffer;
        private UnsafeBuffer ErrorMessageBuffer;

        private CopyBroadcastReceiver MockToClientReceiver;

        private UnsafeBuffer CounterValuesBuffer;

        private readonly IEpochClock EpochClock = new SystemEpochClock();
        private readonly INanoClock NanoClock = new SystemNanoClock();
        private ErrorHandler MockClientErrorHandler;

        private DriverProxy DriverProxy;
        private ClientConductor Conductor;
        private AvailableImageHandler MockAvailableImageHandler;
        private UnavailableImageHandler MockUnavailableImageHandler;
        private ILogBuffersFactory LogBuffersFactory;
        private IDictionary<long, long> SubscriberPositionMap;
        private bool SuppressPrintError = false;


        [SetUp]
        public virtual void SetUp()
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
            CorrelatedMessage = new CorrelatedMessageFlyweight();
            ErrorResponse = new ErrorResponseFlyweight();

            PublicationReadyBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            CorrelatedMessageBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            ErrorMessageBuffer = new UnsafeBuffer(new byte[SEND_BUFFER_CAPACITY]);
            CounterValuesBuffer = new UnsafeBuffer(new byte[COUNTER_BUFFER_LENGTH]);
            MockToClientReceiver = A.Fake<CopyBroadcastReceiver>();

            MockAvailableImageHandler = A.Fake<AvailableImageHandler>();
            MockUnavailableImageHandler = A.Fake<UnavailableImageHandler>();

            LogBuffersFactory = A.Fake<ILogBuffersFactory>();

            SubscriberPositionMap = new Dictionary<long, long>(); // should return -1 when element does not exist

            DriverProxy = A.Fake<DriverProxy>();

            A.CallTo(() => DriverProxy.AddPublication(CHANNEL, STREAM_ID_1)).Returns(CORRELATION_ID);
            A.CallTo(() => DriverProxy.AddPublication(CHANNEL, STREAM_ID_2)).Returns(CORRELATION_ID_2);
            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).Returns(CLOSE_CORRELATION_ID);
            A.CallTo(() => DriverProxy.AddSubscription(A<string>._, A<int>._)).Returns(CORRELATION_ID);
            A.CallTo(() => DriverProxy.RemoveSubscription(CORRELATION_ID)).Returns(CLOSE_CORRELATION_ID);

            Conductor = new ClientConductor(EpochClock, NanoClock, MockToClientReceiver, LogBuffersFactory, CounterValuesBuffer, DriverProxy, MockClientErrorHandler, MockAvailableImageHandler, MockUnavailableImageHandler, KEEP_ALIVE_INTERVAL, AWAIT_TIMEOUT, NanoUtil.FromMilliseconds(INTER_SERVICE_TIMEOUT_MS), PUBLICATION_CONNECTION_TIMEOUT_MS);

            PublicationReady.Wrap(PublicationReadyBuffer, 0);
            CorrelatedMessage.Wrap(CorrelatedMessageBuffer, 0);
            ErrorResponse.Wrap(ErrorMessageBuffer, 0);

            PublicationReady.CorrelationId(CORRELATION_ID);
            PublicationReady.SessionId(SESSION_ID_1);
            PublicationReady.StreamId(STREAM_ID_1);
            PublicationReady.LogFileName(SESSION_ID_1 + "-log");

            SubscriberPositionMap.Add(CORRELATION_ID, 0);

            CorrelatedMessage.CorrelationId(CLOSE_CORRELATION_ID);

            var atomicBuffersSession1 = new UnsafeBuffer[NUM_BUFFERS];
            var atomicBuffersSession2 = new UnsafeBuffer[NUM_BUFFERS];

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                var termBuffersSession1 = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
                var metaDataBuffersSession1 = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_META_DATA_LENGTH]);
                var termBuffersSession2 = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
                var metaDataBuffersSession2 = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_META_DATA_LENGTH]);

                atomicBuffersSession1[i] = termBuffersSession1;
                atomicBuffersSession1[i + LogBufferDescriptor.PARTITION_COUNT] = metaDataBuffersSession1;
                atomicBuffersSession2[i] = termBuffersSession2;
                atomicBuffersSession2[i + LogBufferDescriptor.PARTITION_COUNT] = metaDataBuffersSession2;
            }

            atomicBuffersSession1[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
            atomicBuffersSession2[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);

            UnsafeBuffer header1 = DataHeaderFlyweight.CreateDefaultHeader(SESSION_ID_1, STREAM_ID_1, 0);
            UnsafeBuffer header2 = DataHeaderFlyweight.CreateDefaultHeader(SESSION_ID_2, STREAM_ID_2, 0);

            LogBufferDescriptor.StoreDefaultFrameHeader(atomicBuffersSession1[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX], header1);
            LogBufferDescriptor.StoreDefaultFrameHeader(atomicBuffersSession2[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX], header2);

            var logBuffersSession1 = A.Fake<LogBuffers>();
            var logBuffersSession2 = A.Fake<LogBuffers>();

            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_1 + "-log")).Returns(logBuffersSession1);
            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_2 + "-log")).Returns(logBuffersSession2);
            A.CallTo(() => logBuffersSession1.AtomicBuffers()).Returns(atomicBuffersSession1);
            A.CallTo(() => logBuffersSession2.AtomicBuffers()).Returns(atomicBuffersSession2);
        }

        // --------------------------------
        // Publication related interactions
        // --------------------------------

        [Test]
        public virtual void AddPublicationShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            A.CallTo(() => DriverProxy.AddPublication(CHANNEL, STREAM_ID_1)).MustHaveHappened();
        }

        [Test]
        public virtual void AddPublicationShouldMapLogFile()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_1 + "-log")).MustHaveHappened();
        }


        [Test]
        [ExpectedException(typeof(DriverTimeoutException))]
        public virtual void AddPublicationShouldTimeoutWithoutReadyMessage()
        {
            Conductor.AddPublication(CHANNEL, STREAM_ID_1);
        }

        [Test]
        public virtual void ConductorShouldCachePublicationInstances()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var firstPublication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);
            var secondPublication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            Assert.AreSame(firstPublication, secondPublication);
        }

        [Test]
        public virtual void ClosingPublicationShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) => CorrelatedMessageFlyweight.LENGTH);

            publication.Dispose();

            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).MustHaveHappened();
        }

        [Test]
        public virtual void ClosingPublicationShouldPurgeCache()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var firstPublication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) => CorrelatedMessageFlyweight.LENGTH);

            firstPublication.Dispose();

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var secondPublication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            Assert.AreNotSame(firstPublication, secondPublication);
        }

        [Test]
        [ExpectedException(typeof(RegistrationException))]
        public virtual void ShouldFailToClosePublicationOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_ERROR, ErrorMessageBuffer, (buffer) =>
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
        public virtual void ShouldFailToAddPublicationOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_ERROR, ErrorMessageBuffer, (buffer) =>
            {
                ErrorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                ErrorResponse.ErrorMessage("invalid channel");
                ErrorResponse.OffendingCommandCorrelationId(CORRELATION_ID);
                return ErrorResponse.Length();
            });

            Conductor.AddPublication(CHANNEL, STREAM_ID_1);
        }

        [Test]
        public virtual void PublicationOnlyRemovedOnLastClose()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);
            Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            publication.Dispose();

            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).MustNotHaveHappened();

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) => CorrelatedMessageFlyweight.LENGTH);

            publication.Dispose();
            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).MustHaveHappened();
        }


        [Test]
        public virtual void ClosingPublicationDoesNotRemoveOtherPublications()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) => PublicationReady.Length());

            var publication = Conductor.AddPublication(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) =>
            {
                PublicationReady.StreamId(STREAM_ID_2);
                PublicationReady.SessionId(SESSION_ID_2);
                PublicationReady.LogFileName(SESSION_ID_2 + "-log");
                PublicationReady.CorrelationId(CORRELATION_ID_2);
                return PublicationReady.Length();
            });

            Conductor.AddPublication(CHANNEL, STREAM_ID_2);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) => CorrelatedMessageFlyweight.LENGTH);

            publication.Dispose();

            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID)).MustHaveHappened();
            A.CallTo(() => DriverProxy.RemovePublication(CORRELATION_ID_2)).MustNotHaveHappened();
        }

        [Test]
        public virtual void ShouldNotMapBuffersForUnknownCorrelationId()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) =>
            {
                PublicationReady.CorrelationId(UNKNOWN_CORRELATION_ID);
                return PublicationReady.Length();
            });

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_PUBLICATION_READY, PublicationReadyBuffer, (buffer) =>
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
        public virtual void AddSubscriptionShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) =>
            {
                CorrelatedMessage.CorrelationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            A.CallTo(() => DriverProxy.AddSubscription(CHANNEL, STREAM_ID_1)).MustHaveHappened();
        }

        [Test]
        public virtual void ClosingSubscriptionShouldNotifyMediaDriver()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) =>
            {
                CorrelatedMessage.CorrelationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

            var subscription = Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) =>
            {
                CorrelatedMessage.CorrelationId(CLOSE_CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

            subscription.Dispose();

            A.CallTo(() => DriverProxy.RemoveSubscription(CORRELATION_ID)).MustHaveHappened();
        }

        [Test]
        [ExpectedException(typeof(DriverTimeoutException))]
        public virtual void AddSubscriptionShouldTimeoutWithoutOperationSuccessful()
        {
            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);
        }

        [Test]
        [ExpectedException(typeof(RegistrationException))]
        public virtual void ShouldFailToAddSubscriptionOnMediaDriverError()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_ERROR, ErrorMessageBuffer, (buffer) =>
            {
                ErrorResponse.ErrorCode(ErrorCode.INVALID_CHANNEL);
                ErrorResponse.ErrorMessage("invalid channel");
                ErrorResponse.OffendingCommandCorrelationId(CORRELATION_ID);
                return ErrorResponse.Length();
            });

            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);
        }

        [Test]
        public virtual void ClientNotifiedOfNewImageShouldMapLogFile()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) =>
            {
                CorrelatedMessage.CorrelationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

            Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            Conductor.OnAvailableImage(STREAM_ID_1, SESSION_ID_1, SubscriberPositionMap, SESSION_ID_1 + "-log", SOURCE_INFO, CORRELATION_ID);

            A.CallTo(() => LogBuffersFactory.Map(SESSION_ID_1 + "-log")).MustHaveHappened();
        }

        [Test]
        public virtual void ClientNotifiedOfNewAndInactiveImages()
        {
            WhenReceiveBroadcastOnMessage(ControlProtocolEvents.ON_OPERATION_SUCCESS, CorrelatedMessageBuffer, (buffer) =>
            {
                CorrelatedMessage.CorrelationId(CORRELATION_ID);
                return CorrelatedMessageFlyweight.LENGTH;
            });

            var subscription = Conductor.AddSubscription(CHANNEL, STREAM_ID_1);

            Conductor.OnAvailableImage(STREAM_ID_1, SESSION_ID_1, SubscriberPositionMap, SESSION_ID_1 + "-log", SOURCE_INFO, CORRELATION_ID);

            Assert.False(subscription.HasNoImages);

            A.CallTo(() => MockAvailableImageHandler(A<Image>._)).MustHaveHappened();

            Conductor.OnUnavailableImage(STREAM_ID_1, CORRELATION_ID);


            A.CallTo(() => MockUnavailableImageHandler(A<Image>._)).MustHaveHappened();

            Assert.True(subscription.HasNoImages);
            Assert.False(subscription.HasImage(SESSION_ID_1));
        }

        [Test]
        public virtual void ShouldIgnoreUnknownNewImage()
        {
            Conductor.OnAvailableImage(STREAM_ID_2, SESSION_ID_2, SubscriberPositionMap, SESSION_ID_2 + "-log", SOURCE_INFO, CORRELATION_ID_2);

            A.CallTo(() => LogBuffersFactory.Map(A<string>._)).MustNotHaveHappened();
            A.CallTo(() => MockAvailableImageHandler(A<Image>._)).MustNotHaveHappened();
        }

        [Test]
        public virtual void ShouldIgnoreUnknownInactiveImage()
        {
            Conductor.OnUnavailableImage(STREAM_ID_2, CORRELATION_ID_2);

            A.CallTo(() => LogBuffersFactory.Map(A<string>._)).MustNotHaveHappened();
            A.CallTo(() => MockAvailableImageHandler(A<Image>._)).MustNotHaveHappened();
        }


        [Test]
        public virtual void ShouldTimeoutInterServiceIfTooLongBetweenDoWorkCalls()
        {
            SuppressPrintError = true;

            Conductor.DoWork();
            Thread.Sleep((int) INTER_SERVICE_TIMEOUT_MS + 10);
            Conductor.DoWork();

            A.CallTo(() => MockClientErrorHandler(A<ConductorServiceTimeoutException>._)).MustHaveHappened();
        }

        private void WhenReceiveBroadcastOnMessage(int msgTypeId, UnsafeBuffer buffer, Func<UnsafeBuffer, int> filler)
        {
            A.CallTo(() => MockToClientReceiver.Receive(A<MessageHandler>._)).Invokes(() =>
            {
                var length = filler(buffer);
                Conductor.DriverListenerAdapter().OnMessage(msgTypeId, buffer, 0, length);
            });
        }
    }
}