using System;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;
using AeronType = Adaptive.Aeron.Aeron;
using Context = Adaptive.Archiver.AeronArchive.Context;

namespace Adaptive.Archiver.Tests
{
    public class AeronArchiveTest
    {
        private const string SESSION_ID_PARAM_NAME = "session-id";
        private const string MTU_LENGTH_PARAM_NAME = "mtu";
        private const string TERM_LENGTH_PARAM_NAME = "term-length";
        private const string SPARSE_PARAM_NAME = "sparse";

        private AeronType _aeron;
        private ControlResponsePoller _controlResponsePoller;
        private ArchiveProxy _archiveProxy;
        private IErrorHandler _errorHandler;

        [SetUp]
        public void SetUp()
        {
            _aeron = A.Fake<AeronType>();
            _controlResponsePoller = A.Fake<ControlResponsePoller>();
            _archiveProxy = A.Fake<ArchiveProxy>();
            _errorHandler = A.Fake<IErrorHandler>();
        }

        [Test]
        public void AsyncConnectShouldConcludeContext()
        {
            var ctx = A.Fake<Context>();
            var expectedException = new InvalidOperationException("test");
            A.CallTo(() => ctx.Conclude()).Throws(expectedException);

            var actualException = Assert.Throws<InvalidOperationException>(() => AeronArchive.ConnectAsync(ctx));
            Assert.AreSame(expectedException, actualException);

            A.CallTo(() => ctx.Conclude()).MustHaveHappened();
            A.CallTo(ctx).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void AsyncConnectShouldCloseContext()
        {
            const string responseChannel = "aeron:udp?endpoint=localhost:1234";
            const int responseStreamId = 49;
            var ctx = A.Fake<Context>();
            A.CallTo(() => ctx.AeronClient()).Returns(_aeron);
            A.CallTo(() => ctx.ControlResponseChannel()).Returns(responseChannel);
            A.CallTo(() => ctx.ControlResponseStreamId()).Returns(responseStreamId);
            var error = new InvalidOperationException("subscription");
            A.CallTo(() => _aeron.AsyncAddSubscription(
                    responseChannel,
                    responseStreamId,
                    A<AvailableImageHandler>._,
                    A<UnavailableImageHandler>._))
                .Throws(error);

            var actualException = Assert.Throws<InvalidOperationException>(() => AeronArchive.ConnectAsync(ctx));
            Assert.AreSame(error, actualException);

            A.CallTo(() => ctx.Conclude()).MustHaveHappened()
                .Then(A.CallTo(() => ctx.AeronClient()).MustHaveHappened())
                .Then(A.CallTo(() => ctx.ControlResponseChannel()).MustHaveHappened())
                .Then(A.CallTo(() => ctx.ControlResponseStreamId()).MustHaveHappened())
                .Then(A.CallTo(() => _aeron.AsyncAddSubscription(
                        responseChannel, responseStreamId, A<AvailableImageHandler>._, A<UnavailableImageHandler>._))
                    .MustHaveHappened())
                .Then(A.CallTo(() => ctx.Dispose()).MustHaveHappened());
        }

        [Test]
        public void AsyncConnectShouldCloseResourceInCaseOfExceptionUponStartup()
        {
            const string responseChannel = "aeron:udp?endpoint=localhost:0";
            const int responseStreamId = 49;
            const string requestChannel = "aeron:udp?endpoint=localhost:1234";
            const int requestStreamId = -15;
            const long subscriptionId = -3275938475934759L;

            var ctx = A.Fake<Context>();
            A.CallTo(() => ctx.AeronClient()).Returns(_aeron);
            A.CallTo(() => ctx.ControlResponseChannel()).Returns(responseChannel);
            A.CallTo(() => ctx.ControlResponseStreamId()).Returns(responseStreamId);
            A.CallTo(() => ctx.ControlRequestChannel()).Returns(requestChannel);
            A.CallTo(() => ctx.ControlRequestStreamId()).Returns(requestStreamId);
            A.CallTo(() => _aeron.AsyncAddSubscription(
                    responseChannel,
                    responseStreamId,
                    A<AvailableImageHandler>._,
                    A<UnavailableImageHandler>._))
                .Returns(subscriptionId);
            var error = new IndexOutOfRangeException("exception");
            A.CallTo(() => _aeron.Ctx).Throws(error);

            var actualException = Assert.Throws<IndexOutOfRangeException>(() => AeronArchive.ConnectAsync(ctx));
            Assert.AreSame(error, actualException);

            A.CallTo(() => ctx.Conclude()).MustHaveHappened()
                .Then(A.CallTo(() => ctx.AeronClient()).MustHaveHappened())
                .Then(A.CallTo(() => ctx.ControlResponseChannel()).MustHaveHappened())
                .Then(A.CallTo(() => ctx.ControlResponseStreamId()).MustHaveHappened())
                .Then(A.CallTo(() => _aeron.AsyncAddSubscription(
                        responseChannel, responseStreamId, A<AvailableImageHandler>._, A<UnavailableImageHandler>._))
                    .MustHaveHappened())
                .Then(A.CallTo(() => _aeron.AsyncRemoveSubscription(subscriptionId)).MustHaveHappened())
                .Then(A.CallTo(() => ctx.Dispose()).MustHaveHappened());
        }

        [Test]
        public void ShouldQuietClose()
        {
            var previousException = new Exception();
            var thrownException = new Exception();

            var throwingCloseable = A.Fake<IDisposable>();
            var nonThrowingCloseable = A.Fake<IDisposable>();
            A.CallTo(() => throwingCloseable.Dispose()).Throws(thrownException);

            Assert.IsNull(AeronArchive.QuietClose(null, nonThrowingCloseable));
            Assert.AreSame(previousException, AeronArchive.QuietClose(previousException, nonThrowingCloseable));
            var ex = AeronArchive.QuietClose(previousException, throwingCloseable);
            Assert.AreSame(previousException, ex);
            Assert.AreSame(thrownException, ex.GetSuppressed()[0]);
            Assert.AreSame(thrownException, AeronArchive.QuietClose(null, throwingCloseable));
        }

        [TestCase(AeronType.NULL_VALUE)]
        [TestCase(long.MaxValue)]
        [TestCase(long.MinValue)]
        [TestCase(0L)]
        [TestCase(4468236482L)]
        public void ShouldReturnAssignedArchiveId(long archiveId)
        {
            const long controlSessionId = -3924293L;
            A.CallTo(() => _aeron.Ctx).Returns(new AeronType.Context());
            var context = new Context()
                .AeronClient(_aeron)
                .IdleStrategy(new NoOpIdleStrategy())
                .MessageTimeoutNs(100)
                .Lock(new NoOpLock())
                .ErrorHandler(_errorHandler)
                .OwnsAeronClient(true);

            var aeronArchive = new AeronArchive(context, _controlResponsePoller, _archiveProxy, controlSessionId, archiveId);

            Assert.AreEqual(archiveId, aeronArchive.ArchiveId());
        }

        [TestCase(
            "aeron:udp?endpoint=localhost:3388|mtu=2048",
            "aeron:udp?session-id=5|endpoint=localhost:0|sparse=true|mtu=1024")]
        [TestCase(
            "aeron:udp?endpoint=localhost:3388",
            "aeron:udp?control=localhost:10000|control-mode=dynamic")]
        [TestCase(
            "aeron:udp?endpoint=localhost:3388",
            "aeron:udp?control-mode=manual")]
        [TestCase(
            "aeron:ipc?alias=request|ssc=false|linger=0|session-id=42|sparse=false",
            "aeron:ipc?term-length=64k|alias=response")]
        public void ShouldAddAUniqueSessionIdParameterToBothRequestAndResponseChannels(
            string requestChannel, string responseChannel)
        {
            const int requestStreamId = 42;
            const int responseStreamId = -19;
            int sessionId = BitUtil.GenerateRandomisedId();
            A.CallTo(() => _aeron.NextSessionId(requestStreamId)).Returns(sessionId);

            var context = new Context()
                .AeronClient(_aeron)
                .OwnsAeronClient(false)
                .ErrorHandler(_errorHandler)
                .ControlRequestChannel(requestChannel)
                .ControlRequestStreamId(requestStreamId)
                .ControlResponseChannel(responseChannel)
                .ControlResponseStreamId(responseStreamId)
                .ControlTermBufferSparse(false)
                .ControlTermBufferLength(128 * 1024)
                .ControlMtuLength(4096);

            Assert.AreEqual(requestChannel, context.ControlRequestChannel());
            Assert.AreEqual(requestStreamId, context.ControlRequestStreamId());
            Assert.AreEqual(responseChannel, context.ControlResponseChannel());
            Assert.AreEqual(responseStreamId, context.ControlResponseStreamId());

            context.Conclude();

            A.CallTo(() => _aeron.NextSessionId(requestStreamId)).MustHaveHappened();
            Assert.AreEqual(requestStreamId, context.ControlRequestStreamId());
            Assert.AreEqual(responseStreamId, context.ControlResponseStreamId());

            var actualRequestChannel = ChannelUri.Parse(context.ControlRequestChannel());
            var actualResponseChannel = ChannelUri.Parse(context.ControlResponseChannel());
            Assert.IsTrue(actualRequestChannel.ContainsKey(SESSION_ID_PARAM_NAME), "session-id was not added");
            Assert.AreEqual(sessionId.ToString(), actualRequestChannel.Get(SESSION_ID_PARAM_NAME));
            Assert.AreEqual(sessionId.ToString(), actualResponseChannel.Get(SESSION_ID_PARAM_NAME));

            ChannelUri.Parse(requestChannel).ForEachParameter((key, value) =>
            {
                if (!SESSION_ID_PARAM_NAME.Equals(key))
                {
                    Assert.AreEqual(value, actualRequestChannel.Get(key));
                }
            });

            ChannelUri.Parse(responseChannel).ForEachParameter((key, value) =>
            {
                if (!SESSION_ID_PARAM_NAME.Equals(key))
                {
                    Assert.AreEqual(value, actualResponseChannel.Get(key));
                }
            });
        }

        [Test]
        public void ShouldNotAddASessionIdIfControlModeResponseIsSpecifiedOnTheResponseChannel()
        {
            const int requestStreamId = 100;
            const int responseStreamId = 200;
            const string requestChannel = "aeron:udp?endpoint=localhost:8080";
            const string responseChannel = "aeron:udp?control-mode=response|control=localhost:10002";
            var context = new Context()
                .AeronClient(_aeron)
                .OwnsAeronClient(false)
                .ErrorHandler(_errorHandler)
                .ControlRequestChannel(requestChannel)
                .ControlRequestStreamId(requestStreamId)
                .ControlResponseChannel(responseChannel)
                .ControlResponseStreamId(responseStreamId);

            Assert.AreEqual(requestChannel, context.ControlRequestChannel());
            Assert.AreEqual(requestStreamId, context.ControlRequestStreamId());
            Assert.AreEqual(responseChannel, context.ControlResponseChannel());
            Assert.AreEqual(responseStreamId, context.ControlResponseStreamId());

            context.Conclude();

            Assert.AreEqual(requestStreamId, context.ControlRequestStreamId());
            Assert.AreEqual(responseStreamId, context.ControlResponseStreamId());

            var actualRequestChannel = ChannelUri.Parse(context.ControlRequestChannel());
            var actualResponseChannel = ChannelUri.Parse(context.ControlResponseChannel());
            Assert.IsNull(actualRequestChannel.Get(SESSION_ID_PARAM_NAME), "unexpected session-id on request channel");
            Assert.IsNull(actualResponseChannel.Get(SESSION_ID_PARAM_NAME), "unexpected session-id on response channel");

            ChannelUri.Parse(requestChannel).ForEachParameter(
                (key, value) => Assert.AreEqual(value, actualRequestChannel.Get(key)));

            ChannelUri.Parse(responseChannel).ForEachParameter(
                (key, value) => Assert.AreEqual(value, actualResponseChannel.Get(key)));
        }

        [Test]
        public void ShouldAddDefaultUriParametersIfNotSpecified()
        {
            const int requestStreamId = 10;
            const int responseStreamId = 20;
            const string requestChannel = "aeron:udp?endpoint=localhost:8080";
            const string responseChannel = "aeron:udp?endpoint=localhost:0";
            var context = new Context()
                .AeronClient(_aeron)
                .OwnsAeronClient(false)
                .ErrorHandler(_errorHandler)
                .ControlRequestChannel(requestChannel)
                .ControlRequestStreamId(requestStreamId)
                .ControlResponseChannel(responseChannel)
                .ControlResponseStreamId(responseStreamId)
                .ControlMtuLength(2048)
                .ControlTermBufferLength(256 * 1024)
                .ControlTermBufferSparse(true);

            Assert.AreEqual(requestChannel, context.ControlRequestChannel());
            Assert.AreEqual(requestStreamId, context.ControlRequestStreamId());
            Assert.AreEqual(responseChannel, context.ControlResponseChannel());
            Assert.AreEqual(responseStreamId, context.ControlResponseStreamId());

            context.Conclude();

            Assert.AreEqual(requestStreamId, context.ControlRequestStreamId());
            Assert.AreEqual(responseStreamId, context.ControlResponseStreamId());

            var actualRequestChannel = ChannelUri.Parse(context.ControlRequestChannel());
            var actualResponseChannel = ChannelUri.Parse(context.ControlResponseChannel());
            Assert.AreEqual(context.ControlMtuLength().ToString(), actualRequestChannel.Get(MTU_LENGTH_PARAM_NAME));
            Assert.AreEqual(context.ControlMtuLength().ToString(), actualResponseChannel.Get(MTU_LENGTH_PARAM_NAME));
            Assert.AreEqual(
                context.ControlTermBufferLength().ToString(),
                actualRequestChannel.Get(TERM_LENGTH_PARAM_NAME));
            Assert.AreEqual(
                context.ControlTermBufferLength().ToString(),
                actualResponseChannel.Get(TERM_LENGTH_PARAM_NAME));
            string sparseStr = context.ControlTermBufferSparse() ? "true" : "false";
            Assert.AreEqual(sparseStr, actualRequestChannel.Get(SPARSE_PARAM_NAME));
            Assert.AreEqual(sparseStr, actualResponseChannel.Get(SPARSE_PARAM_NAME));
        }

        [TestCase(int.MinValue)]
        [TestCase(-1)]
        [TestCase(0)]
        public void ShouldRejectInvalidRetryAttempts(int retryAttempts)
        {
            var context = new Context()
                .AeronClient(_aeron)
                .ControlRequestChannel("aeron:udp")
                .ControlResponseChannel("aeron:udp")
                .MessageRetryAttempts(retryAttempts);
            Assert.AreEqual(retryAttempts, context.MessageRetryAttempts());

            var exception = Assert.Throws<ConfigurationException>(() => context.Conclude());
            Assert.AreEqual(
                "AeronArchive.Context.messageRetryAttempts must be > 0, got: " + retryAttempts,
                exception.Message);
        }

        [Test]
        public void MaxRetryAttemptsDefaultValue()
        {
            var context = new Context();
            Assert.AreEqual(AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_DEFAULT, context.MessageRetryAttempts());
        }

        [Test]
        public void MaxRetryAttemptsSystemProperty()
        {
            Config.Params[AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_PROP_NAME] = "111";
            try
            {
                var context = new Context();
                Assert.AreEqual(111, context.MessageRetryAttempts());
            }
            finally
            {
                Config.Params.Remove(AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_PROP_NAME);
            }
        }

        [Test]
        public void CloseNotOwningAeronClient()
        {
            const long controlSessionId = 42L;
            const long archiveId = -190L;

            var aeronContext = A.Fake<AeronType.Context>();
            A.CallTo(() => aeronContext.NanoClock()).Returns(SystemNanoClock.INSTANCE);
            A.CallTo(() => _aeron.Ctx).Returns(aeronContext);
            var aeronException = new SynchronizationLockException("aeron closed");
            A.CallTo(() => _aeron.Dispose()).Throws(aeronException);

            var publication = A.Fake<Publication>();
            A.CallTo(() => publication.IsConnected).Returns(true);
            var publicationException = new InvalidOperationException("publication is closed");
            A.CallTo(() => publication.Dispose()).Throws(publicationException);

            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _controlResponsePoller.Subscription()).Returns(subscription);
            var subscriptionException = new IndexOutOfRangeException("subscription");
            A.CallTo(() => subscription.Dispose()).Throws(subscriptionException);

            A.CallTo(() => _archiveProxy.Pub()).Returns(publication);
            var closeSessionException = new IndexOutOfRangeException();
            A.CallTo(() => _archiveProxy.CloseSession(controlSessionId)).Throws(closeSessionException);

            var context = new Context()
                .AeronClient(_aeron)
                .IdleStrategy(new NoOpIdleStrategy())
                .MessageTimeoutNs(100)
                .Lock(new NoOpLock())
                .ErrorHandler(_errorHandler)
                .OwnsAeronClient(false);
            var aeronArchive = new AeronArchive(context, _controlResponsePoller, _archiveProxy, controlSessionId, archiveId);

            aeronArchive.Dispose();

            A.CallTo(() => _errorHandler.OnError(A<Exception>.That.Matches(ex =>
                ReferenceEquals(closeSessionException, ex) &&
                ex.GetSuppressed().Count >= 2 &&
                ReferenceEquals(publicationException, ex.GetSuppressed()[0]) &&
                ReferenceEquals(subscriptionException, ex.GetSuppressed()[1])))).MustHaveHappened();
            A.CallTo(_errorHandler).MustHaveHappenedOnceExactly();
            A.CallTo(() => publication.Dispose()).MustHaveHappened();
            A.CallTo(() => subscription.Dispose()).MustHaveHappened();
        }

        [Test]
        public void CloseOwningAeronClient()
        {
            const long controlSessionId = 42L;
            const long archiveId = 555L;

            var aeronContext = A.Fake<AeronType.Context>();
            A.CallTo(() => aeronContext.NanoClock()).Returns(SystemNanoClock.INSTANCE);
            A.CallTo(() => _aeron.Ctx).Returns(aeronContext);
            var aeronException = new SynchronizationLockException("aeron closed");
            A.CallTo(() => _aeron.Dispose()).Throws(aeronException);

            var publication = A.Fake<Publication>();
            A.CallTo(() => publication.IsConnected).Returns(true);
            A.CallTo(() => publication.Dispose()).Throws(new InvalidOperationException("publication is closed"));

            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _controlResponsePoller.Subscription()).Returns(subscription);
            A.CallTo(() => subscription.Dispose()).Throws(new IndexOutOfRangeException("subscription"));

            A.CallTo(() => _archiveProxy.Pub()).Returns(publication);
            var closeSessionException = new IndexOutOfRangeException();
            A.CallTo(() => _archiveProxy.CloseSession(controlSessionId)).Throws(closeSessionException);

            var context = new Context()
                .AeronClient(_aeron)
                .IdleStrategy(new NoOpIdleStrategy())
                .MessageTimeoutNs(100)
                .Lock(new NoOpLock())
                .ErrorHandler(_errorHandler)
                .OwnsAeronClient(true);
            var aeronArchive = new AeronArchive(context, _controlResponsePoller, _archiveProxy, controlSessionId, archiveId);

            var ex = Assert.Throws<IndexOutOfRangeException>(() => aeronArchive.Dispose());

            Assert.AreSame(closeSessionException, ex);
            A.CallTo(() => _errorHandler.OnError(closeSessionException)).MustHaveHappened();
            A.CallTo(_errorHandler).MustHaveHappenedOnceExactly();
            Assert.AreEqual(aeronException, ex.GetSuppressed()[0]);
        }
    }
}
