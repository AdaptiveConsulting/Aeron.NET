using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class PublicationTest
    {
        private const string CHANNEL = "udp://localhost:40124";
        private const int STREAM_ID_1 = 2;
        private const int SESSION_ID_1 = 13;
        private const int TERM_ID_1 = 1;
        private const int CORRELATION_ID = 2000;
        private const int SEND_BUFFER_CAPACITY = 1024;

        private byte[] SendBuffer;
        private UnsafeBuffer AtomicSendBuffer;
        private UnsafeBuffer LogMetaDataBuffer;
        private UnsafeBuffer[] TermBuffers;
        private UnsafeBuffer[] TermMetaDataBuffers;
        private UnsafeBuffer[] Buffers;

        private ClientConductor Conductor;
        private LogBuffers LogBuffers;
        private IReadablePosition PublicationLimit;
        private Publication Publication;

        [SetUp]
        public virtual void SetUp()
        {
            SendBuffer = new byte[SEND_BUFFER_CAPACITY];
            AtomicSendBuffer = new UnsafeBuffer(SendBuffer);
            LogMetaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);
            TermBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
            TermMetaDataBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
            Buffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT*2 + 1];

            Conductor = A.Fake<ClientConductor>();
            LogBuffers = A.Fake<LogBuffers>();
            PublicationLimit = A.Fake<IReadablePosition>();

            A.CallTo(() => PublicationLimit.Volatile).Returns(2*SEND_BUFFER_CAPACITY);
            A.CallTo(() => LogBuffers.AtomicBuffers()).Returns(Buffers);
            A.CallTo(() => LogBuffers.TermLength()).Returns(LogBufferDescriptor.TERM_MIN_LENGTH);
            
            LogBufferDescriptor.InitialTermId(LogMetaDataBuffer, TERM_ID_1);
            LogBufferDescriptor.TimeOfLastStatusMessage(LogMetaDataBuffer, 0);

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                TermBuffers[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_MIN_LENGTH]);
                TermMetaDataBuffers[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_META_DATA_LENGTH]);

                Buffers[i] = TermBuffers[i];
                Buffers[i + LogBufferDescriptor.PARTITION_COUNT] = TermMetaDataBuffers[i];
            }
            Buffers[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX] = LogMetaDataBuffer;

            Publication = new Publication(Conductor, CHANNEL, STREAM_ID_1, SESSION_ID_1, PublicationLimit, LogBuffers, CORRELATION_ID);

            Publication.IncRef();

            LogBufferDescriptor.InitialiseTailWithTermId(TermMetaDataBuffers[0], TERM_ID_1);
        }

        [Test]
        public virtual void ShouldEnsureThePublicationIsOpenBeforeReadingPosition()
        {
            Publication.Dispose();
            Assert.AreEqual(Publication.Position(), (Publication.CLOSED));
        }

        [Test]
        public virtual void ShouldEnsureThePublicationIsOpenBeforeOffer()
        {
            Publication.Dispose();
            Assert.True(Publication.Closed);
            Assert.AreEqual(Publication.Offer(AtomicSendBuffer), Publication.CLOSED);
        }

        [Test]
        public virtual void ShouldEnsureThePublicationIsOpenBeforeClaim()
        {
            Publication.Dispose();
            var bufferClaim = new BufferClaim();
            Assert.AreEqual(Publication.TryClaim(SEND_BUFFER_CAPACITY, bufferClaim), Publication.CLOSED);
        }

        [Test]
        public virtual void ShouldReportThatPublicationHasNotBeenConnectedYet()
        {
            A.CallTo(() => PublicationLimit.Volatile).Returns(0);
            A.CallTo(() => Conductor.IsPublicationConnected(A<long>._)).Returns(false);
            
            Assert.False(Publication.Connected);
        }

        [Test]
        public virtual void ShouldReportThatPublicationHasBeenConnectedYet()
        {
            A.CallTo(() => Conductor.IsPublicationConnected(A<long>._)).Returns(true);
            Assert.True(Publication.Connected);
        }

        [Test]
        public virtual void ShouldReportInitialPosition()
        {
            Assert.AreEqual(Publication.Position(), 0L);
        }

        [Test]
        public virtual void ShouldReportMaxMessageLength()
        {
            Assert.AreEqual(Publication.MaxMessageLength(), FrameDescriptor.ComputeMaxMessageLength(LogBufferDescriptor.TERM_MIN_LENGTH));
        }

        [Test]
        public virtual void ShouldNotUnmapBuffersBeforeLastRelease()
        {
            Publication.IncRef();
            Publication.Dispose();

            A.CallTo(()=>LogBuffers.Dispose()).MustNotHaveHappened();
        }

        [Test]
        public virtual void ShouldUnmapBuffersWithMultipleReferences()
        {
            Publication.IncRef();
            Publication.Dispose();

            Publication.Dispose();
            A.CallTo(() => Conductor.ReleasePublication(Publication)).MustHaveHappened(Repeated.Exactly.Once);
        }

        [Test]
        public virtual void ShouldReleaseResourcesIdempotently()
        {
            Publication.Dispose();
            Publication.Dispose();

            A.CallTo(() => Conductor.ReleasePublication(Publication)).MustHaveHappened(Repeated.Exactly.Once);
        }
    }
}