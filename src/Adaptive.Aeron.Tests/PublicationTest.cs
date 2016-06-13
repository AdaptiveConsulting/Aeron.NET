using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class PublicationTest
    {
        private const string Channel = "aeron:udp?endpoint=localhost:40124";
        private const int StreamID1 = 2;
        private const int SessionID1 = 13;
        private const int TermID1 = 1;
        private const int CorrelationID = 2000;
        private const int SendBufferCapacity = 1024;

        private byte[] _sendBuffer;
        private UnsafeBuffer _atomicSendBuffer;
        private UnsafeBuffer _logMetaDataBuffer;
        private UnsafeBuffer[] _termBuffers;
        private UnsafeBuffer[] _termMetaDataBuffers;
        private UnsafeBuffer[] _buffers;

        private ClientConductor _conductor;
        private LogBuffers _logBuffers;
        private IReadablePosition _publicationLimit;
        private Publication _publication;

        [SetUp]
        public void SetUp()
        {
            _sendBuffer = new byte[SendBufferCapacity];
            _atomicSendBuffer = new UnsafeBuffer(_sendBuffer);
            _logMetaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);
            _termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
            _termMetaDataBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
            _buffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT*2 + 1];

            _conductor = A.Fake<ClientConductor>();
            _logBuffers = A.Fake<LogBuffers>();
            _publicationLimit = A.Fake<IReadablePosition>();

            A.CallTo(() => _publicationLimit.Volatile).Returns(2*SendBufferCapacity);
            A.CallTo(() => _logBuffers.AtomicBuffers()).Returns(_buffers);
            A.CallTo(() => _logBuffers.TermLength()).Returns(LogBufferDescriptor.TERM_MIN_LENGTH);

            LogBufferDescriptor.InitialTermId(_logMetaDataBuffer, TermID1);
            LogBufferDescriptor.TimeOfLastStatusMessage(_logMetaDataBuffer, 0);

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                _termBuffers[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_MIN_LENGTH]);
                _termMetaDataBuffers[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_META_DATA_LENGTH]);

                _buffers[i] = _termBuffers[i];
                _buffers[i + LogBufferDescriptor.PARTITION_COUNT] = _termMetaDataBuffers[i];
            }
            _buffers[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX] = _logMetaDataBuffer;

            _publication = new Publication(_conductor, Channel, StreamID1, SessionID1, _publicationLimit, _logBuffers, CorrelationID);

            _publication.IncRef();

            LogBufferDescriptor.InitialiseTailWithTermId(_termMetaDataBuffers[0], TermID1);
        }

        [Test]
        public void ShouldEnsureThePublicationIsOpenBeforeReadingPosition()
        {
            _publication.Dispose();
            Assert.AreEqual(_publication.Position, Publication.CLOSED);
        }

        [Test]
        public void ShouldEnsureThePublicationIsOpenBeforeOffer()
        {
            _publication.Dispose();
            Assert.True(_publication.IsClosed);
            Assert.AreEqual(_publication.Offer(_atomicSendBuffer), Publication.CLOSED);
        }

        [Test]
        public void ShouldEnsureThePublicationIsOpenBeforeClaim()
        {
            _publication.Dispose();
            var bufferClaim = new BufferClaim();
            Assert.AreEqual(_publication.TryClaim(SendBufferCapacity, bufferClaim), Publication.CLOSED);
        }

        [Test]
        public void ShouldReportThatPublicationHasNotBeenConnectedYet()
        {
            A.CallTo(() => _publicationLimit.Volatile).Returns(0);
            A.CallTo(() => _conductor.IsPublicationConnected(A<long>._)).Returns(false);

            Assert.False(_publication.IsConnected);
        }

        [Test]
        public void ShouldReportThatPublicationHasBeenConnectedYet()
        {
            A.CallTo(() => _conductor.IsPublicationConnected(A<long>._)).Returns(true);
            Assert.True(_publication.IsConnected);
        }

        [Test]
        public void ShouldReportInitialPosition()
        {
            Assert.AreEqual(_publication.Position, 0L);
        }

        [Test]
        public void ShouldReportMaxMessageLength()
        {
            Assert.AreEqual(_publication.MaxMessageLength, FrameDescriptor.ComputeMaxMessageLength(LogBufferDescriptor.TERM_MIN_LENGTH));
        }

        [Test]
        public void ShouldNotUnmapBuffersBeforeLastRelease()
        {
            _publication.IncRef();
            _publication.Dispose();

            A.CallTo(() => _logBuffers.Dispose()).MustNotHaveHappened();
        }

        [Test]
        public void ShouldUnmapBuffersWithMultipleReferences()
        {
            _publication.IncRef();
            _publication.Dispose();

            _publication.Dispose();
            A.CallTo(() => _conductor.ReleasePublication(_publication)).MustHaveHappened(Repeated.Exactly.Once);
        }

        [Test]
        public void ShouldReleaseResourcesIdempotently()
        {
            _publication.Dispose();
            _publication.Dispose();

            A.CallTo(() => _conductor.ReleasePublication(_publication)).MustHaveHappened(Repeated.Exactly.Once);
        }
    }
}