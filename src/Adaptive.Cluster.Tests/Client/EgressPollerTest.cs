using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using FakeItEasy;
using NUnit.Framework;
using ArchiveMessageHeaderEncoder = Adaptive.Archiver.Codecs.MessageHeaderEncoder;
using ClusterMessageHeaderEncoder = Adaptive.Cluster.Codecs.MessageHeaderEncoder;

namespace Adaptive.Cluster.Tests.Client
{
    public class EgressPollerTest
    {
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(new byte[1024]);
        private readonly Header _header = new Header(42, 16);
        private readonly Subscription _subscription = A.Fake<Subscription>();
        private EgressPoller _egressPoller;

        [SetUp]
        public void SetUp()
        {
            _egressPoller = new EgressPoller(_subscription, 10);
        }

        [Test]
        public void ShouldIgnoreUnknownMessageSchema()
        {
            const int offset = 64;
            var controlResponseEncoder = new ControlResponseEncoder();
            var messageHeaderEncoder = new ArchiveMessageHeaderEncoder();
            controlResponseEncoder
                .WrapAndApplyHeader(_buffer, offset, messageHeaderEncoder)
                .CorrelationId(42)
                .Code(ControlResponseCode.ERROR)
                .ErrorMessage("test");

            Assert.AreEqual(
                ControlledFragmentHandlerAction.CONTINUE,
                _egressPoller.OnFragment(
                    _buffer,
                    offset,
                    messageHeaderEncoder.EncodedLength() + controlResponseEncoder.EncodedLength(),
                    _header));
            Assert.IsFalse(_egressPoller.IsPollComplete());
        }

        [Test]
        public void ShouldHandleSessionMessage()
        {
            const int offset = 16;
            var encoder = new SessionMessageHeaderEncoder();
            var messageHeaderEncoder = new ClusterMessageHeaderEncoder();
            const long clusterSessionId = 7777L;
            const long leadershipTermId = 5L;
            encoder
                .WrapAndApplyHeader(_buffer, offset, messageHeaderEncoder)
                .ClusterSessionId(clusterSessionId)
                .LeadershipTermId(leadershipTermId);

            Assert.AreEqual(
                ControlledFragmentHandlerAction.BREAK,
                _egressPoller.OnFragment(
                    _buffer,
                    offset,
                    messageHeaderEncoder.EncodedLength() + encoder.EncodedLength(),
                    _header));
            Assert.IsTrue(_egressPoller.IsPollComplete());
            Assert.AreEqual(clusterSessionId, _egressPoller.ClusterSessionId());
            Assert.AreEqual(leadershipTermId, _egressPoller.LeadershipTermId());

            Assert.AreEqual(
                ControlledFragmentHandlerAction.ABORT,
                _egressPoller.OnFragment(
                    _buffer,
                    offset,
                    messageHeaderEncoder.EncodedLength() + encoder.EncodedLength(),
                    _header));
        }
    }
}
