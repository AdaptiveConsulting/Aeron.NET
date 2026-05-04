using System;
using System.Collections.Generic;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using FakeItEasy;
using NUnit.Framework;
using AeronType = Adaptive.Aeron.Aeron;

namespace Adaptive.Cluster.Tests.Client
{
    [FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
    public class AeronClusterTest
    {
        private const string INGRESS_ENDPOINTS = "foo:1000,bar:1000,baz:1000";
        private const int CLUSTER_SESSION_ID = 123;

        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(new byte[1024]);
        private readonly UnsafeBuffer _appMessage = new UnsafeBuffer(new byte[8]);
        private readonly IEgressListener _egressListener = A.Fake<IEgressListener>();
        private readonly AeronType _aeron = A.Fake<AeronType>();
        private readonly ExclusivePublication _ingressPublication = A.Fake<ExclusivePublication>();
        private readonly Subscription _egressSubscription = A.Fake<Subscription>();
        private readonly Image _egressImage = A.Fake<Image>();
        private AeronType.Context _aeronContext;
        private AeronCluster.Context _context;
        private AeronCluster _aeronCluster;
        private long _nanoTime;
        private int _leadershipTermId = 2;
        private int _leaderMemberId = 1;
        private bool _newLeaderEventPending;

        [SetUp]
        public void SetUp()
        {
            var nanoClock = A.Fake<INanoClock>();
            A.CallTo(() => nanoClock.NanoTime()).ReturnsLazily(() => _nanoTime);

            _aeronContext = new AeronType.Context()
                .NanoClock(nanoClock)
                .SubscriberErrorHandler(RethrowingErrorHandler.INSTANCE);

            A.CallTo(() => _aeron.Ctx).Returns(_aeronContext);

            _context = new AeronCluster.Context()
                .AeronClient(_aeron)
                .OwnsAeronClient(false)
                .EgressChannel("aeron:udp?endpoint=localhost:0")
                .IngressChannel("aeron:udp")
                .IdleStrategy(new NoOpIdleStrategy())
                .EgressListener(_egressListener)
                .NewLeaderTimeoutNs(TimeSpan.FromSeconds(1).Ticks * 100); // 1 second in nanos

            const long _ingressPublicationRegistrationId = 42L;
            A.CallTo(() => _ingressPublication.RegistrationId).Returns(_ingressPublicationRegistrationId);
            A.CallTo(() => _aeron.AsyncAddExclusivePublication(_context.IngressChannel(), _context.IngressStreamId()))
                .Returns(_ingressPublicationRegistrationId);
            A.CallTo(() => _aeron.GetExclusivePublication(_ingressPublicationRegistrationId))
                .Returns(_ingressPublication);

            _context.Conclude();

            A.CallTo(() => _egressSubscription.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(call =>
            {
                if (_newLeaderEventPending)
                {
                    _newLeaderEventPending = false;

                    int offset = DataHeaderFlyweight.HEADER_LENGTH;
                    FrameDescriptor.FrameFlags(_buffer, 0, FrameDescriptor.UNFRAGMENTED);

                    var newLeaderEventEncoder = new NewLeaderEventEncoder();
                    newLeaderEventEncoder.WrapAndApplyHeader(_buffer, offset, new MessageHeaderEncoder());
                    newLeaderEventEncoder.ClusterSessionId(CLUSTER_SESSION_ID);
                    newLeaderEventEncoder.LeadershipTermId(++_leadershipTermId);
                    newLeaderEventEncoder.LeaderMemberId(++_leaderMemberId);
                    newLeaderEventEncoder.IngressEndpoints(INGRESS_ENDPOINTS);

                    int length = MessageHeaderEncoder.ENCODED_LENGTH + newLeaderEventEncoder.EncodedLength();

                    var header = new Header(0, 0, _egressImage);
                    header.Buffer = _buffer;

                    var handler = call.GetArgument<IFragmentHandler>(0);
                    handler.OnFragment(_buffer, offset, length, header);

                    return 1;
                }

                return 0;
            });

            _aeronCluster = new AeronCluster(
                _context,
                new MessageHeaderEncoder(),
                _ingressPublication,
                _egressSubscription,
                _egressImage,
                new Map<int, MemberIngress>(),
                CLUSTER_SESSION_ID,
                _leadershipTermId,
                _leaderMemberId);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ShouldCloseItselfWhenDisconnectedForLongerThanNewLeaderTimeout(bool withAppMessages)
        {
            MakeIngressPublicationReturn(Publication.NOT_CONNECTED);
            if (withAppMessages)
            {
                Assert.AreEqual(Publication.NOT_CONNECTED, _aeronCluster.Offer(_appMessage, 0, 8));
            }
            else
            {
                Assert.IsFalse(_aeronCluster.SendKeepAlive());
            }

            _nanoTime += _context.NewLeaderTimeoutNs() - 1;

            Assert.AreEqual(0, _aeronCluster.PollEgress());
            Assert.IsFalse(_aeronCluster.Closed);

            _nanoTime += 1;

            Assert.AreEqual(1, _aeronCluster.PollEgress());
            Assert.IsTrue(_aeronCluster.Closed);
        }

        [Test]
        public void ShouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication()
        {
            MakeIngressPublicationReturn(Publication.MAX_POSITION_EXCEEDED);
            Assert.AreEqual(Publication.MAX_POSITION_EXCEEDED, _aeronCluster.Offer(_appMessage, 0, 8));
            A.CallTo(() => _ingressPublication.Dispose()).MustHaveHappened();
            Assert.AreEqual(1, _aeronCluster.PollStateChanges());
            Assert.IsTrue(_aeronCluster.Closed);
        }

        public static IEnumerable<TestCaseData> ShouldStayConnectedAfterSuccessfulFailoverCases()
        {
            yield return new TestCaseData(false, false);
            yield return new TestCaseData(false, true);
            yield return new TestCaseData(true, false);
            yield return new TestCaseData(true, true);
        }

        [TestCaseSource(nameof(ShouldStayConnectedAfterSuccessfulFailoverCases))]
        public void ShouldStayConnectedAfterSuccessfulFailover(bool withIngressDisconnect, bool withAppMessages)
        {
            long initialResult = withIngressDisconnect ? Publication.NOT_CONNECTED : 128L;
            MakeIngressPublicationReturn(initialResult);
            if (withAppMessages)
            {
                Assert.AreEqual(initialResult, _aeronCluster.Offer(_appMessage, 0, 8));
            }
            else
            {
                Assert.AreEqual(!withIngressDisconnect, _aeronCluster.SendKeepAlive());
            }

            _nanoTime += _context.NewLeaderTimeoutNs() - 1;

            MakeEgressSubscriptionDeliverNewLeaderEvent();
            Assert.AreEqual(1, _aeronCluster.PollEgress());
            A.CallTo(() => _egressListener.OnNewLeader(CLUSTER_SESSION_ID, _leadershipTermId, _leaderMemberId, INGRESS_ENDPOINTS))
                .MustHaveHappened();
            Assert.AreEqual(0, _aeronCluster.PollEgress());

            _nanoTime += _context.MessageTimeoutNs() - 1;

            MakeIngressPublicationReturn(256L);
            if (withAppMessages)
            {
                Assert.AreEqual(256L, _aeronCluster.Offer(_appMessage, 0, 8));
            }
            else
            {
                Assert.IsTrue(_aeronCluster.SendKeepAlive());
            }

            _nanoTime += 1;

            Assert.AreEqual(0, _aeronCluster.PollEgress());
            Assert.IsFalse(_aeronCluster.Closed);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ShouldCloseItselfWhenUnableToSendMessageForLongerThanNewLeaderConnectionTimeout(bool withAppMessages)
        {
            MakeIngressPublicationReturn(Publication.NOT_CONNECTED);
            if (withAppMessages)
            {
                Assert.AreEqual(Publication.NOT_CONNECTED, _aeronCluster.Offer(_appMessage, 0, 8));
            }
            else
            {
                Assert.IsFalse(_aeronCluster.SendKeepAlive());
            }

            _nanoTime += _context.NewLeaderTimeoutNs() / 2;

            MakeEgressSubscriptionDeliverNewLeaderEvent();

            Assert.AreEqual(1, _aeronCluster.PollEgress());
            Assert.IsFalse(_aeronCluster.Closed);

            _nanoTime += _context.MessageTimeoutNs() - 1;
            if (withAppMessages)
            {
                Assert.AreEqual(Publication.NOT_CONNECTED, _aeronCluster.Offer(_appMessage, 0, 8));
            }
            else
            {
                Assert.IsFalse(_aeronCluster.SendKeepAlive());
            }

            _nanoTime += 1;

            Assert.AreEqual(1, _aeronCluster.PollEgress());
            Assert.IsTrue(_aeronCluster.Closed);
        }

        [Test]
        public void ShouldCloseIngressPublicationWhenEgressImageCloses()
        {
            // in CONNECTED state
            A.CallTo(() => _egressImage.Closed).Returns(true);
            Assert.AreEqual(1, _aeronCluster.PollEgress());
            A.CallTo(() => _ingressPublication.Dispose()).MustHaveHappened();

            A.CallTo(() => _egressImage.Closed).Returns(false);
            MakeEgressSubscriptionDeliverNewLeaderEvent();
            Assert.AreEqual(1, _aeronCluster.PollEgress());
            A.CallTo(() => _ingressPublication.Dispose()).MustHaveHappenedTwiceOrMore();

            // and in AWAIT_NEW_LEADER_CONNECTION state too
            A.CallTo(() => _egressImage.Closed).Returns(true);
            Assert.AreEqual(1, _aeronCluster.PollEgress());
            A.CallTo(() => _ingressPublication.Dispose())
                .MustHaveHappened(3, Times.OrMore);
        }

        private void MakeIngressPublicationReturn(long result)
        {
            if (result > 0)
            {
                A.CallTo(() => _ingressPublication.TryClaim(A<int>._, A<BufferClaim>._))
                    .ReturnsLazily(call =>
                    {
                        int length = call.GetArgument<int>(0);
                        length = BitUtil.Align(DataHeaderFlyweight.HEADER_LENGTH + length, FrameDescriptor.FRAME_ALIGNMENT);
                        var _bufferClaim = call.GetArgument<BufferClaim>(1);
                        _bufferClaim.Wrap(_buffer, 0, length);
                        return result;
                    });
            }
            else
            {
                A.CallTo(() => _ingressPublication.TryClaim(A<int>._, A<BufferClaim>._)).Returns(result);
            }

            A.CallTo(() => _ingressPublication.Offer(
                    A<IDirectBuffer>._, A<int>._, A<int>._,
                    A<IDirectBuffer>._, A<int>._, A<int>._,
                    A<ReservedValueSupplier>._)).Returns(result);
        }

        private void MakeEgressSubscriptionDeliverNewLeaderEvent()
        {
            _newLeaderEventPending = true;
        }
    }
}
