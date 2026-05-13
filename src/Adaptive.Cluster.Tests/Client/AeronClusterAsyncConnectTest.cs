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

using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Aeron.Security;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using FakeItEasy;
using NUnit.Framework;
using AeronType = Adaptive.Aeron.Aeron;
using AsyncConnectState = Adaptive.Cluster.Client.AeronCluster.AsyncConnect.AsyncConnectState;

namespace Adaptive.Cluster.Tests.Client
{
    public class AeronClusterAsyncConnectTest
    {
        private const long OneHourInNanos = 3_600_000_000_000L;

        private AeronType _aeron;
        private AeronType.Context _aeronContext;
        private AeronCluster.Context _context;

        [SetUp]
        public void Before()
        {
            _aeron = A.Fake<AeronType>();

            _aeronContext = new AeronType.Context().NanoClock(SystemNanoClock.INSTANCE);

            A.CallTo(() => _aeron.Ctx).Returns(_aeronContext);

            _context = A.Fake<AeronCluster.Context>(o => o.CallsBaseMethods());
            _context
                .AeronClient(_aeron)
                .OwnsAeronClient(false)
                .EgressChannel("aeron:udp?endpoint=localhost:0")
                .EgressStreamId(42)
                .IngressChannel("aeron:udp?endpoint=replace-me:5555")
                .IngressStreamId(-19)
                .CredentialsSupplier(new NullCredentialsSupplier())
                .IdleStrategy(new NoOpIdleStrategy());
        }

        [Test]
        public void InitialState()
        {
            var asyncConnect = new AeronCluster.AsyncConnect(_context, 1L);
            Assert.AreEqual(AsyncConnectState.CREATE_EGRESS_SUBSCRIPTION, asyncConnect.State());
            Assert.AreEqual((int)AsyncConnectState.CREATE_EGRESS_SUBSCRIPTION, asyncConnect.Step());
        }

        [Test]
        public void ShouldCloseAsyncSubscription()
        {
            const long subscriptionId = 999L;
            A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                .Returns(subscriptionId);
            A.CallTo(() => _aeron.GetSubscription(subscriptionId)).Returns((Subscription)null);

            var asyncConnect = new AeronCluster.AsyncConnect(
                _context,
                _aeronContext.NanoClock().NanoTime() + OneHourInNanos
            );

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_EGRESS_SUBSCRIPTION, asyncConnect.State());

            asyncConnect.Dispose();

            A.CallTo(() => _aeron.Ctx)
                .MustHaveHappened()
                .Then(
                    A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _aeron.GetSubscription(subscriptionId)).MustHaveHappened())
                .Then(A.CallTo(() => _aeron.AsyncRemoveSubscription(subscriptionId)).MustHaveHappened());
            A.CallTo(() => _context.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void ShouldCloseEgressSubscription()
        {
            const long subscriptionId = -4343L;
            A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                .Returns(subscriptionId);
            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _aeron.GetSubscription(subscriptionId)).Returns(subscription);

            var asyncConnect = new AeronCluster.AsyncConnect(
                _context,
                _aeronContext.NanoClock().NanoTime() + OneHourInNanos
            );

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            asyncConnect.Dispose();
            A.CallTo(() => subscription.Dispose()).MustHaveHappened();
            A.CallTo(() => _context.Dispose()).MustHaveHappenedOnceExactly();
            A.CallTo(() => _aeron.AsyncRemoveSubscription(subscriptionId)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldCloseAsyncPublication()
        {
            const long subscriptionId = 87L;
            A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                .Returns(subscriptionId);
            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _aeron.GetSubscription(subscriptionId)).Returns(subscription);

            _context.IsIngressExclusive(true);
            const long publicationId = long.MaxValue;
            A.CallTo(() => _aeron.AsyncAddExclusivePublication(_context.IngressChannel(), _context.IngressStreamId()))
                .Returns(publicationId);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId)).Returns((ExclusivePublication)null);

            var asyncConnect = new AeronCluster.AsyncConnect(
                _context,
                _aeronContext.NanoClock().NanoTime() + OneHourInNanos
            );

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            asyncConnect.Dispose();

            A.CallTo(() => _aeron.Ctx)
                .MustHaveHappened()
                .Then(
                    A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _aeron.GetSubscription(subscriptionId)).MustHaveHappened())
                .Then(
                    A.CallTo(() =>
                            _aeron.AsyncAddExclusivePublication(_context.IngressChannel(), _context.IngressStreamId())
                        )
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _aeron.GetExclusivePublication(publicationId)).MustHaveHappened())
                .Then(A.CallTo(() => _aeron.AsyncRemovePublication(publicationId)).MustHaveHappened())
                .Then(A.CallTo(() => subscription.Dispose()).MustHaveHappened());
            A.CallTo(() => _context.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void ShouldCloseIngressPublicationsOnMembers()
        {
            const long subscriptionId = 42L;
            A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                .Returns(subscriptionId);
            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _aeron.GetSubscription(subscriptionId)).Returns(subscription);

            const int ingressStreamId = 878;
            _context
                .IsIngressExclusive(true)
                .IngressEndpoints("0=localhost:20000,1=localhost:20001,2=localhost:20002")
                .IngressStreamId(ingressStreamId);
            const long publicationId1 = -6342756432L;
            var publication1 = A.Fake<ExclusivePublication>();
            A.CallTo(() => _aeron.AsyncAddExclusivePublication("aeron:udp?endpoint=localhost:20000", ingressStreamId))
                .Returns(publicationId1);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId1))
                .ReturnsNextFromSequence((ExclusivePublication)null, publication1);
            const long publicationId2 = AeronType.NULL_VALUE;
            A.CallTo(() => _aeron.AsyncAddExclusivePublication("aeron:udp?endpoint=localhost:20001", ingressStreamId))
                .Returns(publicationId2);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId2)).Returns((ExclusivePublication)null);
            const long publicationId3 = 573495L;
            A.CallTo(() => _aeron.AsyncAddExclusivePublication("aeron:udp?endpoint=localhost:20002", ingressStreamId))
                .Returns(publicationId3);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId3)).Returns((ExclusivePublication)null);

            var asyncConnect = new AeronCluster.AsyncConnect(
                _context,
                _aeronContext.NanoClock().NanoTime() + OneHourInNanos
            );

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            const int iterations = 10;
            for (int i = 0; i < iterations; i++)
            {
                Assert.IsNull(asyncConnect.Poll());
                Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());
            }

            A.CallTo(() => _aeron.AsyncAddExclusivePublication("aeron:udp?endpoint=localhost:20000", ingressStreamId))
                .MustHaveHappenedANumberOfTimesMatching(n => n <= 1);
            A.CallTo(() => _aeron.AsyncAddExclusivePublication("aeron:udp?endpoint=localhost:20001", ingressStreamId))
                .MustHaveHappened(iterations, Times.Exactly);
            A.CallTo(() => _aeron.AsyncAddExclusivePublication("aeron:udp?endpoint=localhost:20002", ingressStreamId))
                .MustHaveHappenedANumberOfTimesMatching(n => n <= 1);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId1)).MustHaveHappened(2, Times.Exactly);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId2)).MustHaveHappened(iterations, Times.Exactly);
            A.CallTo(() => _aeron.GetExclusivePublication(publicationId3)).MustHaveHappened(iterations, Times.Exactly);

            asyncConnect.Dispose();

            A.CallTo(() => subscription.Dispose()).MustHaveHappened();
            A.CallTo(subscription).MustHaveHappenedOnceExactly();
            A.CallTo(() => publication1.Dispose()).MustHaveHappened();
            A.CallTo(() => _context.Dispose()).MustHaveHappenedOnceExactly();
            A.CallTo(() => _aeron.AsyncRemovePublication(publicationId3))
                .MustHaveHappenedANumberOfTimesMatching(n => n <= 1);
            A.CallTo(() => _aeron.AsyncRemoveSubscription(subscriptionId)).MustNotHaveHappened();
            A.CallTo(() => _aeron.AsyncRemovePublication(publicationId1)).MustNotHaveHappened();
            A.CallTo(() => _aeron.AsyncRemovePublication(publicationId2)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldCloseIngressPublication()
        {
            const long subscriptionId = 42L;
            A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                .Returns(subscriptionId);
            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _aeron.GetSubscription(subscriptionId)).Returns(subscription);

            _context.IsIngressExclusive(false);
            const long publicationId = -6342756432L;
            A.CallTo(() => _aeron.AsyncAddPublication(_context.IngressChannel(), _context.IngressStreamId()))
                .Returns(publicationId);
            var publication = A.Fake<ConcurrentPublication>();
            A.CallTo(() => _aeron.GetPublication(publicationId)).Returns(publication);

            var asyncConnect = new AeronCluster.AsyncConnect(
                _context,
                _aeronContext.NanoClock().NanoTime() + OneHourInNanos
            );

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.AWAIT_PUBLICATION_CONNECTED, asyncConnect.State());

            asyncConnect.Dispose();
            A.CallTo(() => publication.Dispose()).MustHaveHappened();
            A.CallTo(() => subscription.Dispose()).MustHaveHappened();
            A.CallTo(() => _context.Dispose()).MustHaveHappenedOnceExactly();
            A.CallTo(() => _aeron.AsyncRemovePublication(publicationId)).MustNotHaveHappened();
            A.CallTo(() => _aeron.AsyncRemoveSubscription(subscriptionId)).MustNotHaveHappened();
        }

        [Test]
#pragma warning disable CA1506
        public void ShouldConnectViaIngressChannel()
        {
            const long subscriptionId = 42L;
            long correlationIdSeq = 0;
            A.CallTo(() => _aeron.NextCorrelationId()).ReturnsLazily(() => ++correlationIdSeq);
            A.CallTo(() => _aeron.AsyncAddSubscription(_context.EgressChannel(), _context.EgressStreamId()))
                .Returns(subscriptionId);
            var subscription = A.Fake<Subscription>();
            A.CallTo(() => _aeron.GetSubscription(subscriptionId)).Returns(subscription);

            _context.IsIngressExclusive(false);
            const long publicationId = -19L;
            A.CallTo(() => _aeron.AsyncAddPublication(_context.IngressChannel(), _context.IngressStreamId()))
                .Returns(publicationId);
            var publication = A.Fake<ConcurrentPublication>();
            A.CallTo(() => _aeron.GetPublication(publicationId)).Returns(publication);

            var asyncConnect = new AeronCluster.AsyncConnect(
                _context,
                _aeronContext.NanoClock().NanoTime() + OneHourInNanos
            );

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CREATE_INGRESS_PUBLICATIONS, asyncConnect.State());

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.AWAIT_PUBLICATION_CONNECTED, asyncConnect.State());

            const string responseChannel = "aeron:udp?endpoint=localhost:8888";
            A.CallTo(() => subscription.TryResolveChannelEndpointPort()).Returns(responseChannel);
            A.CallTo(() => publication.IsConnected).Returns(true);

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.SEND_MESSAGE, asyncConnect.State());
            long sendMessageCorrelationId = correlationIdSeq;

            A.CallTo(() => publication.Offer(A<IDirectBuffer>._, 0, A<int>._, A<ReservedValueSupplier>._)).Returns(8L);

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.POLL_RESPONSE, asyncConnect.State());

            var responseBuffer = new UnsafeBuffer(new byte[256]);
            var headerEncoder = new MessageHeaderEncoder();
            var sessionEventEncoder = new SessionEventEncoder();
            const long clusterSessionId = 888L;
            const long leadershipTermId = 5L;
            const int leaderMemberId = 2;
            sessionEventEncoder
                .WrapAndApplyHeader(responseBuffer, DataHeaderFlyweight.HEADER_LENGTH, headerEncoder)
                .ClusterSessionId(clusterSessionId)
                .CorrelationId(sendMessageCorrelationId)
                .LeadershipTermId(leadershipTermId)
                .LeaderMemberId(leaderMemberId)
                .Code(EventCode.OK)
                .Version(AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION)
                .LeaderHeartbeatTimeoutNs(SessionEventEncoder.LeaderHeartbeatTimeoutNsNullValue())
                .Detail("you are now connected");
            var egressImage = A.Fake<Image>();
            var header = new Header(1, 16, egressImage) { Buffer = responseBuffer, Offset = 0 };
            var headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.Wrap(responseBuffer, 0, DataHeaderFlyweight.HEADER_LENGTH);
            headerFlyweight.Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
            A.CallTo(() => subscription.ControlledPoll(A<IControlledFragmentHandler>._, A<int>._))
                .ReturnsLazily(call =>
                {
                    var assembler = call.GetArgument<IControlledFragmentHandler>(0);
                    assembler.OnFragment(
                        responseBuffer,
                        DataHeaderFlyweight.HEADER_LENGTH,
                        sessionEventEncoder.EncodedLength(),
                        header
                    );
                    return 1;
                });

            Assert.IsNull(asyncConnect.Poll());
            Assert.AreEqual(AsyncConnectState.CONCLUDE_CONNECT, asyncConnect.State());

            var aeronCluster = asyncConnect.Poll();
            Assert.IsNotNull(aeronCluster);
            Assert.AreEqual(leadershipTermId, aeronCluster.LeadershipTermId);
            Assert.AreEqual(leaderMemberId, aeronCluster.LeaderMemberId);
            Assert.AreEqual(clusterSessionId, aeronCluster.ClusterSessionId);
            Assert.AreEqual(
                2 * AeronCluster.Configuration.LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS,
                _context.NewLeaderTimeoutNs()
            );

            asyncConnect.Dispose();
            A.CallTo(() => publication.Dispose()).MustNotHaveHappened();
            A.CallTo(() => subscription.Dispose()).MustNotHaveHappened();

            A.CallTo(() => publication.TryClaim(A<int>._, A<BufferClaim>._))
                .ReturnsLazily(call =>
                {
                    int length = call.GetArgument<int>(0);
                    var bufferClaim = call.GetArgument<BufferClaim>(1);
                    bufferClaim.Wrap(
                        responseBuffer,
                        0,
                        BitUtil.Align(DataHeaderFlyweight.HEADER_LENGTH + length, DataHeaderFlyweight.HEADER_LENGTH)
                    );
                    return 42L;
                });
            aeronCluster.Dispose();
            Assert.IsTrue(aeronCluster.Closed);
            A.CallTo(() => publication.TryClaim(A<int>._, A<BufferClaim>._))
                .MustHaveHappened()
                .Then(A.CallTo(() => subscription.Dispose()).MustHaveHappened())
                .Then(A.CallTo(() => publication.Dispose()).MustHaveHappened());
            A.CallTo(() => _context.Dispose()).MustHaveHappenedOnceExactly();
        }
#pragma warning restore CA1506
    }
}
