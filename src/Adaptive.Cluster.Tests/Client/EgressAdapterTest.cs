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
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Cluster.Tests.Client
{
    public class EgressAdapterTest
    {
        private UnsafeBuffer _buffer;
        private MessageHeaderEncoder _messageHeaderEncoder;
        private SessionMessageHeaderEncoder _sessionMessageHeaderEncoder;
        private SessionEventEncoder _sessionEventEncoder;
        private NewLeaderEventEncoder _newLeaderEventEncoder;
        private AdminResponseEncoder _adminResponseEncoder;

        [SetUp]
        public void SetUp()
        {
            _buffer = new UnsafeBuffer(new byte[512]);
            _messageHeaderEncoder = new MessageHeaderEncoder();
            _sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
            _sessionEventEncoder = new SessionEventEncoder();
            _newLeaderEventEncoder = new NewLeaderEventEncoder();
            _adminResponseEncoder = new AdminResponseEncoder();
        }

        [Test]
        public void OnFragmentShouldDelegateToEgressListenerOnUnknownSchemaId()
        {
            const ushort schemaId = 17;
            const ushort templateId = 19;
            _messageHeaderEncoder.Wrap(_buffer, 0).SchemaId(schemaId).TemplateId(templateId);

            var listenerExtension = A.Fake<IEgressListenerExtension>();
            var header = new Header(0, 0);
            var adapter = new EgressAdapter(A.Fake<IEgressListener>(), listenerExtension, 0, A.Fake<Subscription>(), 3);

            adapter.OnFragment(_buffer, 0, MessageHeaderDecoder.ENCODED_LENGTH * 2, header);

            A.CallTo(() =>
                    listenerExtension.OnExtensionMessage(
                        A<int>._,
                        templateId,
                        schemaId,
                        0,
                        _buffer,
                        MessageHeaderDecoder.ENCODED_LENGTH,
                        MessageHeaderDecoder.ENCODED_LENGTH
                    )
                )
                .MustHaveHappenedOnceExactly();
            A.CallTo(listenerExtension).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DefaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId()
        {
            var listener = A.Fake<IEgressListener>();
            var adapter = new EgressAdapter(listener, 42, A.Fake<Subscription>(), 5);
            var exception = Assert.Throws<ClusterException>(() => adapter.OnFragment(_buffer, 0, 64, new Header(0, 0)));
            Assert.AreEqual(
                "ERROR - expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=0",
                exception.Message);
        }

        [Test]
        public void OnFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches()
        {
            const int offset = 4;
            const long sessionId = 2973438724L;
            const long timestamp = -46328746238764832L;
            _sessionMessageHeaderEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .ClusterSessionId(sessionId)
                .Timestamp(timestamp);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(0, 0);
            var adapter = new EgressAdapter(egressListener, sessionId, A.Fake<Subscription>(), 3);

            adapter.OnFragment(_buffer, offset, _sessionMessageHeaderEncoder.EncodedLength(), header);

            A.CallTo(() =>
                    egressListener.OnMessage(
                        sessionId,
                        timestamp,
                        _buffer,
                        offset + AeronCluster.SESSION_HEADER_LENGTH,
                        _sessionMessageHeaderEncoder.EncodedLength() - AeronCluster.SESSION_HEADER_LENGTH,
                        header
                    )
                )
                .MustHaveHappenedOnceExactly();
            A.CallTo(egressListener).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void OnFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage()
        {
            const int offset = 18;
            const long sessionId = 21;
            const long timestamp = 1000;
            _sessionMessageHeaderEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .ClusterSessionId(sessionId)
                .Timestamp(timestamp);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(0, 0);
            var adapter = new EgressAdapter(egressListener, -19, A.Fake<Subscription>(), 3);

            adapter.OnFragment(_buffer, offset, _sessionMessageHeaderEncoder.EncodedLength(), header);

            A.CallTo(egressListener).MustNotHaveHappened();
        }

        [Test]
        public void OnFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches()
        {
            const int offset = 8;
            const long clusterSessionId = 42;
            const long correlationId = 777;
            const long leadershipTermId = 6;
            const int leaderMemberId = 3;
            const EventCode eventCode = EventCode.REDIRECT;
            const int version = 18;
            const string eventDetail = "Event details";
            _sessionEventEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .ClusterSessionId(clusterSessionId)
                .CorrelationId(correlationId)
                .LeadershipTermId(leadershipTermId)
                .LeaderMemberId(leaderMemberId)
                .Code(eventCode)
                .Version(version)
                .Detail(eventDetail);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(1, 3);
            var adapter = new EgressAdapter(egressListener, clusterSessionId, A.Fake<Subscription>(), 10);

            adapter.OnFragment(_buffer, offset, _sessionEventEncoder.EncodedLength(), header);

            A.CallTo(() =>
                    egressListener.OnSessionEvent(
                        correlationId,
                        clusterSessionId,
                        leadershipTermId,
                        leaderMemberId,
                        eventCode,
                        eventDetail
                    )
                )
                .MustHaveHappenedOnceExactly();
            A.CallTo(egressListener).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void OnFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionEvent()
        {
            const int offset = 8;
            const long clusterSessionId = 42;
            const long correlationId = 777;
            const long leadershipTermId = 6;
            const int leaderMemberId = 3;
            const EventCode eventCode = EventCode.REDIRECT;
            const int version = 18;
            const string eventDetail = "Event details";
            _sessionEventEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .ClusterSessionId(clusterSessionId)
                .CorrelationId(correlationId)
                .LeadershipTermId(leadershipTermId)
                .LeaderMemberId(leaderMemberId)
                .Code(eventCode)
                .Version(version)
                .Detail(eventDetail);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(0, 0);
            var adapter = new EgressAdapter(egressListener, clusterSessionId + 1, A.Fake<Subscription>(), 3);

            adapter.OnFragment(_buffer, offset, _sessionEventEncoder.EncodedLength(), header);

            A.CallTo(egressListener).MustNotHaveHappened();
        }

        [Test]
        public void OnFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches()
        {
            const int offset = 0;
            const long clusterSessionId = 0;
            const long leadershipTermId = 6;
            const int leaderMemberId = 9999;
            const string ingressEndpoints = "ingress endpoints ...";
            _newLeaderEventEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .LeadershipTermId(leadershipTermId)
                .ClusterSessionId(clusterSessionId)
                .LeaderMemberId(leaderMemberId)
                .IngressEndpoints(ingressEndpoints);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(1, 3);
            var adapter = new EgressAdapter(egressListener, clusterSessionId, A.Fake<Subscription>(), 10);

            adapter.OnFragment(_buffer, offset, _newLeaderEventEncoder.EncodedLength(), header);

            A.CallTo(() =>
                    egressListener.OnNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints)
                )
                .MustHaveHappenedOnceExactly();
            A.CallTo(egressListener).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void OnFragmentIsANoOpIfSessionIdDoesNotMatchOnNewLeader()
        {
            const int offset = 0;
            const long clusterSessionId = -100;
            const long leadershipTermId = 6;
            const int leaderMemberId = 9999;
            const string ingressEndpoints = "ingress endpoints ...";
            _newLeaderEventEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .LeadershipTermId(leadershipTermId)
                .ClusterSessionId(clusterSessionId)
                .LeaderMemberId(leaderMemberId)
                .IngressEndpoints(ingressEndpoints);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(1, 3);
            var adapter = new EgressAdapter(egressListener, 0, A.Fake<Subscription>(), 10);

            adapter.OnFragment(_buffer, offset, _newLeaderEventEncoder.EncodedLength(), header);

            A.CallTo(egressListener).MustNotHaveHappened();
        }

        [Test]
        public void OnFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches()
        {
            const int offset = 24;
            const long clusterSessionId = 18;
            const long correlationId = 3274239749237498239L;
            const AdminRequestType type = AdminRequestType.SNAPSHOT;
            const AdminResponseCode responseCode = AdminResponseCode.UNAUTHORISED_ACCESS;
            const string message = "Unauthorised access detected!";
            byte[] payload = { 0x1, 0x2, 0x3 };
            _adminResponseEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .ClusterSessionId(clusterSessionId)
                .CorrelationId(correlationId)
                .RequestType(type)
                .ResponseCode(responseCode)
                .Message(message);
            _adminResponseEncoder.PutPayload(payload, 0, payload.Length);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(1, 3);
            var adapter = new EgressAdapter(egressListener, clusterSessionId, A.Fake<Subscription>(), 10);

            adapter.OnFragment(_buffer, offset, _adminResponseEncoder.EncodedLength(), header);

            A.CallTo(() =>
                    egressListener.OnAdminResponse(
                        clusterSessionId,
                        correlationId,
                        type,
                        responseCode,
                        message,
                        _buffer,
                        offset
                            + MessageHeaderEncoder.ENCODED_LENGTH
                            + _adminResponseEncoder.EncodedLength()
                            - payload.Length,
                        payload.Length
                    )
                )
                .MustHaveHappenedOnceExactly();
            A.CallTo(egressListener).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void OnFragmentIsANoOpIfSessionIdDoesNotMatchOnAdminResponse()
        {
            const int offset = 24;
            const long clusterSessionId = 18;
            const long correlationId = 3274239749237498239L;
            const AdminRequestType type = AdminRequestType.SNAPSHOT;
            const AdminResponseCode responseCode = AdminResponseCode.OK;
            const string message = "Unauthorised access detected!";
            byte[] payload = { 0x1, 0x2, 0x3 };
            _adminResponseEncoder
                .WrapAndApplyHeader(_buffer, offset, _messageHeaderEncoder)
                .ClusterSessionId(clusterSessionId)
                .CorrelationId(correlationId)
                .RequestType(type)
                .ResponseCode(responseCode)
                .Message(message);
            _adminResponseEncoder.PutPayload(payload, 0, payload.Length);

            var egressListener = A.Fake<IEgressListener>();
            var header = new Header(1, 3);
            var adapter = new EgressAdapter(egressListener, -clusterSessionId, A.Fake<Subscription>(), 10);

            adapter.OnFragment(_buffer, offset, _adminResponseEncoder.EncodedLength(), header);

            A.CallTo(egressListener).MustNotHaveHappened();
        }
    }
}
