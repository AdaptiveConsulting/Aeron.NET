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

using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Status;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class PublicationTest
    {
        private const string Channel = "aeron:udp?endpoint=localhost:40124";
        private const int StreamID1 = 1002;
        private const int SessionID1 = 13;
        private const int TermID1 = 1;
        private const int CorrelationID = 2000;
        private const int SendBufferCapacity = 1024;
        private const int PartionIndex = 0;
        private const int MTU_LENGTH = 4096;
        private const int PAGE_SIZE = 4 * 1024;

        private byte[] _sendBuffer;
        private UnsafeBuffer _atomicSendBuffer;
        private UnsafeBuffer _logMetaDataBuffer;
        private UnsafeBuffer[] _termBuffers;

        private ClientConductor _conductor;
        private LogBuffers _logBuffers;
        private IReadablePosition _publicationLimit;
        private ConcurrentPublication _publication;

        [SetUp]
        public void SetUp()
        {
            _sendBuffer = new byte[SendBufferCapacity];
            _atomicSendBuffer = new UnsafeBuffer(_sendBuffer);
            _logMetaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);
            _termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];

            _conductor = A.Fake<ClientConductor>();
            _logBuffers = A.Fake<LogBuffers>();
            _publicationLimit = A.Fake<IReadablePosition>();

            A.CallTo(() => _publicationLimit.GetVolatile()).Returns(2 * SendBufferCapacity);
            A.CallTo(() => _logBuffers.DuplicateTermBuffers()).Returns(_termBuffers);
            A.CallTo(() => _logBuffers.TermLength()).Returns(LogBufferDescriptor.TERM_MIN_LENGTH);
            A.CallTo(() => _logBuffers.MetaDataBuffer()).Returns(_logMetaDataBuffer);

            LogBufferDescriptor.InitialTermId(_logMetaDataBuffer, TermID1);
            LogBufferDescriptor.MtuLength(_logMetaDataBuffer, MTU_LENGTH);
            LogBufferDescriptor.TermLength(_logMetaDataBuffer, LogBufferDescriptor.TERM_MIN_LENGTH);
            LogBufferDescriptor.PageSize(_logMetaDataBuffer, PAGE_SIZE);
            LogBufferDescriptor.IsConnected(_logMetaDataBuffer, false);

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                _termBuffers[i] = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_MIN_LENGTH]);
            }

            _publication = new ConcurrentPublication(
                _conductor,
                Channel,
                StreamID1,
                SessionID1,
                _publicationLimit,
                ChannelEndpointStatus.NO_ID_ALLOCATED,
                _logBuffers,
                CorrelationID,
                CorrelationID);

            LogBufferDescriptor.InitialiseTailWithTermId(_logMetaDataBuffer, PartionIndex, TermID1);

            A.CallTo(() => _conductor.RemovePublication(_publication)).Invokes(() => _publication.InternalClose());
        }

        [Test]
        public void ShouldEnsureThePublicationIsOpenBeforeReadingPosition()
        {
            _publication.Dispose();
            Assert.AreEqual(Publication.CLOSED, _publication.Position);

            A.CallTo(() => _conductor.RemovePublication(_publication)).MustHaveHappened();
        }

        [Test]
        public void ShouldEnsureThePublicationIsOpenBeforeOffer()
        {
            _publication.Dispose();
            Assert.True(_publication.IsClosed);
            Assert.AreEqual(Publication.CLOSED, _publication.Offer(_atomicSendBuffer));
        }

        [Test]
        public void ShouldEnsureThePublicationIsOpenBeforeClaim()
        {
            _publication.Dispose();
            var bufferClaim = new BufferClaim();
            Assert.AreEqual(Publication.CLOSED, _publication.TryClaim(SendBufferCapacity, bufferClaim));
        }

        [Test]
        public void ShouldReportThatPublicationHasNotBeenConnectedYet()
        {
            A.CallTo(() => _publicationLimit.GetVolatile()).Returns(0);
            LogBufferDescriptor.IsConnected(_logMetaDataBuffer, false);

            Assert.False(_publication.IsConnected);
        }

        [Test]
        public void ShouldReportThatPublicationHasBeenConnectedYet()
        {
            LogBufferDescriptor.IsConnected(_logMetaDataBuffer, true);
            Assert.True(_publication.IsConnected);
        }

        [Test]
        public void ShouldReportInitialPosition()
        {
            Assert.AreEqual(0L, _publication.Position);
        }

        [Test]
        public void ShouldReportMaxMessageLength()
        {
            Assert.AreEqual(FrameDescriptor.ComputeMaxMessageLength(LogBufferDescriptor.TERM_MIN_LENGTH), _publication.MaxMessageLength);
        }

        [Test]
        public void ShouldReleasePublicationOnClose()
        {
            _publication.Dispose();

            A.CallTo(() => _conductor.RemovePublication(_publication)).MustHaveHappened();
        }
        
        [Test]
        public void ShouldReturnErrorMessages()
        {
            Assert.Equals("NOT_CONNECTED", Publication.ErrorString(-1L));
            Assert.Equals("BACK_PRESSURED", Publication.ErrorString(-2L));
            Assert.Equals("ADMIN_ACTION", Publication.ErrorString(-3L));
            Assert.Equals("CLOSED", Publication.ErrorString(-4L));
            Assert.Equals("MAX_POSITION_EXCEEDED", Publication.ErrorString(-5L));
            Assert.Equals("NONE", Publication.ErrorString(0L));
            Assert.Equals("NONE", Publication.ErrorString(1L));
            Assert.Equals("UNKNOWN", Publication.ErrorString(-6L));
            Assert.Equals("UNKNOWN", Publication.ErrorString(long.MinValue));
        }
    }
}