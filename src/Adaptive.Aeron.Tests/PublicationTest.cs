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
using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;
using static Adaptive.Aeron.Publication;

namespace Adaptive.Aeron.Tests
{
    public class PublicationTest
    {
        private const string CHANNEL = "aeron:udp?endpoint=localhost:40124";
        private const int STREAM_ID1 = 1002;
        private const int SESSION_ID1 = 13;
        private const int TERM_ID_1 = 1;
        private const int CORRELATION_ID = 2000;
        private const int SEND_BUFFER_CAPACITY = 1024;
        private const int PARTITION_INDEX = 0;
        private const int MTU_LENGTH = 4096;
        private const int PAGE_SIZE = 4 * 1024;
        private const int TERM_LENGTH = TERM_MIN_LENGTH * 4;
        private const int MAX_PAYLOAD_SIZE = MTU_LENGTH - HEADER_LENGTH;
        private static readonly int MAX_MESSAGE_SIZE = ComputeMaxMessageLength(TERM_LENGTH);

        private static readonly int TOTAL_ALIGNED_MAX_MESSAGE_SIZE =
            (MAX_MESSAGE_SIZE / MAX_PAYLOAD_SIZE) * MTU_LENGTH +
            BitUtil.Align(MAX_MESSAGE_SIZE % MAX_PAYLOAD_SIZE + HEADER_LENGTH, FRAME_ALIGNMENT);

        private static readonly int POSITION_BITS_TO_SHIFT = PositionBitsToShift(TERM_LENGTH);
        private static readonly int DEFAULT_FRAME_TYPE = HeaderFlyweight.HDR_TYPE_RTTM;
        private const int SESSION_ID = 42;
        private const int STREAM_ID = 111;

        private UnsafeBuffer _logMetaDataBuffer;
        private UnsafeBuffer[] _termBuffers;
        private ClientConductor _conductor;
        private LogBuffers _logBuffers;
        private IReadablePosition _publicationLimit;
        private ConcurrentPublication _publication;

        [SetUp]
        public void SetUp()
        {
            _logMetaDataBuffer =
                A.Fake<UnsafeBuffer>(x => x.Wrapping(new UnsafeBuffer()));
            _logMetaDataBuffer.Wrap(new byte[LOG_META_DATA_LENGTH]);
            
            _termBuffers = new UnsafeBuffer[PARTITION_COUNT];
            
            _conductor = A.Fake<ClientConductor>();
            _logBuffers = A.Fake<LogBuffers>();
            _publicationLimit = A.Fake<IReadablePosition>();

            A.CallTo(() => _publicationLimit.GetVolatile()).Returns(2 * SEND_BUFFER_CAPACITY);
            A.CallTo(() => _logBuffers.DuplicateTermBuffers()).Returns(_termBuffers);
            A.CallTo(() => _logBuffers.TermLength()).Returns(TERM_LENGTH);
            A.CallTo(() => _logBuffers.MetaDataBuffer()).Returns(_logMetaDataBuffer);

            var defaultHeader = CreateDefaultHeader(SESSION_ID, STREAM_ID, TERM_ID_1);
            defaultHeader.PutShort(DataHeaderFlyweight.TYPE_FIELD_OFFSET, (short)DEFAULT_FRAME_TYPE,
                ByteOrder.LittleEndian);
            StoreDefaultFrameHeader(_logMetaDataBuffer, defaultHeader);

            InitialTermId(_logMetaDataBuffer, TERM_ID_1);
            MtuLength(_logMetaDataBuffer, MTU_LENGTH);
            TermLength(_logMetaDataBuffer, TERM_LENGTH);
            PageSize(_logMetaDataBuffer, PAGE_SIZE);
            IsConnected(_logMetaDataBuffer, false);

            for (var i = 0; i < PARTITION_COUNT; i++)
            {
                _termBuffers[i] = new UnsafeBuffer(BufferUtil.AllocateDirect(TERM_LENGTH));
            }

            _publication = new ConcurrentPublication(
                _conductor,
                CHANNEL,
                STREAM_ID1,
                SESSION_ID1,
                _publicationLimit,
                ChannelEndpointStatus.NO_ID_ALLOCATED,
                _logBuffers,
                CORRELATION_ID,
                CORRELATION_ID);

            InitialiseTailWithTermId(_logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1);

            A.CallTo(() => _conductor.RemovePublication(_publication)).Invokes(() => _publication.InternalClose());
        }

        public class OriginalTests : PublicationTest
        {
            [Test]
            public void ShouldEnsureThePublicationIsOpenBeforeReadingPosition()
            {
                _publication.Dispose();
                Assert.AreEqual(CLOSED, _publication.Position);

                A.CallTo(() => _conductor.RemovePublication(_publication)).MustHaveHappened();
            }

            [Test]
            public void ShouldReportThatPublicationHasNotBeenConnectedYet()
            {
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(0);
                IsConnected(_logMetaDataBuffer, false);

                Assert.False(_publication.IsConnected);
            }

            [Test]
            public void ShouldReportThatPublicationHasBeenConnectedYet()
            {
                IsConnected(_logMetaDataBuffer, true);
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
                Assert.AreEqual(ComputeMaxMessageLength(TERM_LENGTH), _publication.MaxMessageLength);
            }

            [Test]
            public void ShouldRemovePublicationOnClose()
            {
                _publication.Dispose();

                A.CallTo(() => _conductor.RemovePublication(_publication)).MustHaveHappened();
            }

            [Test]
            public void ShouldReturnErrorMessages()
            {
                Assert.AreEqual("NOT_CONNECTED", ErrorString(-1L));
                Assert.AreEqual("BACK_PRESSURED", ErrorString(-2L));
                Assert.AreEqual("ADMIN_ACTION", ErrorString(-3L));
                Assert.AreEqual("CLOSED", ErrorString(-4L));
                Assert.AreEqual("MAX_POSITION_EXCEEDED", ErrorString(-5L));
                Assert.AreEqual("NONE", ErrorString(0L));
                Assert.AreEqual("NONE", ErrorString(1L));
                Assert.AreEqual("UNKNOWN", ErrorString(-6L));
                Assert.AreEqual("UNKNOWN", ErrorString(long.MinValue));
            }
        }

        public abstract class BaseTests : PublicationTest
        {
            protected abstract long Invoke(int length, ReservedValueSupplier reservedValueSupplier);

            protected abstract void OnError(UnsafeBuffer termBuffer, int termOffset, int length);

            protected abstract void OnSuccess(UnsafeBuffer termBuffer, int termOffset, int length);

            [Test]
            public void ReturnsClosedIfPublicationWasClosed()
            {
                const int length = 6;
                const int termOffset = 0;
                _publication.Dispose();

                Assert.AreEqual(CLOSED, Invoke(length, null));

                OnError(_termBuffers[PARTITION_INDEX], termOffset, length);
                AssertFrameType(PARTITION_INDEX, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(PARTITION_INDEX, termOffset, 0);
            }

            [Test]
            public void ReturnsAdminActionIfTermCountAndTermIdDoNotMatch()
            {
                const int termCount = 9;
                const int termOffset = 0;
                const int length = 10;
                Assert.AreEqual(IndexByTermCount(termCount), PARTITION_INDEX);
                InitialiseTailWithTermId(_logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1 + 5);

                Assert.AreEqual(ADMIN_ACTION, Invoke(length, null));

                OnError(_termBuffers[PARTITION_INDEX], termOffset, length);
                AssertFrameType(PARTITION_INDEX, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(PARTITION_INDEX, termOffset, 0);
            }

            [Test]
            public void ReturnsMaxPositionExceededIfPublicationLimitReached()
            {
                const int partitionIndex = 1;
                int termOffset = TERM_LENGTH - 140;
                const int length = 100;

                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(16L);
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(int.MinValue, termOffset));
                ActiveTermCount(_logMetaDataBuffer, int.MaxValue);
                IsConnected(_logMetaDataBuffer, true);

                Assert.AreEqual(MAX_POSITION_EXCEEDED, Invoke(length, null));

                OnError(_termBuffers[partitionIndex], termOffset, length);
                AssertFrameType(partitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(partitionIndex, termOffset, 0);
            }

            [Test]
            public void ReturnsBackPressuredIfPublicationLimitReached()
            {
                const int partitionIndex = 1;
                const int termOffset = 64;
                const int length = 11;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(16L);
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(TERM_ID_1 + 1, termOffset));
                ActiveTermCount(_logMetaDataBuffer, 1);
                IsConnected(_logMetaDataBuffer, true);

                Assert.AreEqual(BACK_PRESSURED,
                    Invoke(length, (termBuffer, termOffset1, frameLength) => long.MaxValue));

                UnsafeBuffer termBuffer = _termBuffers[partitionIndex];
                OnError(termBuffer, termOffset, length);
                AssertFrameType(partitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(partitionIndex, termOffset, 0);
                Assert.AreEqual(0, ReservedValue(termBuffer, termOffset));
            }

            [Test]
            public void ReturnsNotConnectedIfPublicationLimitReachedAndNotConnected()
            {
                const int partitionIndex = 1;
                const int termOffset = 0;
                const int length = 10;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(16L);
                InitialiseTailWithTermId(_logMetaDataBuffer, partitionIndex, TERM_ID_1 + 1);
                ActiveTermCount(_logMetaDataBuffer, 1);

                Assert.AreEqual(NOT_CONNECTED, Invoke(length, null));

                OnError(_termBuffers[partitionIndex], termOffset, length);
                AssertFrameType(partitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(partitionIndex, termOffset, 0);
            }

            [Test]
            public void ReturnsAdminActionIfTermWasRotated()
            {
                int termOffset = TERM_LENGTH - 64;
                const int length = 60;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(1L + int.MaxValue);
                RawTailVolatile(_logMetaDataBuffer, 0, PackTail(TERM_ID_1, termOffset));
                RawTailVolatile(_logMetaDataBuffer, 1, PackTail(TERM_ID_1 + 1 - PARTITION_COUNT, 555));
                RawTailVolatile(_logMetaDataBuffer, 2, PackTail(STREAM_ID, 777));

                Assert.AreEqual(ADMIN_ACTION, Invoke(length, null));

                OnError(_termBuffers[0], termOffset, length);
                AssertFrameType(0, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(0, termOffset, 64);
                Assert.AreEqual(PackTail(TERM_ID_1, termOffset + 96), RawTail(_logMetaDataBuffer, 0));
                Assert.AreEqual(PackTail(TERM_ID_1 + 1, 0), RawTail(_logMetaDataBuffer, 1));
                Assert.AreEqual(PackTail(STREAM_ID, 777), RawTail(_logMetaDataBuffer, 2));
            }

            [Test]
            public void ReturnsMaxPositionExceededIfThereIsNotEnoughSpaceLeft()
            {
                const int partitionIndex = 1;
                int termOffset = TERM_LENGTH - 128;
                const int length = 96;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(1L + int.MaxValue);
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(int.MinValue, termOffset));
                RawTail(_logMetaDataBuffer, 0, PackTail(55, 55));
                RawTail(_logMetaDataBuffer, 2, PackTail(222, 222));
                ActiveTermCount(_logMetaDataBuffer, int.MaxValue);
                IsConnected(_logMetaDataBuffer, true);

                Assert.AreEqual(MAX_POSITION_EXCEEDED, Invoke(length, null));

                OnError(_termBuffers[partitionIndex], termOffset, length);
                AssertFrameType(partitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(partitionIndex, termOffset, 0);
                Assert.AreEqual(PackTail(int.MinValue, termOffset), RawTail(_logMetaDataBuffer, partitionIndex));
                Assert.AreEqual(PackTail(55, 55), RawTail(_logMetaDataBuffer, 0));
                Assert.AreEqual(PackTail(222, 222), RawTail(_logMetaDataBuffer, 2));
            }

            protected void TestPositionUponSuccess(int length, int termId, int termCount, long tailAfterUpdate,
                long expectedPosition)
            {
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(int.MaxValue);
                IsConnected(_logMetaDataBuffer, true);
                ActiveTermCount(_logMetaDataBuffer, termCount);
                int partitionIndex = IndexByTermCount(termCount);
                long rawTail = PackTail(termId, 192);
                RawTail(_logMetaDataBuffer, partitionIndex, rawTail);

                A.CallTo(() => _logMetaDataBuffer.GetAndAddLong(A<int>._, A<long>._)).Returns(tailAfterUpdate);

                long position = Invoke(length, ((termBuffer, termOffset, frameLength) => termOffset ^ frameLength));

                Assert.AreEqual(expectedPosition, position);
                UnsafeBuffer buffer = _termBuffers[partitionIndex];
                int termOffset = TermOffset(tailAfterUpdate);
                AssertFrameType(partitionIndex, termOffset, DEFAULT_FRAME_TYPE);
                Assert.AreEqual(SESSION_ID, FrameSessionId(buffer, termOffset));
                Assert.AreEqual(STREAM_ID, StreamId(buffer, termOffset));
                Assert.AreEqual(TermId(tailAfterUpdate), TermId(buffer, termOffset));
                OnSuccess(buffer, termOffset, length);
            }
        }

        [TestFixture]
        public class TryClaim : BaseTests
        {
            private readonly BufferClaim bufferClaim = new BufferClaim();

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                return _publication.TryClaim(length, bufferClaim);
            }

            protected override void OnError(UnsafeBuffer termBuffer, int termOffset, int length)
            {
                IMutableDirectBuffer buffer = bufferClaim.Buffer;
                Assert.AreEqual(0, buffer.Capacity);
            }

            protected override void OnSuccess(UnsafeBuffer termBuffer, int termOffset, int length)
            {
                Assert.AreEqual(-(length + HEADER_LENGTH), FragmentLength(termBuffer, termOffset));
                Assert.AreEqual(0, ReservedValue(termBuffer, termOffset));
                Assert.AreEqual(length, bufferClaim.Length);
            }

            [Test, TestCaseSource(nameof(TryClaimPositions))]
            public void TryClaimShouldReturnPositionAtWhichTheClaimedSpaceEnds(int length, int termId, int termCount,
                long tailAfterUpdate, long expectedPosition)
            {
                TestPositionUponSuccess(length, termId, termCount, tailAfterUpdate, expectedPosition);
            }
        }

        public abstract class OfferBase : BaseTests
        {
            protected readonly Random Random = new Random();

            protected abstract UnsafeBuffer Buffer(int processedBytes);

            protected override void OnError(UnsafeBuffer termBuffer, int termOffset, int length)
            {
                int offset = termOffset + HEADER_LENGTH;
                for (int i = 0, capacity = termBuffer.Capacity; offset < capacity && i < length; i++, offset++)
                {
                    Assert.AreEqual(0, termBuffer.GetByte(offset));
                }
            }

            protected override void OnSuccess(UnsafeBuffer termBuffer, int termOffset, int length)
            {
                int index = 0, processedBytes = 0;
                int offset = termOffset;
                UnsafeBuffer dataBuffer = Buffer(0);
                while (processedBytes < length)
                {
                    int frameLength = FrameLength(termBuffer, offset);
                    if (frameLength <= 0)
                    {
                        break;
                    }

                    int chunkLength = frameLength - HEADER_LENGTH;
                    byte frameFlags = FrameFlags(termBuffer, offset);
                    if (0 == processedBytes)
                    {
                        Assert.AreEqual(BEGIN_FRAG_FLAG, (frameFlags & BEGIN_FRAG_FLAG));
                    }

                    if (length == processedBytes + chunkLength)
                    {
                        Assert.AreEqual(END_FRAG_FLAG, (frameFlags & END_FRAG_FLAG));
                    }

                    Assert.AreEqual(offset ^ frameLength, ReservedValue(termBuffer, offset));
                    offset += HEADER_LENGTH;
                    for (int i = 0; i < chunkLength; i++)
                    {
                        Assert.AreEqual(dataBuffer.GetByte(index++), termBuffer.GetByte(offset++));
                        UnsafeBuffer nextBuffer = Buffer(++processedBytes);
                        if (nextBuffer != dataBuffer)
                        {
                            dataBuffer = nextBuffer;
                            index = 0;
                        }
                    }
                }

                Assert.AreEqual(length, processedBytes);
            }

            [Test, TestCaseSource(nameof(OfferPositions))]
            public void ReturnsPositionAfterDataIsCopied(int length, int termId, int termCount, long tailAfterUpdate,
                long expectedPosition)
            {
                TestPositionUponSuccess(length, termId, termCount, tailAfterUpdate, expectedPosition);
            }
        }

        [TestFixture]
        public class Offer : OfferBase
        {
            private UnsafeBuffer sendBuffer;

            [SetUp]
            public void Before()
            {
                byte[] bytes = new byte[MAX_MESSAGE_SIZE];
                Random.NextBytes(bytes);
                sendBuffer = new UnsafeBuffer(bytes);
            }

            protected override UnsafeBuffer Buffer(int length)
            {
                return sendBuffer;
            }

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                return _publication.Offer(sendBuffer, 0, length, reservedValueSupplier);
            }

            [Test]
            public void OfferWithAnOffset()
            {
                const int partitionIndex = 2;
                int termId = TERM_ID_1 + 2;
                const int termOffset = 978;
                const int offset = 11;
                const int length = 23;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(int.MaxValue);
                IsConnected(_logMetaDataBuffer, true);
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(termId, termOffset));
                ActiveTermCount(_logMetaDataBuffer, 2);

                long position = _publication.Offer(sendBuffer, offset, length,
                    (termBuffer, frameOffset, frameLength) => long.MinValue);

                Assert.AreEqual(525330, position);
                UnsafeBuffer termBuffer = _termBuffers[partitionIndex];
                Assert.AreEqual(long.MinValue, ReservedValue(termBuffer, termOffset));
                for (int i = 0; i < length; i++)
                {
                    Assert.AreEqual(sendBuffer.GetByte(offset + i), termBuffer.GetByte(termOffset + HEADER_LENGTH + i));
                }
            }
        }

        [TestFixture]
        public class OfferWithTwoBuffers : OfferBase
        {
            private UnsafeBuffer buffer1;
            private UnsafeBuffer buffer2;

            protected override UnsafeBuffer Buffer(int processedBytes)
            {
                return processedBytes < buffer1.Capacity ? buffer1 : buffer2;
            }

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                byte[] bytes = new byte[length];
                Random.NextBytes(bytes);
                int chunk1Length = (int)(length * 0.25);
                int chunk2Length = length - chunk1Length;
                buffer1 = new UnsafeBuffer(bytes, 0, chunk1Length);
                buffer2 = new UnsafeBuffer(bytes, chunk1Length, chunk2Length);

                return _publication.Offer(buffer1, 0, chunk1Length, buffer2, 0, chunk2Length, reservedValueSupplier);
            }

            [Test]
            public void OfferWithOffsets()
            {
                const int partitionIndex = 1;
                int termId = TERM_ID_1 + 1;
                const int termOffset = 64;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(int.MaxValue);
                IsConnected(_logMetaDataBuffer, true);
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(termId, termOffset));
                ActiveTermCount(_logMetaDataBuffer, 1);

                byte[] bytes = new byte[16];
                Random.NextBytes(bytes);
                UnsafeBuffer buffer1 = new UnsafeBuffer(bytes, 0, 8);
                UnsafeBuffer buffer2 = new UnsafeBuffer(bytes, 8, 8);

                const int lengthOne = 5;
                const int offsetOne = 2;
                const int lengthTwo = 3;
                const int offsetTwo = 4;
                long position = _publication.Offer(buffer1, offsetOne, lengthOne, buffer2, offsetTwo, lengthTwo,
                    (termBuffer, frameOffset, frameLength) => -1);

                Assert.AreEqual(262272, position);
                UnsafeBuffer termBuffer = _termBuffers[partitionIndex];
                Assert.AreEqual(-1, ReservedValue(termBuffer, termOffset));
                for (int i = 0; i < lengthOne; i++)
                {
                    Assert.AreEqual(buffer1.GetByte(offsetOne + i), termBuffer.GetByte(termOffset + HEADER_LENGTH + i));
                }

                for (int i = 0; i < lengthTwo; i++)
                {
                    Assert.AreEqual(buffer2.GetByte(offsetTwo + i),
                        termBuffer.GetByte(termOffset + HEADER_LENGTH + lengthOne + i));
                }
            }
        }

        [TestFixture]
        public class VectorOffer : OfferBase
        {
            private readonly DirectBufferVector[] vectors = new DirectBufferVector[3];

            protected override UnsafeBuffer Buffer(int processedBytes)
            {
                return (UnsafeBuffer)vectors[0].Buffer();
            }

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                byte[] bytes = new byte[length];
                Random.NextBytes(bytes);
                int numVectors = vectors.Length;
                int chunkSize = length / numVectors;
                UnsafeBuffer buffer = new UnsafeBuffer(bytes);
                for (int i = 0, offset = 0; i < numVectors; i++)
                {
                    int size = numVectors - 1 == i ? (length - offset) : chunkSize;
                    vectors[i] = new DirectBufferVector(buffer, offset, size);
                    offset += size;
                }

                return _publication.Offer(vectors, reservedValueSupplier);
            }
        }

        private void AssertFrameType(int partitionIndex, int termOffset, int expectedFrameType)
        {
            Assert.AreEqual(expectedFrameType, FrameType(_termBuffers[partitionIndex], termOffset));
        }

        private void AssertFrameLength(int partitionIndex, int termOffset, int expectedFrameLength)
        {
            Assert.AreEqual(expectedFrameLength, FrameLength(_termBuffers[partitionIndex], termOffset));
        }

        protected static IEnumerable<TestCaseData> TryClaimPositions()
        {
            yield return new TestCaseData(100, 31, 30, PackTail(31, 16 * 1024),
                ComputePosition(31, 16 * 1024 + 160, POSITION_BITS_TO_SHIFT, TERM_ID_1));
            yield return new TestCaseData(999, 7, 6, PackTail(212, 8192),
                ComputePosition(212, 8192 + 1056, POSITION_BITS_TO_SHIFT, TERM_ID_1));
        }

        protected static IEnumerable<TestCaseData> OfferPositions()
        {
            yield return new TestCaseData(124, 5, 4, PackTail(11, 3072),
                ComputePosition(11, 3072 + 160, POSITION_BITS_TO_SHIFT, TERM_ID_1));
            yield return new TestCaseData(MAX_MESSAGE_SIZE, 77, 76, PackTail(77, 1024),
                ComputePosition(77, 1024 + TOTAL_ALIGNED_MAX_MESSAGE_SIZE, POSITION_BITS_TO_SHIFT, TERM_ID_1));
        }
    }
}