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
        private const string Channel = "aeron:udp?endpoint=localhost:40124";
        private const int StreamId1 = 1002;
        private const int SessionId1 = 13;
        private const int TermId1 = 1;
        private const int CorrelationId = 2000;
        private const int SendBufferCapacity = 1024;
        private const int PartitionIndex = 0;
        private const int MtuLength = 4096;
        private const int PageSize = 4 * 1024;
        private const int TermLength = TERM_MIN_LENGTH * 4;
        private const int MaxPayloadSize = MtuLength - HEADER_LENGTH;
        private static readonly int MaxMessageSize = ComputeMaxMessageLength(TermLength);

        private static readonly int TotalAlignedMaxMessageSize =
            (MaxMessageSize / MaxPayloadSize) * MtuLength
            + BitUtil.Align(MaxMessageSize % MaxPayloadSize + HEADER_LENGTH, FRAME_ALIGNMENT);

        private static readonly int PositionBitsToShift = PositionBitsToShift(TermLength);
        private static readonly int DefaultFrameType = HeaderFlyweight.HDR_TYPE_RTTM;
        private const int SessionId = 42;
        private const int StreamId = 111;

        private UnsafeBuffer _logMetaDataBuffer;
        private UnsafeBuffer[] _termBuffers;
        private ClientConductor _conductor;
        private LogBuffers _logBuffers;
        private IReadablePosition _publicationLimit;
        private ConcurrentPublication _publication;

        [SetUp]
        public void SetUp()
        {
            _logMetaDataBuffer = A.Fake<UnsafeBuffer>(x => x.Wrapping(new UnsafeBuffer()));
            _logMetaDataBuffer.Wrap(new byte[LOG_META_DATA_LENGTH]);

            _termBuffers = new UnsafeBuffer[PARTITION_COUNT];

            _conductor = A.Fake<ClientConductor>();
            _logBuffers = A.Fake<LogBuffers>();
            _publicationLimit = A.Fake<IReadablePosition>();

            A.CallTo(() => _publicationLimit.GetVolatile()).Returns(2 * SendBufferCapacity);
            A.CallTo(() => _logBuffers.DuplicateTermBuffers()).Returns(_termBuffers);
            A.CallTo(() => _logBuffers.TermLength()).Returns(TermLength);
            A.CallTo(() => _logBuffers.MetaDataBuffer()).Returns(_logMetaDataBuffer);

            var defaultHeader = CreateDefaultHeader(SessionId, StreamId, TermId1);
            defaultHeader.PutShort(
                DataHeaderFlyweight.TYPE_FIELD_OFFSET,
                (short)DefaultFrameType,
                ByteOrder.LittleEndian
            );
            StoreDefaultFrameHeader(_logMetaDataBuffer, defaultHeader);

            InitialTermId(_logMetaDataBuffer, TermId1);
            MtuLength(_logMetaDataBuffer, MtuLength);
            TermLength(_logMetaDataBuffer, TermLength);
            PageSize(_logMetaDataBuffer, PageSize);
            IsConnected(_logMetaDataBuffer, false);

            for (var i = 0; i < PARTITION_COUNT; i++)
            {
                _termBuffers[i] = new UnsafeBuffer(BufferUtil.AllocateDirect(TermLength));
            }

            _publication = new ConcurrentPublication(
                _conductor,
                Channel,
                StreamId1,
                SessionId1,
                _publicationLimit,
                ChannelEndpointStatus.NO_ID_ALLOCATED,
                _logBuffers,
                CorrelationId,
                CorrelationId
            );

            InitialiseTailWithTermId(_logMetaDataBuffer, PartitionIndex, TermId1);

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
                Assert.AreEqual(ComputeMaxMessageLength(TermLength), _publication.MaxMessageLength);
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

                OnError(_termBuffers[PartitionIndex], termOffset, length);
                AssertFrameType(PartitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(PartitionIndex, termOffset, 0);
            }

            [Test]
            public void ReturnsAdminActionIfTermCountAndTermIdDoNotMatch()
            {
                const int termCount = 9;
                const int termOffset = 0;
                const int length = 10;
                Assert.AreEqual(IndexByTermCount(termCount), PartitionIndex);
                InitialiseTailWithTermId(_logMetaDataBuffer, PartitionIndex, TermId1 + 5);

                Assert.AreEqual(ADMIN_ACTION, Invoke(length, null));

                OnError(_termBuffers[PartitionIndex], termOffset, length);
                AssertFrameType(PartitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(PartitionIndex, termOffset, 0);
            }

            [Test]
            public void ReturnsMaxPositionExceededIfPublicationLimitReached()
            {
                const int partitionIndex = 1;
                int termOffset = TermLength - 140;
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
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(TermId1 + 1, termOffset));
                ActiveTermCount(_logMetaDataBuffer, 1);
                IsConnected(_logMetaDataBuffer, true);

                Assert.AreEqual(
                    BACK_PRESSURED,
                    Invoke(length, (termBuffer, termOffset1, frameLength) => long.MaxValue)
                );

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
                InitialiseTailWithTermId(_logMetaDataBuffer, partitionIndex, TermId1 + 1);
                ActiveTermCount(_logMetaDataBuffer, 1);

                Assert.AreEqual(NOT_CONNECTED, Invoke(length, null));

                OnError(_termBuffers[partitionIndex], termOffset, length);
                AssertFrameType(partitionIndex, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(partitionIndex, termOffset, 0);
            }

            [Test]
            public void ReturnsAdminActionIfTermWasRotated()
            {
                int termOffset = TermLength - 64;
                const int length = 60;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(1L + int.MaxValue);
                RawTailVolatile(_logMetaDataBuffer, 0, PackTail(TermId1, termOffset));
                RawTailVolatile(_logMetaDataBuffer, 1, PackTail(TermId1 + 1 - PARTITION_COUNT, 555));
                RawTailVolatile(_logMetaDataBuffer, 2, PackTail(StreamId, 777));

                Assert.AreEqual(ADMIN_ACTION, Invoke(length, null));

                OnError(_termBuffers[0], termOffset, length);
                AssertFrameType(0, termOffset, PADDING_FRAME_TYPE);
                AssertFrameLength(0, termOffset, 64);
                Assert.AreEqual(PackTail(TermId1, termOffset + 96), RawTail(_logMetaDataBuffer, 0));
                Assert.AreEqual(PackTail(TermId1 + 1, 0), RawTail(_logMetaDataBuffer, 1));
                Assert.AreEqual(PackTail(StreamId, 777), RawTail(_logMetaDataBuffer, 2));
            }

            [Test]
            public void ReturnsMaxPositionExceededIfThereIsNotEnoughSpaceLeft()
            {
                const int partitionIndex = 1;
                int termOffset = TermLength - 128;
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

            protected void TestPositionUponSuccess(
                int length,
                int termId,
                int termCount,
                long tailAfterUpdate,
                long expectedPosition
            )
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
                AssertFrameType(partitionIndex, termOffset, DefaultFrameType);
                Assert.AreEqual(SessionId, FrameSessionId(buffer, termOffset));
                Assert.AreEqual(StreamId, StreamId(buffer, termOffset));
                Assert.AreEqual(TermId(tailAfterUpdate), TermId(buffer, termOffset));
                OnSuccess(buffer, termOffset, length);
            }
        }

        [TestFixture]
        public class TryClaim : BaseTests
        {
            private readonly BufferClaim _bufferClaim = new BufferClaim();

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                return _publication.TryClaim(length, _bufferClaim);
            }

            protected override void OnError(UnsafeBuffer termBuffer, int termOffset, int length)
            {
                IMutableDirectBuffer buffer = _bufferClaim.Buffer;
                Assert.AreEqual(0, buffer.Capacity);
            }

            protected override void OnSuccess(UnsafeBuffer termBuffer, int termOffset, int length)
            {
                Assert.AreEqual(-(length + HEADER_LENGTH), FragmentLength(termBuffer, termOffset));
                Assert.AreEqual(0, ReservedValue(termBuffer, termOffset));
                Assert.AreEqual(length, _bufferClaim.Length);
            }

            [Test, TestCaseSource(nameof(TryClaimPositions))]
            public void TryClaimShouldReturnPositionAtWhichTheClaimedSpaceEnds(
                int length,
                int termId,
                int termCount,
                long tailAfterUpdate,
                long expectedPosition
            )
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
                int index = 0,
                    processedBytes = 0;
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
            public void ReturnsPositionAfterDataIsCopied(
                int length,
                int termId,
                int termCount,
                long tailAfterUpdate,
                long expectedPosition
            )
            {
                TestPositionUponSuccess(length, termId, termCount, tailAfterUpdate, expectedPosition);
            }
        }

        [TestFixture]
        public class Offer : OfferBase
        {
            private UnsafeBuffer _sendBuffer;

            [SetUp]
            public void Before()
            {
                byte[] bytes = new byte[MaxMessageSize];
                Random.NextBytes(bytes);
                _sendBuffer = new UnsafeBuffer(bytes);
            }

            protected override UnsafeBuffer Buffer(int length)
            {
                return _sendBuffer;
            }

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                return _publication.Offer(_sendBuffer, 0, length, reservedValueSupplier);
            }

            [Test]
            public void OfferWithAnOffset()
            {
                const int partitionIndex = 2;
                int termId = TermId1 + 2;
                const int termOffset = 992;
                const int offset = 11;
                const int length = 23;
                A.CallTo(() => _publicationLimit.GetVolatile()).Returns(int.MaxValue);
                IsConnected(_logMetaDataBuffer, true);
                RawTail(_logMetaDataBuffer, partitionIndex, PackTail(termId, termOffset));
                ActiveTermCount(_logMetaDataBuffer, 2);

                long position = _publication.Offer(
                    _sendBuffer,
                    offset,
                    length,
                    (termBuffer, frameOffset, frameLength) => long.MinValue
                );

                Assert.AreEqual(525344, position);
                UnsafeBuffer termBuffer = _termBuffers[partitionIndex];
                Assert.AreEqual(long.MinValue, ReservedValue(termBuffer, termOffset));
                for (int i = 0; i < length; i++)
                {
                    Assert.AreEqual(
                        _sendBuffer.GetByte(offset + i),
                        termBuffer.GetByte(termOffset + HEADER_LENGTH + i)
                    );
                }
            }
        }

        [TestFixture]
        public class OfferWithTwoBuffers : OfferBase
        {
            private UnsafeBuffer _buffer1;
            private UnsafeBuffer _buffer2;

            protected override UnsafeBuffer Buffer(int processedBytes)
            {
                return processedBytes < _buffer1.Capacity ? _buffer1 : _buffer2;
            }

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                byte[] bytes = new byte[length];
                Random.NextBytes(bytes);
                int chunk1Length = (int)(length * 0.25);
                int chunk2Length = length - chunk1Length;
                _buffer1 = new UnsafeBuffer(bytes, 0, chunk1Length);
                _buffer2 = new UnsafeBuffer(bytes, chunk1Length, chunk2Length);

                return _publication.Offer(_buffer1, 0, chunk1Length, _buffer2, 0, chunk2Length, reservedValueSupplier);
            }

            [Test]
            public void OfferWithOffsets()
            {
                const int partitionIndex = 1;
                int termId = TermId1 + 1;
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
                long position = _publication.Offer(
                    buffer1,
                    offsetOne,
                    lengthOne,
                    buffer2,
                    offsetTwo,
                    lengthTwo,
                    (termBuffer, frameOffset, frameLength) => -1
                );

                Assert.AreEqual(262272, position);
                UnsafeBuffer termBuffer = _termBuffers[partitionIndex];
                Assert.AreEqual(-1, ReservedValue(termBuffer, termOffset));
                for (int i = 0; i < lengthOne; i++)
                {
                    Assert.AreEqual(buffer1.GetByte(offsetOne + i), termBuffer.GetByte(termOffset + HEADER_LENGTH + i));
                }

                for (int i = 0; i < lengthTwo; i++)
                {
                    Assert.AreEqual(
                        buffer2.GetByte(offsetTwo + i),
                        termBuffer.GetByte(termOffset + HEADER_LENGTH + lengthOne + i)
                    );
                }
            }
        }

        [TestFixture]
        public class VectorOffer : OfferBase
        {
            private readonly DirectBufferVector[] _vectors = new DirectBufferVector[3];

            protected override UnsafeBuffer Buffer(int processedBytes)
            {
                return (UnsafeBuffer)_vectors[0].Buffer();
            }

            protected override long Invoke(int length, ReservedValueSupplier reservedValueSupplier)
            {
                byte[] bytes = new byte[length];
                Random.NextBytes(bytes);
                int numVectors = _vectors.Length;
                int chunkSize = length / numVectors;
                UnsafeBuffer buffer = new UnsafeBuffer(bytes);
                for (int i = 0, offset = 0; i < numVectors; i++)
                {
                    int size = numVectors - 1 == i ? (length - offset) : chunkSize;
                    _vectors[i] = new DirectBufferVector(buffer, offset, size);
                    offset += size;
                }

                return _publication.Offer(_vectors, reservedValueSupplier);
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
            yield return new TestCaseData(
                100,
                31,
                30,
                PackTail(31, 16 * 1024),
                ComputePosition(31, 16 * 1024 + 160, PositionBitsToShift, TermId1)
            );
            yield return new TestCaseData(
                999,
                7,
                6,
                PackTail(212, 8192),
                ComputePosition(212, 8192 + 1056, PositionBitsToShift, TermId1)
            );
        }

        protected static IEnumerable<TestCaseData> OfferPositions()
        {
            yield return new TestCaseData(
                124,
                5,
                4,
                PackTail(11, 3072),
                ComputePosition(11, 3072 + 160, PositionBitsToShift, TermId1)
            );
            yield return new TestCaseData(
                MaxMessageSize,
                77,
                76,
                PackTail(77, 1024),
                ComputePosition(77, 1024 + TotalAlignedMaxMessageSize, PositionBitsToShift, TermId1)
            );
        }
    }
}
