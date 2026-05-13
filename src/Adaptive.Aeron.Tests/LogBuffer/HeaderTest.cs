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

using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    public class HeaderTest
    {
        [Test]
        public void ConstructorInitializedData()
        {
            const int initialTermId = 18;
            object context = "context-value-Long.MaxValue";
            const int positionBitsToShift = 2;
            var header = new Header(initialTermId, positionBitsToShift, context);

            Assert.AreEqual(initialTermId, header.InitialTermId);
            Assert.AreEqual(positionBitsToShift, header.PositionBitsToShift);
            Assert.AreEqual(context, header.Context);
        }

        [TestCase(0, 2, 100, 1024, 5, 1172L)]
        [TestCase(42, 16, 13, 4096, 46, 266272L)]
        [TestCase(1, 30, 1024, 1073741824, 111, 119185343488L)]
        public void PositionCalculationTheEndOfTheMessageInTheLog(
            int initialTermId,
            int positionBitsToShift,
            int frameLength,
            int termOffset,
            int termId,
            long expectedPosition
        )
        {
            var dataHeaderFlyweight = new DataHeaderFlyweight();
            var header = new Header(initialTermId, positionBitsToShift) { Buffer = dataHeaderFlyweight, Offset = 0 };
            dataHeaderFlyweight.Wrap(new byte[64], 16, 32);
            dataHeaderFlyweight.FrameLength(frameLength);
            dataHeaderFlyweight.TermId(termId);
            dataHeaderFlyweight.TermOffset(termOffset);

            Assert.AreEqual(expectedPosition, header.Position);
            Assert.AreEqual(Aeron.NULL_VALUE, header.FragmentedFrameLength);
        }

        [Test]
        public void OffsetIsRelativeToTheBufferStart()
        {
            var header = new Header(42, 3, "xyz");

            Assert.AreEqual(0, header.Offset);

            header.Offset = 142;

            Assert.AreEqual(142, header.Offset);
        }

        [TestCase(103, (byte)0x3, (byte)0x1A, (short)0x6, 2080, -46234, 333, 5, 909090909090909L)]
        [TestCase(512, (byte)0x1, (byte)0xC, (short)0x1, 1073741824, 42, -876, 1543, -4632842384627834687L)]
        public void ShouldReadDataFromTheBuffer(
            int frameLength,
            byte version,
            byte flags,
            short type,
            int termOffset,
            int sessionId,
            int streamId,
            int termId,
            long reservedValue
        )
        {
            var array = new byte[100];
            const int offset = 16;
            var dataHeaderFlyweight = new DataHeaderFlyweight();
            dataHeaderFlyweight.Wrap(array, offset, 64);

            dataHeaderFlyweight.FrameLength(frameLength).Version(version).Flags(flags).HeaderType(type);

            dataHeaderFlyweight
                .TermOffset(termOffset)
                .SessionId(sessionId)
                .StreamId(streamId)
                .TermId(termId)
                .ReservedValue(reservedValue);

            var header = new Header(5, 22) { Buffer = new UnsafeBuffer(array), Offset = offset };

            Assert.AreEqual(frameLength, header.FrameLength);
            Assert.AreEqual(flags, header.Flags);
            Assert.AreEqual(type, header.Type);
            Assert.AreEqual(termOffset, header.TermOffset);
            Assert.AreEqual(sessionId, header.SessionId);
            Assert.AreEqual(streamId, header.StreamId);
            Assert.AreEqual(termId, header.TermId);
            Assert.AreEqual(reservedValue, header.ReservedValue);
        }

        [Test]
        public void ShouldOverrideInitialTermId()
        {
            const int initialTermId = -178;
            const int newInitialTermId = 871;
            var header = new Header(initialTermId, 3);
            Assert.AreEqual(initialTermId, header.InitialTermId);

            header.InitialTermId = newInitialTermId;
            Assert.AreEqual(newInitialTermId, header.InitialTermId);
        }

        [Test]
        public void ShouldOverridePositionBitsToShift()
        {
            const int positionBitsToShift = -6;
            const int newPositionBitsToShift = 20;
            var header = new Header(42, positionBitsToShift);
            Assert.AreEqual(positionBitsToShift, header.PositionBitsToShift);

            header.PositionBitsToShift = newPositionBitsToShift;
            Assert.AreEqual(newPositionBitsToShift, header.PositionBitsToShift);
        }
    }
}
