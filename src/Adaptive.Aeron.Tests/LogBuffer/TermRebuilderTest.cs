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
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    [TestFixture]
    public class TermRebuilderTest
    {
        private static readonly int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;

        private IAtomicBuffer _termBuffer;

        [SetUp]
        public void SetUp()
        {
            _termBuffer = A.Fake<IAtomicBuffer>();
            A.CallTo(() => _termBuffer.Capacity).Returns(TERM_BUFFER_CAPACITY);
        }

        [Test]
        public void ShouldInsertIntoEmptyBuffer()
        {
            UnsafeBuffer packet = new UnsafeBuffer(new byte[256]);
            const int termOffset = 0;
            const int srcOffset = 0;
            const int length = 256;
            packet.PutInt(srcOffset, length);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, length);
            
            A.CallTo(() => _termBuffer.PutBytes(termOffset + DataHeaderFlyweight.HEADER_LENGTH, packet, srcOffset + DataHeaderFlyweight.HEADER_LENGTH, length - DataHeaderFlyweight.HEADER_LENGTH)).MustHaveHappened()
                .Then(A.CallTo(() => _termBuffer.PutLong(termOffset + 24, packet.GetLong(24))).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutLong(termOffset + 16, packet.GetLong(16))).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutLong(termOffset + 8, packet.GetLong(8))).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutLongOrdered(termOffset, packet.GetLong(0))).MustHaveHappened());
        }


        [Test]
        public void ShouldInsertLastFrameIntoBuffer()
        {
            int frameLength = BitUtil.Align(256, FrameDescriptor.FRAME_ALIGNMENT);
            const int srcOffset = 0;
            int tail = TERM_BUFFER_CAPACITY - frameLength;
            int termOffset = tail;
            UnsafeBuffer packet = new UnsafeBuffer(new byte[frameLength]);
            packet.PutShort(FrameDescriptor.TypeOffset(srcOffset), (short)FrameDescriptor.PADDING_FRAME_TYPE);
            packet.PutInt(srcOffset, frameLength);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, frameLength);

            A.CallTo(() => _termBuffer.PutBytes(tail + DataHeaderFlyweight.HEADER_LENGTH, packet, srcOffset + DataHeaderFlyweight.HEADER_LENGTH, frameLength - DataHeaderFlyweight.HEADER_LENGTH)).MustHaveHappened();
        }

        [Test]
        public void ShouldFillSingleGap()
        {
            const int frameLength = 50;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int srcOffset = 0;
            int tail = alignedFrameLength;
            int termOffset = tail;
            UnsafeBuffer packet = new UnsafeBuffer(new byte[alignedFrameLength]);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, alignedFrameLength);

            A.CallTo(() => _termBuffer.PutBytes(tail + DataHeaderFlyweight.HEADER_LENGTH, packet, srcOffset + DataHeaderFlyweight.HEADER_LENGTH, alignedFrameLength - DataHeaderFlyweight.HEADER_LENGTH)).MustHaveHappened();
        }

        [Test]
        public void ShouldFillAfterAGap()
        {
            const int frameLength = 50;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int srcOffset = 0;
            UnsafeBuffer packet = new UnsafeBuffer(new byte[alignedFrameLength]);
            int termOffset = alignedFrameLength * 2;

            TermRebuilder.Insert(_termBuffer, termOffset, packet, alignedFrameLength);

            A.CallTo(() => _termBuffer.PutBytes((alignedFrameLength * 2) + DataHeaderFlyweight.HEADER_LENGTH, packet, srcOffset + DataHeaderFlyweight.HEADER_LENGTH, alignedFrameLength - DataHeaderFlyweight.HEADER_LENGTH)).MustHaveHappened();
        }

        [Test]
        public void ShouldFillGapButNotMoveTailOrHwm()
        {
            const int frameLength = 50;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int srcOffset = 0;
            UnsafeBuffer packet = new UnsafeBuffer(new byte[alignedFrameLength]);
            int termOffset = alignedFrameLength * 2;
            
            TermRebuilder.Insert(_termBuffer, termOffset, packet, alignedFrameLength);

            A.CallTo(() => _termBuffer.PutBytes((alignedFrameLength * 2) + DataHeaderFlyweight.HEADER_LENGTH, packet, srcOffset + DataHeaderFlyweight.HEADER_LENGTH, alignedFrameLength - DataHeaderFlyweight.HEADER_LENGTH)).MustHaveHappened();
        }
    }
}