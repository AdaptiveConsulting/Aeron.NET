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
    public class TermBlockScannerTest
    {
        private IAtomicBuffer _termBuffer;

        [SetUp]
        public void SetUp()
        {
            _termBuffer = A.Fake<IAtomicBuffer>();
            A.CallTo(() => _termBuffer.Capacity).Returns(LogBufferDescriptor.TERM_MIN_LENGTH);
        }

        [Test]
        public void ShouldScanEmptyBuffer()
        {
            const int offset = 0;
            int limit = _termBuffer.Capacity;

            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);
            Assert.That(newOffset, Is.EqualTo(offset));
        }

        [Test]
        public void ShouldReadFirstMessage()
        {
            const int offset = 0;
            int limit = _termBuffer.Capacity;
            const int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);

            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(offset)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);
            Assert.That(newOffset, Is.EqualTo(alignedMessageLength));
        }

        [Test]
        public void ShouldReadBlockOfTwoMessages()
        {
            const int offset = 0;
            int limit = _termBuffer.Capacity;
            const int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);

            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(offset)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(alignedMessageLength)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);

            Assert.That(newOffset, Is.EqualTo(alignedMessageLength * 2));
        }

        [Test]
        public void ShouldReadBlockOfThreeMessagesThatFillBuffer()
        {
            const int offset = 0;
            int limit = _termBuffer.Capacity;
            const int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);
            int thirdMessageLength = limit - (alignedMessageLength * 2);

            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(offset)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(alignedMessageLength)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength * 2)))
                .Returns(thirdMessageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(alignedMessageLength * 2)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
         
            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);

            Assert.That(newOffset, Is.EqualTo(limit));
        }

        [Test]
        public void ShouldReadBlockOfTwoMessagesBecauseOfLimit()
        {
            const int offset = 0;
            const int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);
            int limit = (alignedMessageLength * 2) + 1;

            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(offset)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(alignedMessageLength)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength * 2)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(alignedMessageLength * 2)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            
            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);

            Assert.That(newOffset, Is.EqualTo(alignedMessageLength * 2));
        }

        [Test]
        public void ShouldFailToReadFirstMessageBecauseOfLimit()
        {
            const int offset = 0;
            const int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);
            int limit = alignedMessageLength - 1;

            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(offset)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);

            Assert.That(newOffset, Is.EqualTo(offset));
        }

        [Test]
        public void ShouldReadOneMessageOnLimit()
        {
            const int offset = 0;
            const int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);
            int limit = alignedMessageLength;

            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset)))
                .Returns(messageLength);

            int newOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);

            Assert.That(newOffset, Is.EqualTo(alignedMessageLength));
        }

        [Test]
        public void ShouldReadBlockOfOneMessageThenPadding()
        {
            const int offset = 0;
            int limit = _termBuffer.Capacity;
            int messageLength = 50;
            int alignedMessageLength = BitUtil.Align(messageLength, FrameDescriptor.FRAME_ALIGNMENT);
            
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(offset))).Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(offset))).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength))).Returns(messageLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(alignedMessageLength))).Returns((short)HeaderFlyweight.HDR_TYPE_PAD);
            
            int firstOffset = TermBlockScanner.Scan(_termBuffer, offset, limit);
            Assert.That(firstOffset, Is.EqualTo(alignedMessageLength));

            int secondOffset = TermBlockScanner.Scan(_termBuffer, firstOffset, limit);
            Assert.That(secondOffset, Is.EqualTo(alignedMessageLength * 2));
        }
    }
}