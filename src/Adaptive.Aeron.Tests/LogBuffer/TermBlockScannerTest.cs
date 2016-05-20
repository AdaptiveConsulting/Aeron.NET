using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    [TestFixture]
    public class TermBlockScannerTest
    {
        private UnsafeBuffer _termBuffer;

        [SetUp]
        public void SetUp()
        {
            _termBuffer = A.Fake<UnsafeBuffer>();
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
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength)))
                .Returns(messageLength);

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
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength * 2)))
                .Returns(thirdMessageLength);
         
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
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength)))
                .Returns(messageLength);
            A.CallTo(() => _termBuffer.GetIntVolatile(FrameDescriptor.LengthOffset(alignedMessageLength * 2)))
                .Returns(messageLength);
            
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
    }
}