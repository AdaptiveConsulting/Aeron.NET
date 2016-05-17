using Adaptive.Aeron.LogBuffer;
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
            A.CallTo(() => _termBuffer.GetInt(0)).Returns(length);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, length);

            A.CallTo(() => _termBuffer.PutBytes(termOffset, packet, srcOffset, length)).MustHaveHappened()
                .Then(A.CallTo(() => _termBuffer.PutIntOrdered(termOffset, length)).MustHaveHappened());
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

            A.CallTo(() => _termBuffer.GetInt(tail)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(tail))).Returns((short)FrameDescriptor.PADDING_FRAME_TYPE);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, frameLength);

            A.CallTo(() => (_termBuffer).PutBytes(tail, packet, srcOffset, frameLength)).MustHaveHappened();
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

            A.CallTo(() => _termBuffer.GetInt(0)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetInt(alignedFrameLength)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetInt(alignedFrameLength * 2)).Returns(frameLength);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, alignedFrameLength);

            A.CallTo(() => (_termBuffer).PutBytes(tail, packet, srcOffset, alignedFrameLength)).MustHaveHappened();
        }

        [Test]
        public void ShouldFillAfterAGap()
        {
            const int frameLength = 50;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int srcOffset = 0;
            UnsafeBuffer packet = new UnsafeBuffer(new byte[alignedFrameLength]);
            int termOffset = alignedFrameLength * 2;

            A.CallTo(() => _termBuffer.GetInt(0)).Returns(0);
            A.CallTo(() => _termBuffer.GetInt(alignedFrameLength)).Returns(frameLength);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, alignedFrameLength);

            A.CallTo(() => (_termBuffer).PutBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength)).MustHaveHappened();
        }

        [Test]
        public void ShouldFillGapButNotMoveTailOrHwm()
        {
            const int frameLength = 50;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int srcOffset = 0;
            UnsafeBuffer packet = new UnsafeBuffer(new byte[alignedFrameLength]);
            int termOffset = alignedFrameLength * 2;

            A.CallTo(() => _termBuffer.GetInt(0)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetInt(alignedFrameLength)).Returns(0);

            TermRebuilder.Insert(_termBuffer, termOffset, packet, alignedFrameLength);

            A.CallTo(() => (_termBuffer).PutBytes(alignedFrameLength * 2, packet, srcOffset, alignedFrameLength)).MustHaveHappened();
        }
    }
}