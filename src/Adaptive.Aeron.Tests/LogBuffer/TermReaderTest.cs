using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    [TestFixture]
    public class TermReaderTest
    {
        private static readonly int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
        private static readonly int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
        private const int INITIAL_TERM_ID = 7;

        private Header header;
        private UnsafeBuffer termBuffer;
        private ErrorHandler errorHandler;
        private IFragmentHandler handler;

        [SetUp]
        public virtual void SetUp()
        {
            header = new Header(INITIAL_TERM_ID, TERM_BUFFER_CAPACITY);
            termBuffer = A.Fake<UnsafeBuffer>();
            errorHandler = A.Fake<ErrorHandler>();
            handler = A.Fake<IFragmentHandler>();

            A.CallTo(() => termBuffer.Capacity).Returns(TERM_BUFFER_CAPACITY);
        }

        [Test]
        public virtual void ShouldPackPaddingAndOffsetIntoResultingStatus()
        {
            const int offset = 77;
            const int fragmentsRead = 999;

            long scanOutcome = TermReader.Pack(offset, fragmentsRead);

            Assert.That(TermReader.Offset(scanOutcome), Is.EqualTo(offset));
            Assert.That(TermReader.FragmentsRead(scanOutcome), Is.EqualTo(fragmentsRead));
        }

        [Test]
        public virtual void ShouldReadFirstMessage()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            const int termOffset = 0;

            A.CallTo(() => termBuffer.GetIntVolatile(0))
                .Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(FrameDescriptor.TypeOffset(0)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            long readOutcome = TermReader.Read(termBuffer, termOffset, handler, int.MaxValue, header, errorHandler);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(1));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened();
            A.CallTo(() => handler.OnFragment(termBuffer, HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public virtual void ShouldNotReadPastTail()
        {
            const int termOffset = 0;

            long readOutcome = TermReader.Read(termBuffer, termOffset, handler, int.MaxValue, header, errorHandler);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(0));
            Assert.That(TermReader.Offset(readOutcome), Is.EqualTo(termOffset));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened();
            A.CallTo(() => handler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public virtual void ShouldReadOneLimitedMessage()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            const int termOffset = 0;

            A.CallTo(() => termBuffer.GetIntVolatile(A<int>._))
                .Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(A<int>._))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            long readOutcome = TermReader.Read(termBuffer, termOffset, handler, 1, header, errorHandler);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(1));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened());
        }

        [Test]
        public virtual void ShouldReadMultipleMessages()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => termBuffer.GetIntVolatile(0)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetIntVolatile(alignedFrameLength)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(A<int>._)).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            long readOutcome = TermReader.Read(termBuffer, termOffset, handler, int.MaxValue, header, errorHandler);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(2));
            Assert.That(TermReader.Offset(readOutcome), Is.EqualTo(alignedFrameLength * 2));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => termBuffer.GetIntVolatile(alignedFrameLength)).MustHaveHappened())
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, alignedFrameLength + HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened());
        }

        [Test]
        public virtual void ShouldReadLastMessage()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;
            
            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(FrameDescriptor.TypeOffset(frameOffset))).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            long readOutcome = TermReader.Read(termBuffer, frameOffset, handler, int.MaxValue, header, errorHandler);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(1));
            Assert.That(TermReader.Offset(readOutcome), Is.EqualTo(TERM_BUFFER_CAPACITY));

            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, frameOffset + HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened());
        }

        [Test]
        public virtual void ShouldNotReadLastMessageWhenPadding()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(FrameDescriptor.TypeOffset(frameOffset))).Returns((short)FrameDescriptor.PADDING_FRAME_TYPE);
            
            long readOutcome = TermReader.Read(termBuffer, frameOffset, handler, int.MaxValue, header, errorHandler);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(0));
            Assert.That(TermReader.Offset(readOutcome), Is.EqualTo(TERM_BUFFER_CAPACITY));

            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).MustHaveHappened();
            A.CallTo(() => handler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }
    }
}