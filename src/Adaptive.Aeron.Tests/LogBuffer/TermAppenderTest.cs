using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    [TestFixture]
    public class TermAppenderTest
    {
        private static readonly int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
        private static readonly int META_DATA_BUFFER_LENGTH = LogBufferDescriptor.TERM_META_DATA_LENGTH;
        private const int MAX_FRAME_LENGTH = 1024;
        private static readonly int MAX_PAYLOAD_LENGTH = MAX_FRAME_LENGTH - DataHeaderFlyweight.HEADER_LENGTH;
        private static UnsafeBuffer DEFAULT_HEADER;
        private const int TERM_ID = 7;

        private UnsafeBuffer _termBuffer;
        private UnsafeBuffer _metaDataBuffer;
        private HeaderWriter _headerWriter;
        private TermAppender _termAppender;

        [SetUp]
        public void SetUp()
        {
            DEFAULT_HEADER = new UnsafeBuffer(new byte[DataHeaderFlyweight.HEADER_LENGTH]);

            _termBuffer = A.Fake<UnsafeBuffer>(x => x.Wrapping(new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH])));
            _metaDataBuffer = A.Fake<UnsafeBuffer>();

            _headerWriter = A.Fake<HeaderWriter>(x => x.Wrapping(new HeaderWriter(DataHeaderFlyweight.CreateDefaultHeader(0, 0, TERM_ID))));

            A.CallTo(() => _termBuffer.Capacity).Returns(TERM_BUFFER_LENGTH);
            A.CallTo(() => _metaDataBuffer.Capacity).Returns(META_DATA_BUFFER_LENGTH);

            _termAppender = new TermAppender(_termBuffer, _metaDataBuffer);
        }

        [Test]
        public void ShouldPackResult()
        {
            const int termId = 7;
            const int termOffset = -1;

            long result = TermAppender.Pack(termId, termOffset);

            Assert.That(TermAppender.TermId(result), Is.EqualTo(termId));
            Assert.That(TermAppender.TermOffset(result), Is.EqualTo(termOffset));
        }

        [Test]
        public void ShouldAppendFrameToEmptyLog()
        {
            int headerLength = DEFAULT_HEADER.Capacity;
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
            const int msgLength = 20;
            int frameLength = msgLength + headerLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int tail = 0;
            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).Returns(TermAppender.Pack(TERM_ID, tail));
            
            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo((long)alignedFrameLength));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened()
                .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, frameLength, TERM_ID)).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutBytes(headerLength, buffer, 0, msgLength)).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tail, frameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldAppendFrameTwiceToLog()
        {
            int headerLength = DEFAULT_HEADER.Capacity;
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
            const int msgLength = 20;
            int frameLength = msgLength + headerLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int tail = 0;

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
                .ReturnsNextFromSequence(TermAppender.Pack(TERM_ID, tail), TermAppender.Pack(TERM_ID, alignedFrameLength)); 

            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo((long)alignedFrameLength));
            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo((long)alignedFrameLength * 2));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, frameLength, TERM_ID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(headerLength, buffer, 0, msgLength)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tail, frameLength)).MustHaveHappened())

            .Then(A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened())
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, alignedFrameLength, frameLength, TERM_ID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(alignedFrameLength + headerLength, buffer, 0, msgLength)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(alignedFrameLength, frameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity()
        {
            const int msgLength = 120;
            int headerLength = DEFAULT_HEADER.Capacity;
            int requiredFrameSize = BitUtil.Align(headerLength + msgLength, FrameDescriptor.FRAME_ALIGNMENT);
            int tailValue = TERM_BUFFER_LENGTH - BitUtil.Align(msgLength, FrameDescriptor.FRAME_ALIGNMENT);
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
            int frameLength = TERM_BUFFER_LENGTH - tailValue;

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredFrameSize)).Returns(TermAppender.Pack(TERM_ID, tailValue));
            
            long expectResult = TermAppender.Pack(TERM_ID, TermAppender.TRIPPED);
            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo(expectResult));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredFrameSize)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tailValue, frameLength, TERM_ID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutShort(FrameDescriptor.TypeOffset(tailValue), (short)FrameDescriptor.PADDING_FRAME_TYPE)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tailValue, frameLength)).MustHaveHappened());
            
        }

        [Test]
        public void ShouldFragmentMessageOverTwoFrames()
        {
            int msgLength = MAX_PAYLOAD_LENGTH + 1;
            int headerLength = DEFAULT_HEADER.Capacity;
            int frameLength = headerLength + 1;
            int requiredCapacity = BitUtil.Align(headerLength + 1, FrameDescriptor.FRAME_ALIGNMENT) + MAX_FRAME_LENGTH;
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[msgLength]);
            int tail = 0;

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredCapacity)).Returns(TermAppender.Pack(TERM_ID, tail));

            Assert.That(_termAppender.AppendFragmentedMessage(_headerWriter, buffer, 0, msgLength, MAX_PAYLOAD_LENGTH), Is.EqualTo((long)requiredCapacity));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredCapacity)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, MAX_FRAME_LENGTH, TERM_ID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(tail + headerLength, buffer, 0, MAX_PAYLOAD_LENGTH)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutByte(FrameDescriptor.FlagsOffset(tail), FrameDescriptor.BEGIN_FRAG_FLAG)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tail, MAX_FRAME_LENGTH)).MustHaveHappened())

            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, MAX_FRAME_LENGTH, frameLength, TERM_ID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(MAX_FRAME_LENGTH + headerLength, buffer, MAX_PAYLOAD_LENGTH, 1)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutByte(FrameDescriptor.FlagsOffset(MAX_FRAME_LENGTH), FrameDescriptor.END_FRAG_FLAG)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(MAX_FRAME_LENGTH, frameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldClaimRegionForZeroCopyEncoding()
        {
            int headerLength = DEFAULT_HEADER.Capacity;
            const int msgLength = 20;
            int frameLength = msgLength + headerLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int tail = 0;
            BufferClaim bufferClaim = new BufferClaim();

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
                .Returns(TermAppender.Pack(TERM_ID, tail));
            
            Assert.That(_termAppender.Claim(_headerWriter, msgLength, bufferClaim), Is.EqualTo((long)alignedFrameLength));

            Assert.That(bufferClaim.Offset, Is.EqualTo(tail + headerLength));
            Assert.That(bufferClaim.Length, Is.EqualTo(msgLength));

            // Map flyweight or encode to buffer directly then call commit() when done
            bufferClaim.Commit();

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, frameLength, TERM_ID)).MustHaveHappened());
        }
    }
}