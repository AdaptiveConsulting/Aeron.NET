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
        private const int TermBufferLength = LogBufferDescriptor.TERM_MIN_LENGTH;
        private static readonly int MetaDataBufferLength = LogBufferDescriptor.TERM_META_DATA_LENGTH;
        private const int MaxFrameLength = 1024;
        private const int MaxPayloadLength = MaxFrameLength - DataHeaderFlyweight.HEADER_LENGTH;
        private static UnsafeBuffer _defaultHeader;
        private const int TermID = 7;

         private IAtomicBuffer _termBuffer;
        private IAtomicBuffer _metaDataBuffer;
        private HeaderWriter _headerWriter;
        private TermAppender _termAppender;

        [SetUp]
        public void SetUp()
        {
            _defaultHeader = new UnsafeBuffer(new byte[DataHeaderFlyweight.HEADER_LENGTH]);

            _termBuffer = A.Fake<IAtomicBuffer>(x => x.Wrapping(new UnsafeBuffer(new byte[TermBufferLength])));
            _metaDataBuffer = A.Fake<UnsafeBuffer>();
            _headerWriter = A.Fake<HeaderWriter>(x => x.Wrapping(new HeaderWriter(DataHeaderFlyweight.CreateDefaultHeader(0, 0, TermID))));

            A.CallTo(() => _termBuffer.Capacity).Returns(TermBufferLength);
            A.CallTo(() => _metaDataBuffer.Capacity).Returns(MetaDataBufferLength);

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
            int headerLength = _defaultHeader.Capacity;
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
            const int msgLength = 20;
            int frameLength = msgLength + headerLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int tail = 0;
            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).Returns(TermAppender.Pack(TermID, tail));
            
            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo((long)alignedFrameLength));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened()
                .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, frameLength, TermID)).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutBytes(headerLength, buffer, 0, msgLength)).MustHaveHappened())
                .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tail, frameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldAppendFrameTwiceToLog()
        {
            int headerLength = _defaultHeader.Capacity;
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
            const int msgLength = 20;
            int frameLength = msgLength + headerLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int tail = 0;

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
                .ReturnsNextFromSequence(TermAppender.Pack(TermID, tail), TermAppender.Pack(TermID, alignedFrameLength)); 

            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo((long)alignedFrameLength));
            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo((long)alignedFrameLength * 2));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, frameLength, TermID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(headerLength, buffer, 0, msgLength)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tail, frameLength)).MustHaveHappened())

            .Then(A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened())
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, alignedFrameLength, frameLength, TermID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(alignedFrameLength + headerLength, buffer, 0, msgLength)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(alignedFrameLength, frameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldPadLogAndTripWhenAppendingWithInsufficientRemainingCapacity()
        {
            const int msgLength = 120;
            int headerLength = _defaultHeader.Capacity;
            int requiredFrameSize = BitUtil.Align(headerLength + msgLength, FrameDescriptor.FRAME_ALIGNMENT);
            int tailValue = TermBufferLength - BitUtil.Align(msgLength, FrameDescriptor.FRAME_ALIGNMENT);
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
            int frameLength = TermBufferLength - tailValue;

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredFrameSize)).Returns(TermAppender.Pack(TermID, tailValue));
            
            long expectResult = TermAppender.Pack(TermID, TermAppender.TRIPPED);
            Assert.That(_termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, 0, msgLength), Is.EqualTo(expectResult));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredFrameSize)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tailValue, frameLength, TermID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutShort(FrameDescriptor.TypeOffset(tailValue), (short)FrameDescriptor.PADDING_FRAME_TYPE)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tailValue, frameLength)).MustHaveHappened());
            
        }

        [Test]
        public void ShouldFragmentMessageOverTwoFrames()
        {
            int msgLength = MaxPayloadLength + 1;
            int headerLength = _defaultHeader.Capacity;
            int frameLength = headerLength + 1;
            int requiredCapacity = BitUtil.Align(headerLength + 1, FrameDescriptor.FRAME_ALIGNMENT) + MaxFrameLength;
            UnsafeBuffer buffer = new UnsafeBuffer(new byte[msgLength]);
            int tail = 0;

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredCapacity)).Returns(TermAppender.Pack(TermID, tail));

            Assert.That(_termAppender.AppendFragmentedMessage(_headerWriter, buffer, 0, msgLength, MaxPayloadLength), Is.EqualTo((long)requiredCapacity));

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, requiredCapacity)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, MaxFrameLength, TermID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(tail + headerLength, buffer, 0, MaxPayloadLength)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutByte(FrameDescriptor.FlagsOffset(tail), FrameDescriptor.BEGIN_FRAG_FLAG)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(tail, MaxFrameLength)).MustHaveHappened())

            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, MaxFrameLength, frameLength, TermID)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutBytes(MaxFrameLength + headerLength, buffer, MaxPayloadLength, 1)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutByte(FrameDescriptor.FlagsOffset(MaxFrameLength), FrameDescriptor.END_FRAG_FLAG)).MustHaveHappened())
            .Then(A.CallTo(() => _termBuffer.PutIntOrdered(MaxFrameLength, frameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldClaimRegionForZeroCopyEncoding()
        {
            int headerLength = _defaultHeader.Capacity;
            const int msgLength = 20;
            int frameLength = msgLength + headerLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int tail = 0;
            BufferClaim bufferClaim = new BufferClaim();

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength))
                .Returns(TermAppender.Pack(TermID, tail));
            
            Assert.That(_termAppender.Claim(_headerWriter, msgLength, bufferClaim), Is.EqualTo((long)alignedFrameLength));

            Assert.That(bufferClaim.Offset, Is.EqualTo(tail + headerLength));
            Assert.That(bufferClaim.Length, Is.EqualTo(msgLength));

            // Map flyweight or encode to buffer directly then call commit() when done
            bufferClaim.Commit();

            A.CallTo(() => _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedFrameLength)).MustHaveHappened()
            .Then(A.CallTo(() => _headerWriter.Write(_termBuffer, tail, frameLength, TermID)).MustHaveHappened());
        }
    }
}