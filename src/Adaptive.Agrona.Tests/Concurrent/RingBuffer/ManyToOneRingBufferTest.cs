using System;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.RingBuffer;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent.RingBuffer
{
    [TestFixture]
    public class ManyToOneRingBufferTest
    {
        private const int MsgTypeID = 7;
        private const int Capacity = 4096;
        private static readonly int TotalBufferLength = Capacity + RingBufferDescriptor.TrailerLength;
        private static readonly int TailCounterIndex = Capacity + RingBufferDescriptor.TailPositionOffset;
        private static readonly int HeadCounterIndex = Capacity + RingBufferDescriptor.HeadPositionOffset;
        private static readonly int HeadCounterCacheIndex = Capacity + RingBufferDescriptor.HeadCachePositionOffset;

        private IAtomicBuffer _buffer;
        private ManyToOneRingBuffer _ringBuffer;

        [SetUp]
        public void SetUp()
        {
            _buffer = A.Fake<IAtomicBuffer>();

            A.CallTo(() => _buffer.Capacity).Returns(TotalBufferLength);

            _ringBuffer = new ManyToOneRingBuffer(_buffer);
        }

        [Test]
        public void ShouldWriteToEmptyBuffer()
        {
            const int length = 8;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            const long tail = 0L;
            const long head = 0L;

            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);
            A.CallTo(() => _buffer.CompareAndSetLong(TailCounterIndex, tail, tail + alignedRecordLength)).Returns(true);


            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int srcIndex = 0;

            Assert.True(_ringBuffer.Write(MsgTypeID, srcBuffer, srcIndex, length));

            A.CallTo(() => _buffer.PutLongOrdered((int) tail, RecordDescriptor.MakeHeader(-recordLength, MsgTypeID))).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.EncodedMsgOffset((int) tail), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutIntOrdered(RecordDescriptor.LengthOffset((int) tail), recordLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldRejectWriteWhenInsufficientSpace()
        {
            const int length = 200;
            const long head = 0L;
            var tail = head + (Capacity - BitUtil.Align(length - RecordDescriptor.Alignment, RecordDescriptor.Alignment));

            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);

            const int srcIndex = 0;
            Assert.False(_ringBuffer.Write(MsgTypeID, srcBuffer, srcIndex, length));

            A.CallTo(() => _buffer.PutInt(A<int>._, A<int>._)).MustNotHaveHappened();
            A.CallTo(() => _buffer.CompareAndSetLong(A<int>._, A<long>._, A<long>._)).MustNotHaveHappened();
            A.CallTo(() => _buffer.PutBytes(A<int>._, srcBuffer, A<int>._, A<int>._)).MustNotHaveHappened();
            A.CallTo(() => _buffer.PutIntOrdered(A<int>._, A<int>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldRejectWriteWhenBufferFull()
        {
            const int length = 8;
            const long head = 0L;

            var tail = head + Capacity;

            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);

            const int srcIndex = 0;
            Assert.False(_ringBuffer.Write(MsgTypeID, srcBuffer, srcIndex, length));

            A.CallTo(() => _buffer.PutInt(A<int>._, A<int>._)).MustNotHaveHappened();
            A.CallTo(() => _buffer.CompareAndSetLong(A<int>._, A<long>._, A<long>._)).MustNotHaveHappened();
            A.CallTo(() => _buffer.PutIntOrdered(A<int>._, A<int>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldInsertPaddingRecordPlusMessageOnBufferWrap()
        {
            const int length = 200;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            long tail = Capacity - RecordDescriptor.HeaderLength;
            var head = tail - (RecordDescriptor.Alignment*4);

            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);
            A.CallTo(() => _buffer.CompareAndSetLong(TailCounterIndex, tail, tail + alignedRecordLength + RecordDescriptor.Alignment)).Returns(true);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);

            const int srcIndex = 0;
            Assert.True(_ringBuffer.Write(MsgTypeID, srcBuffer, srcIndex, length));


            A.CallTo(() => _buffer.PutLongOrdered((int) tail, RecordDescriptor.MakeHeader(RecordDescriptor.HeaderLength, ManyToOneRingBuffer.PaddingMsgTypeId))).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutLongOrdered(0, RecordDescriptor.MakeHeader(-recordLength, MsgTypeID))).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.EncodedMsgOffset(0), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutIntOrdered(RecordDescriptor.LengthOffset(0), recordLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldInsertPaddingRecordPlusMessageOnBufferWrapWithHeadEqualToTail()
        {
            const int length = 200;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            long tail = Capacity - RecordDescriptor.HeaderLength;
            var head = tail;

            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);
            A.CallTo(() => _buffer.CompareAndSetLong(TailCounterIndex, tail, tail + alignedRecordLength + RecordDescriptor.Alignment)).Returns(true);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);

            const int srcIndex = 0;
            Assert.True(_ringBuffer.Write(MsgTypeID, srcBuffer, srcIndex, length));


            A.CallTo(() => _buffer.PutLongOrdered((int) tail, RecordDescriptor.MakeHeader(RecordDescriptor.HeaderLength, ManyToOneRingBuffer.PaddingMsgTypeId))).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutLongOrdered(0, RecordDescriptor.MakeHeader(-recordLength, MsgTypeID))).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.EncodedMsgOffset(0), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutIntOrdered(RecordDescriptor.LengthOffset(0), recordLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldReadNothingFromEmptyBuffer()
        {
            const long head = 0L;

            A.CallTo(() => _buffer.GetLong(HeadCounterIndex)).Returns(head);

            MessageHandler handler = (msgTypeId, buffer, index, length) => Assert.Fail("should not be called");

            var messagesRead = _ringBuffer.Read(handler);

            Assert.AreEqual(messagesRead, 0);
        }

        [Test]
        public void ShouldNotReadSingleMessagePartWayThroughWriting()
        {
            const long head = 0L;
            var headIndex = (int) head;

            A.CallTo(() => _buffer.GetLong(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetIntVolatile(RecordDescriptor.LengthOffset(headIndex))).Returns(0);

            var times = new int[1];

            MessageHandler handler = (msgTypeId, buffer, index, length) => times[0]++;
            var messagesRead = _ringBuffer.Read(handler);

            Assert.AreEqual(messagesRead, 0);
            Assert.AreEqual(times[0], 0);

            A.CallTo(() => _buffer.GetLongVolatile(headIndex)).MustHaveHappened(Repeated.Exactly.Once);
            A.CallTo(() => _buffer.SetMemory(headIndex, 0, 0)).MustNotHaveHappened();
            A.CallTo(() => _buffer.PutLongOrdered(HeadCounterIndex, headIndex)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldReadTwoMessages()
        {
            const int msgLength = 16;

            var recordLength = RecordDescriptor.HeaderLength + msgLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            long tail = alignedRecordLength*2;
            const long head = 0L;
            var headIndex = (int) head;

            A.CallTo(() => _buffer.GetLong(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(headIndex)).Returns(RecordDescriptor.MakeHeader(recordLength, MsgTypeID));
            A.CallTo(() => _buffer.GetLongVolatile(headIndex + alignedRecordLength)).Returns(RecordDescriptor.MakeHeader(recordLength, MsgTypeID));

            var times = new int[1];
            MessageHandler handler = (msgTypeId, buffer, index, length) => times[0]++;
            var messagesRead = _ringBuffer.Read(handler);

            Assert.AreEqual(messagesRead, 2);
            Assert.AreEqual(times[0], 2);

            A.CallTo(() => _buffer.SetMemory(headIndex, alignedRecordLength*2, 0)).MustHaveHappened(Repeated.Exactly.Once).Then(
                A.CallTo(() => _buffer.PutLongOrdered(HeadCounterIndex, tail)).MustHaveHappened(Repeated.Exactly.Once));
        }

        [Test]
        public void ShouldLimitReadOfMessages()
        {
            const int msgLength = 16;
            var recordLength = RecordDescriptor.HeaderLength + msgLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            const long head = 0L;
            var headIndex = (int) head;

            A.CallTo(() => _buffer.GetLong(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(headIndex)).Returns(RecordDescriptor.MakeHeader(recordLength, MsgTypeID));

            var times = new int[1];

            MessageHandler handler = (msgTypeId, buffer, index, length) => times[0]++;
            const int limit = 1;
            var messagesRead = _ringBuffer.Read(handler, limit);

            Assert.AreEqual(messagesRead, 1);
            Assert.AreEqual(times[0], 1);

            A.CallTo(() => _buffer.SetMemory(headIndex, alignedRecordLength, 0)).MustHaveHappened(Repeated.Exactly.Once).Then(
                A.CallTo(() => _buffer.PutLongOrdered(HeadCounterIndex, head + alignedRecordLength)).MustHaveHappened(Repeated.Exactly.Once));
        }

        [Test]
        public void ShouldCopeWithExceptionFromHandler()
        {
            const int msgLength = 16;
            var recordLength = RecordDescriptor.HeaderLength + msgLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            long tail = alignedRecordLength*2;
            const long head = 0L;
            var headIndex = (int) head;

            A.CallTo(() => _buffer.GetLong(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(headIndex)).Returns(RecordDescriptor.MakeHeader(recordLength, MsgTypeID));
            A.CallTo(() => _buffer.GetLongVolatile(headIndex + alignedRecordLength)).Returns(RecordDescriptor.MakeHeader(recordLength, MsgTypeID));

            var times = new int[1];
            MessageHandler handler = (msgTypeId, buffer, index, length) =>
            {
                times[0]++;
                if (times[0] == 2)
                {
                    throw new Exception();
                }
            };

            try
            {
                _ringBuffer.Read(handler);
            }
            catch (Exception)
            {
                Assert.AreEqual(times[0], 2);

                A.CallTo(() => _buffer.SetMemory(headIndex, alignedRecordLength*2, 0)).MustHaveHappened(Repeated.Exactly.Once)
                    .Then(A.CallTo(() => _buffer.PutLongOrdered(HeadCounterIndex, tail)).MustHaveHappened(Repeated.Exactly.Once));

                return;
            }

            Assert.Fail("Should have thrown exception");
        }

        [Test]
        public void ShouldNotUnblockWhenEmpty()
        {
            long position = RecordDescriptor.Alignment*4;
            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(position);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(position);

            Assert.False(_ringBuffer.Unblock());
        }

        [Test]
        public void ShouldUnblockMessageWithHeader()
        {
            var messageLength = RecordDescriptor.Alignment*4;
            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(messageLength);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns((long) messageLength*2);
            A.CallTo(() => _buffer.GetIntVolatile(messageLength)).Returns(-messageLength);

            Assert.True(_ringBuffer.Unblock());

            A.CallTo(() => _buffer.PutLongOrdered(messageLength, RecordDescriptor.MakeHeader(messageLength, ManyToOneRingBuffer.PaddingMsgTypeId))).MustHaveHappened();
        }

        public void ShouldUnblockGapWithZeros()
        {
            var messageLength = RecordDescriptor.Alignment*4;
            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(messageLength);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns((long) messageLength*3);
            A.CallTo(() => _buffer.GetIntVolatile(messageLength*2)).Returns(messageLength);

            Assert.True(_ringBuffer.Unblock());


            A.CallTo(() => _buffer.PutLongOrdered(messageLength, RecordDescriptor.MakeHeader(messageLength, ManyToOneRingBuffer.PaddingMsgTypeId))).MustHaveHappened();
        }

        [Test]
        public void ShouldNotUnblockGapWithMessageRaceOnSecondMessageIncreasingTailThenInterrupting()
        {
            var messageLength = RecordDescriptor.Alignment*4;
            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(messageLength);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns((long) messageLength*3);
            A.CallTo(() => _buffer.GetIntVolatile(messageLength*2)).ReturnsNextFromSequence(0, messageLength);

            Assert.False(_ringBuffer.Unblock());

            A.CallTo(() => _buffer.PutLongOrdered(messageLength, RecordDescriptor.MakeHeader(messageLength, ManyToOneRingBuffer.PaddingMsgTypeId))).MustNotHaveHappened();
        }

        [Test]
        public void ShouldNotUnblockGapWithMessageRaceWhenScanForwardTakesAnInterrupt()
        {
            var messageLength = RecordDescriptor.Alignment*4;
            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(messageLength);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns((long) messageLength*3);
            A.CallTo(() => _buffer.GetIntVolatile(messageLength*2)).ReturnsNextFromSequence(0, messageLength);
            A.CallTo(() => _buffer.GetIntVolatile(messageLength*2 + RecordDescriptor.Alignment)).Returns(7);

            Assert.False(_ringBuffer.Unblock());

            A.CallTo(() => _buffer.PutLongOrdered(messageLength, RecordDescriptor.MakeHeader(messageLength, ManyToOneRingBuffer.PaddingMsgTypeId))).MustNotHaveHappened();
        }

        [Test]
        public void ShouldCalculateCapacityForBuffer()
        {
            Assert.AreEqual(_ringBuffer.Capacity(), (Capacity));
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public void ShouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
        {
            const int capacity = 777;
            var totalBufferLength = capacity + RingBufferDescriptor.TrailerLength;
            new ManyToOneRingBuffer(new UnsafeBuffer(new byte[totalBufferLength]));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void ShouldThrowExceptionWhenMaxMessageSizeExceeded()
        {
            var srcBuffer = new UnsafeBuffer(new byte[1024]);

            _ringBuffer.Write(MsgTypeID, srcBuffer, 0, _ringBuffer.MaxMsgLength() + 1);
        }

        [Test]
        public void ShouldInsertPaddingAndWriteToBuffer()
        {
            const int padding = 200;
            const int messageLength = 400;
            var recordLength = messageLength + RecordDescriptor.HeaderLength;
            var alignedRecordLength = BitUtil.Align(recordLength, RecordDescriptor.Alignment);

            long tail = 2*Capacity - padding;
            var head = tail;

            // free space is (200 + 300) more than message length (400) but contiguous space (300) is less than message length (400)
            long headCache = Capacity + 300;

            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterIndex)).Returns(head);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);
            A.CallTo(() => _buffer.GetLongVolatile(HeadCounterCacheIndex)).Returns(headCache);
            A.CallTo(() => _buffer.CompareAndSetLong(TailCounterIndex, tail, tail + alignedRecordLength + padding)).Returns(true);

            var srcBuffer = new UnsafeBuffer(new byte[messageLength]);
            Assert.True(_ringBuffer.Write(MsgTypeID, srcBuffer, 0, messageLength));
        }
    }
}