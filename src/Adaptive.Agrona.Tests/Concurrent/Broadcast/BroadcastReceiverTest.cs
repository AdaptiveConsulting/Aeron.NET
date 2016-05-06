using System;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent.Broadcast
{
    [TestFixture]
    public class BroadcastReceiverTest
    {
        private const int MsgTypeID = 7;
        private const int Capacity = 1024;
        private static readonly int TotalBufferLength = Capacity + BroadcastBufferDescriptor.TrailerLength;
        private static readonly int TailIntentCounterOffset = Capacity + BroadcastBufferDescriptor.TailIntentCounterOffset;
        private static readonly int TailCounterIndex = Capacity + BroadcastBufferDescriptor.TailCounterOffset;
        private static readonly int LatestCounterIndex = Capacity + BroadcastBufferDescriptor.LatestCounterOffset;

        private IAtomicBuffer _buffer;
        private BroadcastReceiver _broadcastReceiver;

        [SetUp]
        public void SetUp()
        {
            _buffer = A.Fake<IAtomicBuffer>();
            A.CallTo(() => _buffer.Capacity).Returns(TotalBufferLength);
            _broadcastReceiver = new BroadcastReceiver(_buffer);
        }

        [Test]
        public void ShouldCalculateCapacityForBuffer()
        {
            Assert.AreEqual(Capacity, _broadcastReceiver.Capacity());
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public void ShouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
        {
            const int capacity = 777;
            var totalBufferLength = capacity + BroadcastBufferDescriptor.TrailerLength;

            A.CallTo(() => _buffer.Capacity).Returns(totalBufferLength);

            new BroadcastReceiver(_buffer);
        }

        [Test]
        public void ShouldNotBeLappedBeforeReception()
        {
            Assert.AreEqual(0, _broadcastReceiver.LappedCount());
        }

        [Test]
        public void ShouldNotReceiveFromEmptyBuffer()
        {
            Assert.False(_broadcastReceiver.ReceiveNext());
        }

        [Test]
        public void ShouldReceiveFirstMessageFromBuffer()
        {
            const int length = 8;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            long tail = recordLengthAligned;
            var latestRecord = tail - recordLengthAligned;
            var recordOffset = (int) latestRecord;

            A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).Returns(tail);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffset))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffset))).Returns(MsgTypeID);

            Assert.True(_broadcastReceiver.ReceiveNext());
            Assert.AreEqual(MsgTypeID, _broadcastReceiver.TypeId());
            Assert.AreEqual(_buffer, _broadcastReceiver.Buffer());
            Assert.AreEqual(RecordDescriptor.GetMsgOffset(recordOffset), _broadcastReceiver.Offset());
            Assert.AreEqual(length, _broadcastReceiver.Length());

            Assert.True(_broadcastReceiver.Validate());

            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).MustHaveHappened();
            A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).MustHaveHappened();
        }

        [Test]
        public void ShouldReceiveTwoMessagesFromBuffer()
        {
            const int length = 8;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            long tail = recordLengthAligned*2;
            var latestRecord = tail - recordLengthAligned;
            const int recordOffsetOne = 0;
            var recordOffsetTwo = (int) latestRecord;

            A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).Returns(tail);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);

            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffsetOne))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffsetOne))).Returns(MsgTypeID);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffsetTwo))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffsetTwo))).Returns(MsgTypeID);


            Assert.IsTrue(_broadcastReceiver.ReceiveNext());
            Assert.AreEqual(MsgTypeID, _broadcastReceiver.TypeId());
            Assert.AreEqual(_buffer, _broadcastReceiver.Buffer());
            Assert.AreEqual(RecordDescriptor.GetMsgOffset(recordOffsetOne), _broadcastReceiver.Offset());
            Assert.AreEqual(length, _broadcastReceiver.Length());
            Assert.True(_broadcastReceiver.Validate());

            Assert.IsTrue(_broadcastReceiver.ReceiveNext());
            Assert.AreEqual(MsgTypeID, _broadcastReceiver.TypeId());
            Assert.AreEqual(_buffer, _broadcastReceiver.Buffer());
            Assert.AreEqual(RecordDescriptor.GetMsgOffset(recordOffsetTwo), _broadcastReceiver.Offset());
            Assert.AreEqual(length, _broadcastReceiver.Length());
            Assert.True(_broadcastReceiver.Validate());

            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).MustHaveHappened());
        }

        [Test]
        public virtual void ShouldLateJoinTransmission()
        {
            const int length = 8;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            var tail = Capacity*3L + RecordDescriptor.HeaderLength + recordLengthAligned;
            var latestRecord = tail - recordLengthAligned;
            var recordOffset = (int) latestRecord & (Capacity - 1);

            A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).Returns(tail);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);
            A.CallTo(() => _buffer.GetLong(LatestCounterIndex)).Returns(latestRecord);

            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffset))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffset))).Returns(MsgTypeID);

            Assert.IsTrue(_broadcastReceiver.ReceiveNext());
            Assert.AreEqual(MsgTypeID, _broadcastReceiver.TypeId());
            Assert.AreEqual(_buffer, _broadcastReceiver.Buffer());
            Assert.AreEqual(RecordDescriptor.GetMsgOffset(recordOffset), _broadcastReceiver.Offset());
            Assert.AreEqual(length, _broadcastReceiver.Length());
            Assert.True(_broadcastReceiver.Validate());

            Assert.Greater(_broadcastReceiver.LappedCount(), 0);
        }


        [Test]
        public virtual void ShouldCopeWithPaddingRecordAndWrapOfBufferForNextRecord()
        {
            const int length = 120;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            var catchupTail = (Capacity*2L) - RecordDescriptor.HeaderLength;
            var postPaddingTail = catchupTail + RecordDescriptor.HeaderLength + recordLengthAligned;
            var latestRecord = catchupTail - recordLengthAligned;
            var catchupOffset = (int) latestRecord & (Capacity - 1);

            A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).ReturnsNextFromSequence(catchupTail, postPaddingTail);
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).ReturnsNextFromSequence(catchupTail, postPaddingTail);
            A.CallTo(() => _buffer.GetLong(LatestCounterIndex)).Returns(latestRecord);

            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(catchupOffset))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(catchupOffset))).Returns(MsgTypeID);

            var paddingOffset = (int) catchupTail & (Capacity - 1);
            var recordOffset = (int) (postPaddingTail - recordLengthAligned) & (Capacity - 1);

            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(paddingOffset))).Returns(RecordDescriptor.PaddingMsgTypeID);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffset))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffset))).Returns(MsgTypeID);

            Assert.IsTrue(_broadcastReceiver.ReceiveNext()); // To catch up to record before padding.
            Assert.IsTrue(_broadcastReceiver.ReceiveNext()); // no skip over the padding and read next record.
            Assert.AreEqual(MsgTypeID, _broadcastReceiver.TypeId());
            Assert.AreEqual(_buffer, _broadcastReceiver.Buffer());
            Assert.AreEqual(RecordDescriptor.GetMsgOffset(recordOffset), _broadcastReceiver.Offset());
            Assert.AreEqual(length, _broadcastReceiver.Length());
            Assert.True(_broadcastReceiver.Validate());
        }

        [Test]
        public virtual void ShouldDealWithRecordBecomingInvalidDueToOverwrite()
        {
            const int length = 8;
            var recordLength = length + RecordDescriptor.HeaderLength;
            var recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            long tail = recordLengthAligned;
            var latestRecord = tail - recordLengthAligned;
            var recordOffset = (int) latestRecord;


            A.CallTo(() => _buffer.GetLongVolatile(TailIntentCounterOffset)).ReturnsNextFromSequence(tail, tail + (Capacity - recordLengthAligned));
            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).Returns(tail);

            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffset))).Returns(recordLength);
            A.CallTo(() => _buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffset))).Returns(MsgTypeID);

            Assert.IsTrue(_broadcastReceiver.ReceiveNext());
            Assert.AreEqual(MsgTypeID, _broadcastReceiver.TypeId());
            Assert.AreEqual(_buffer, _broadcastReceiver.Buffer());
            Assert.AreEqual(RecordDescriptor.GetMsgOffset(recordOffset), _broadcastReceiver.Offset());
            Assert.AreEqual(length, _broadcastReceiver.Length());

            Assert.False(_broadcastReceiver.Validate()); // Need to receiveNext() to catch up with transmission again.

            A.CallTo(() => _buffer.GetLongVolatile(TailCounterIndex)).MustHaveHappened();
        }
    }
}