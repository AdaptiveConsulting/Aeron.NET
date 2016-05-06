using System;
using System.Threading;

namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Receive messages broadcast from a BroadcastTransmitter via an underlying buffer. Receivers can join
    /// a transmission stream at any point by consuming the latest message at the point of joining and forward.
    /// <para>
    /// If a Receiver cannot keep up with the transmission stream then loss will be experienced. Loss is not an
    /// error condition.
    /// </para>
    /// <para>
    /// <b>Note:</b> Each Receiver is not threadsafe but there can be zero or many receivers to a transmission stream.
    /// </para>
    /// </summary>
    public class BroadcastReceiver
    {
        private long _cursor;
        private long _nextRecord;
        private int _recordOffset;

        private readonly int _capacity;
        private readonly int _tailIntentCounterIndex;
        private readonly int _tailCounterIndex;

        private readonly int _latestCounterIndex;
        private readonly IAtomicBuffer _buffer;
        private readonly AtomicLong _lappedCount = new AtomicLong();

        /// <summary>
        /// Construct a new broadcast receiver based on an underlying <seealso cref="IAtomicBuffer"/>.
        /// The underlying buffer must a power of 2 in size plus sufficient space
        /// for the <seealso cref="BroadcastBufferDescriptor.TrailerLength"/>.
        /// </summary>
        /// <param name="buffer"> via which messages will be exchanged. </param>
        /// <exception cref="InvalidOperationException"> if the buffer capacity is not a power of 2
        /// plus <seealso cref="BroadcastBufferDescriptor.TrailerLength"/> in capacity. </exception>
        public BroadcastReceiver(IAtomicBuffer buffer)
        {
            _buffer = buffer;
            _capacity = buffer.Capacity - BroadcastBufferDescriptor.TrailerLength;

            BroadcastBufferDescriptor.CheckCapacity(_capacity);
            buffer.VerifyAlignment();

            _tailIntentCounterIndex = _capacity + BroadcastBufferDescriptor.TailIntentCounterOffset;
            _tailCounterIndex = _capacity + BroadcastBufferDescriptor.TailCounterOffset;
            _latestCounterIndex = _capacity + BroadcastBufferDescriptor.LatestCounterOffset;
        }

        /// <summary>
        /// Get the capacity of the underlying broadcast buffer.
        /// </summary>
        /// <returns> the capacity of the underlying broadcast buffer. </returns>
        public virtual int Capacity()
        {
            return _capacity;
        }

        /// <summary>
        /// Get the number of times the transmitter has lapped this receiver around the buffer. On each lap
        /// as least a buffer's worth of loss will be experienced.
        /// <para>
        /// <b>Note:</b> This method is threadsafe for calling from an external monitoring thread.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the capacity of the underlying broadcast buffer. </returns>
        public virtual long LappedCount()
        {
            return _lappedCount.get();
        }

        /// <summary>
        /// Type of the message received.
        /// </summary>
        /// <returns> typeId of the message received. </returns>
        public virtual int TypeId()
        {
            return _buffer.GetInt(RecordDescriptor.GetTypeOffset(_recordOffset));
        }

        /// <summary>
        /// The offset for the beginning of the next message in the transmission stream.
        /// </summary>
        /// <returns> offset for the beginning of the next message in the transmission stream. </returns>
        public virtual int Offset()
        {
            return RecordDescriptor.GetMsgOffset(_recordOffset);
        }

        /// <summary>
        /// The length of the next message in the transmission stream.
        /// </summary>
        /// <returns> length of the next message in the transmission stream. </returns>
        public virtual int Length()
        {
            return _buffer.GetInt(RecordDescriptor.GetLengthOffset(_recordOffset)) - RecordDescriptor.HeaderLength;
        }

        /// <summary>
        /// The underlying buffer containing the broadcast message stream.
        /// </summary>
        /// <returns> the underlying buffer containing the broadcast message stream. </returns>
        public virtual IMutableDirectBuffer Buffer()
        {
            return _buffer;
        }

        /// <summary>
        /// Non-blocking receive of next message from the transmission stream.
        /// <para>
        /// If loss has occurred then <seealso cref="LappedCount()"/> will be incremented.
        /// 
        /// </para>
        /// </summary>
        /// <returns> true if transmission is available with <seealso cref="Offset()"/>, <seealso cref="Length()"/> and <seealso cref="TypeId()"/>
        /// set for the next message to be consumed. If no transmission is available then false. </returns>
        public virtual bool ReceiveNext()
        {
            var isAvailable = false;
            var buffer = _buffer;
            var tail = buffer.GetLongVolatile(_tailCounterIndex);
            var cursor = _nextRecord;

            if (tail > cursor)
            {
                var recordOffset = (int) cursor & (_capacity - 1);

                if (!Validate(cursor))
                {
                    _lappedCount.lazySet(_lappedCount.get() + 1);

                    cursor = buffer.GetLong(_latestCounterIndex);
                    recordOffset = (int) cursor & (_capacity - 1);
                }

                _cursor = cursor;
                _nextRecord = cursor + BitUtil.Align(buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffset)), RecordDescriptor.RecordAlignment);

                if (RecordDescriptor.PaddingMsgTypeID == buffer.GetInt(RecordDescriptor.GetTypeOffset(recordOffset)))
                {
                    recordOffset = 0;
                    _cursor = _nextRecord;
                    _nextRecord += BitUtil.Align(buffer.GetInt(RecordDescriptor.GetLengthOffset(recordOffset)), RecordDescriptor.RecordAlignment);
                }

                _recordOffset = recordOffset;
                isAvailable = true;
            }

            return isAvailable;
        }

        /// <summary>
        /// Validate that the current received record is still valid and has not been overwritten.
        /// <para>
        /// If the receiver is not consuming messages fast enough to keep up with the transmitter then loss
        /// can be experienced resulting in messages being overwritten thus making them no longer valid.
        /// 
        /// </para>
        /// </summary>
        /// <returns> true if still valid otherwise false. </returns>
        public virtual bool Validate()
        {
            // TODO check equivalent semantics
            // Replaces UNSAFE.loadFence(); Needed to prevent older loads being moved ahead of the validate, see j.u.c.StampedLock.
            Thread.MemoryBarrier();

            return Validate(_cursor);
        }

        private bool Validate(long cursor)
        {
            return cursor + _capacity > _buffer.GetLongVolatile(_tailIntentCounterIndex);
        }
    }
}