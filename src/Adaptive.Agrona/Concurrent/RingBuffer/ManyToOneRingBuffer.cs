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

using System;
using System.Threading;

namespace Adaptive.Agrona.Concurrent.RingBuffer
{
    /// <summary>
    /// A ring-buffer that supports the exchange of messages from many producers to a single consumer.
    /// </summary>
    public class ManyToOneRingBuffer : IRingBuffer
    {
        /// <summary>
        /// Record type is padding to prevent fragmentation in the buffer.
        /// </summary>
        public const int PaddingMsgTypeId = -1;

        /// <summary>
        /// Buffer has insufficient capacity to record a message.
        /// </summary>
        private const int InsufficientCapacity = -2;

        private readonly int _capacity;
        private readonly int _maxMsgLength;
        private readonly int _tailPositionIndex;
        private readonly int _headCachePositionIndex;
        private readonly int _headPositionIndex;
        private readonly int _correlationIdCounterIndex;
        private readonly int _consumerHeartbeatIndex;
        private readonly IAtomicBuffer _buffer;

        /// <summary>
        /// Construct a new <seealso cref="RingBuffer"/> based on an underlying <seealso cref="IAtomicBuffer"/>.
        /// The underlying buffer must a power of 2 in size plus sufficient space
        /// for the <seealso cref="RingBufferDescriptor.TrailerLength"/>.
        /// </summary>
        /// <param name="buffer"> via which events will be exchanged. </param>
        /// <exception cref="InvalidOperationException"> if the buffer capacity is not a power of 2
        /// plus <seealso cref="RingBufferDescriptor.TrailerLength"/> in capacity. </exception>
        public ManyToOneRingBuffer(IAtomicBuffer buffer)
        {
            _buffer = buffer;
            RingBufferDescriptor.CheckCapacity(buffer.Capacity);
            _capacity = buffer.Capacity - RingBufferDescriptor.TrailerLength;

            buffer.VerifyAlignment();

            _maxMsgLength = _capacity / 8;
            _tailPositionIndex = _capacity + RingBufferDescriptor.TailPositionOffset;
            _headCachePositionIndex = _capacity + RingBufferDescriptor.HeadCachePositionOffset;
            _headPositionIndex = _capacity + RingBufferDescriptor.HeadPositionOffset;
            _correlationIdCounterIndex = _capacity + RingBufferDescriptor.CorrelationCounterOffset;
            _consumerHeartbeatIndex = _capacity + RingBufferDescriptor.ConsumerHeartbeatOffset;
        }

        /// <summary>
        /// Get the capacity of the ring-buffer in bytes for exchange.
        /// </summary>
        /// <returns> the capacity of the ring-buffer in bytes for exchange. </returns>
        public int Capacity()
        {
            return _capacity;
        }

        /// <inheritdoc />
        public bool Write(int msgTypeId, IDirectBuffer srcBuffer, int offset, int length)
        {
            RecordDescriptor.CheckTypeId(msgTypeId);
            CheckMsgLength(length);

            IAtomicBuffer buffer = _buffer;
            int recordLength = length + RecordDescriptor.HeaderLength;
            int recordIndex = ClaimCapacity(buffer, recordLength);

            if (InsufficientCapacity == recordIndex)
            {
                return false;
            }

            buffer.PutIntOrdered(RecordDescriptor.LengthOffset(recordIndex), -recordLength);
            // TODO JPW original: UnsafeAccess.UNSAFE.storeFence();
            Thread.MemoryBarrier();
            buffer.PutInt(RecordDescriptor.TypeOffset(recordIndex), msgTypeId);
            buffer.PutBytes(RecordDescriptor.EncodedMsgOffset(recordIndex), srcBuffer, offset, length);
            buffer.PutIntOrdered(RecordDescriptor.LengthOffset(recordIndex), recordLength);

            return true;
        }


        /// <summary>
        /// Read as many messages as are available from the ring buffer.
        /// </summary>
        /// <param name="handler"> to be called for processing each message in turn. </param>
        /// <returns> the number of messages that have been processed. </returns>
        public int Read(MessageHandler handler)
        {
            return Read(handler, int.MaxValue);
        }

        /// <summary>
        /// Read as many messages as are available from the ring buffer to up a supplied maximum.
        /// </summary>
        /// <param name="handler"> to be called for processing each message in turn. </param>
        /// <param name="messageCountLimit"> the number of messages will be read in a single invocation. </param>
        /// <returns> the number of messages that have been processed. </returns>
        public virtual int Read(MessageHandler handler, int messageCountLimit)
        {
            int messagesRead = 0;

            IAtomicBuffer buffer = _buffer;
            int headPositionIndex = _headPositionIndex;
            long head = buffer.GetLong(headPositionIndex);

            int capacity = _capacity;
            int headIndex = (int)head & (capacity - 1);
            int maxBlockLength = capacity - headIndex;
            int bytesRead = 0;

            try
            {
                while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit))
                {
                    int recordIndex = headIndex + bytesRead;
                    int recordLength = buffer.GetIntVolatile(RecordDescriptor.LengthOffset(recordIndex));
                    if (recordLength <= 0)
                    {
                        break;
                    }

                    bytesRead += BitUtil.Align(recordLength, RecordDescriptor.Alignment);

                    int messageTypeId = buffer.GetInt(RecordDescriptor.TypeOffset(recordIndex));
                    if (PaddingMsgTypeId == messageTypeId)
                    {
                        continue;
                    }

                    ++messagesRead;
                    handler(messageTypeId, buffer, recordIndex + RecordDescriptor.HeaderLength,
                        recordLength - RecordDescriptor.HeaderLength);
                }
            }
            finally
            {
                if (bytesRead != 0)
                {
                    buffer.SetMemory(headIndex, bytesRead, (byte)0);
                    buffer.PutLongOrdered(headPositionIndex, head + bytesRead);
                }
            }

            return messagesRead;
        }


        /// <summary>
        /// The maximum message length in bytes supported by the underlying ring buffer.
        /// </summary>
        /// <returns> the maximum message length in bytes supported by the underlying ring buffer. </returns>
        public int MaxMsgLength()
        {
            return _maxMsgLength;
        }

        /// <summary>
        /// Get the next value that can be used for a correlation id on an message when a response needs to be correlated.
        /// 
        /// This method should be thread safe.
        /// </summary>
        /// <returns> the next value in the correlation sequence. </returns>
        public long NextCorrelationId()
        {
            return _buffer.GetAndAddLong(_correlationIdCounterIndex, 1);
        }

        /// <summary>
        /// Get the underlying buffer used by the RingBuffer for storage.
        /// </summary>
        /// <returns> the underlying buffer used by the RingBuffer for storage. </returns>
        public IAtomicBuffer Buffer()
        {
            return _buffer;
        }

        /// <summary>
        /// Set the time of the last consumer heartbeat.
        /// 
        /// <b>Note:</b> The value for time must be valid across processes.
        /// </summary>
        /// <param name="time"> of the last consumer heartbeat. </param>
        public void ConsumerHeartbeatTime(long time)
        {
            _buffer.PutLongOrdered(_consumerHeartbeatIndex, time);
        }

        /// <summary>
        /// The time of the last consumer heartbeat.
        /// </summary>
        /// <returns> the time of the last consumer heartbeat. </returns>
        public long ConsumerHeartbeatTime()
        {
            return _buffer.GetLongVolatile(_consumerHeartbeatIndex);
        }

        /// <summary>
        /// The position in bytes from start up of the producers.  The figure includes the headers.
        /// This is the range they are working with but could still be in the act of working with.
        /// </summary>
        /// <returns> number of bytes produced by the producers in claimed space. </returns>
        public long ProducerPosition()
        {
            return _buffer.GetLongVolatile(_tailPositionIndex);
        }

        /// <summary>
        /// The position in bytes from start up for the consumers.  The figure includes the headers.
        /// </summary>
        /// <returns> the count of bytes consumed by the consumers. </returns>
        public long ConsumerPosition()
        {
            return _buffer.GetLongVolatile(_headPositionIndex);
        }

        /// <summary>
        /// Size of the backlog of bytes in the buffer between producers and consumers. The figure includes the size of headers.
        /// </summary>
        /// <returns> size of the backlog of bytes in the buffer between producers and consumers. </returns>
        public int Size()
        {
            long headBefore;
            long tail;
            var headAfter = _buffer.GetLongVolatile(_headPositionIndex);

            do
            {
                headBefore = headAfter;
                tail = _buffer.GetLongVolatile(_tailPositionIndex);
                headAfter = _buffer.GetLongVolatile(_headPositionIndex);
            } while (headAfter != headBefore);

            return (int)(tail - headAfter);
        }

        /// <inheritdoc />
        public bool Unblock()
        {
            IAtomicBuffer buffer = _buffer;
            long headPosition = buffer.GetLongVolatile(_headPositionIndex);
            long tailPosition = buffer.GetLongVolatile(_tailPositionIndex);

            if (headPosition == tailPosition)
            {
                return false;
            }

            int mask = _capacity - 1;
            int consumerIndex = (int)(headPosition & mask);
            int producerIndex = (int)(tailPosition & mask);

            bool unblocked = false;
            int length = buffer.GetIntVolatile(consumerIndex);
            if (length < 0)
            {
                buffer.PutInt(RecordDescriptor.TypeOffset(consumerIndex), PaddingMsgTypeId);
                buffer.PutIntOrdered(RecordDescriptor.LengthOffset(consumerIndex), -length);
                unblocked = true;
            }
            else if (0 == length)
            {
                // go from (consumerIndex to producerIndex) or (consumerIndex to capacity)
                int limit = producerIndex > consumerIndex ? producerIndex : _capacity;
                int i = consumerIndex + RecordDescriptor.Alignment;

                do
                {
                    // read the top int of every long (looking for length aligned to 8=ALIGNMENT)
                    length = buffer.GetIntVolatile(i);
                    if (0 != length)
                    {
                        if (ScanBackToConfirmStillZeroed(buffer, i, consumerIndex))
                        {
                            buffer.PutInt(RecordDescriptor.TypeOffset(consumerIndex), PaddingMsgTypeId);
                            buffer.PutIntOrdered(RecordDescriptor.LengthOffset(consumerIndex), i - consumerIndex);
                            unblocked = true;
                        }

                        break;
                    }

                    i += RecordDescriptor.Alignment;
                } while (i < limit);
            }

            return unblocked;
        }

        public int TryClaim(int msgTypeId, int length)
        {
            RecordDescriptor.CheckTypeId(msgTypeId);
            CheckMsgLength(length);

            IAtomicBuffer buffer = _buffer;
            int recordLength = length + RecordDescriptor.HeaderLength;
            int recordIndex = ClaimCapacity(buffer, recordLength);

            if (InsufficientCapacity == recordIndex)
            {
                return recordIndex;
            }

            buffer.PutIntOrdered(RecordDescriptor.LengthOffset(recordIndex), -recordLength);
            // TODO JPW original: UnsafeAccess.UNSAFE.storeFence();
            Thread.MemoryBarrier();
            buffer.PutInt(RecordDescriptor.TypeOffset(recordIndex), msgTypeId);

            return RecordDescriptor.EncodedMsgOffset(recordIndex);
        }

        public void Commit(int index)
        {
            int recordIndex = ComputeRecordIndex(index);
            IAtomicBuffer buffer = _buffer;
            int recordLength = VerifyClaimedSpaceNotReleased(buffer, recordIndex);

            buffer.PutIntOrdered(RecordDescriptor.LengthOffset(recordIndex), -recordLength);
        }

        public void Abort(int index)
        {
            int recordIndex = ComputeRecordIndex(index);
            IAtomicBuffer buffer = _buffer;
            int recordLength = VerifyClaimedSpaceNotReleased(buffer, recordIndex);

            buffer.PutInt(RecordDescriptor.TypeOffset(recordIndex), PaddingMsgTypeId);
            buffer.PutIntOrdered(RecordDescriptor.LengthOffset(recordIndex), -recordLength);
        }

        private static bool ScanBackToConfirmStillZeroed(IAtomicBuffer buffer, int from, int limit)
        {
            var i = from - RecordDescriptor.Alignment;
            var allZeros = true;
            while (i >= limit)
            {
                if (0 != buffer.GetIntVolatile(i))
                {
                    allZeros = false;
                    break;
                }

                i -= RecordDescriptor.Alignment;
            }

            return allZeros;
        }

        private void CheckMsgLength(int length)
        {
            if (length > _maxMsgLength)
            {
                var msg = $"encoded message exceeds maxMsgLength of {_maxMsgLength:D}, length={length:D}";

                throw new ArgumentException(msg);
            }
        }

        private int ClaimCapacity(IAtomicBuffer buffer, int recordLength)
        {
            var requiredCapacity = BitUtil.Align(recordLength, RecordDescriptor.Alignment);
            var capacity = this._capacity;
            var tailPositionIndex = _tailPositionIndex;
            var headCachePositionIndex = _headCachePositionIndex;
            var mask = capacity - 1;

            var head = buffer.GetLongVolatile(headCachePositionIndex);

            long tail;
            int tailIndex;
            int padding;
            do
            {
                tail = buffer.GetLongVolatile(tailPositionIndex);
                var availableCapacity = capacity - (int)(tail - head);

                if (requiredCapacity > availableCapacity)
                {
                    head = buffer.GetLongVolatile(_headPositionIndex);

                    if (requiredCapacity > (capacity - (int)(tail - head)))
                    {
                        return InsufficientCapacity;
                    }

                    buffer.PutLongOrdered(headCachePositionIndex, head);
                }

                padding = 0;
                tailIndex = (int)tail & mask;
                var toBufferEndLength = capacity - tailIndex;

                if (requiredCapacity > toBufferEndLength)
                {
                    var headIndex = (int)head & mask;

                    if (requiredCapacity > headIndex)
                    {
                        head = buffer.GetLongVolatile(_headPositionIndex);
                        headIndex = (int)head & mask;
                        if (requiredCapacity > headIndex)
                        {
                            return InsufficientCapacity;
                        }

                        buffer.PutLongOrdered(headCachePositionIndex, head);
                    }

                    padding = toBufferEndLength;
                }
            } while (!buffer.CompareAndSetLong(tailPositionIndex, tail, tail + requiredCapacity + padding));

            if (0 != padding)
            {
                buffer.PutIntOrdered(RecordDescriptor.LengthOffset(tailIndex), -padding);
                Thread.MemoryBarrier();

                buffer.PutInt(RecordDescriptor.TypeOffset(tailIndex), PaddingMsgTypeId);
                buffer.PutIntOrdered(RecordDescriptor.LengthOffset(tailIndex), padding);
                tailIndex = 0;
            }

            return tailIndex;
        }

        private int ComputeRecordIndex(int index)
        {
            int recordIndex = index - RecordDescriptor.HeaderLength;
            if (recordIndex < 0 || recordIndex > (_capacity - RecordDescriptor.HeaderLength))
            {
                throw new ArgumentException("invalid message index " + index);
            }

            return recordIndex;
        }

        private int VerifyClaimedSpaceNotReleased(IAtomicBuffer buffer, int recordIndex)
        {
            int recordLength = buffer.GetInt(RecordDescriptor.LengthOffset(recordIndex));
            if (recordLength < 0)
            {
                return recordLength;
            }

            throw new InvalidOperationException("claimed space previously " +
                                                (PaddingMsgTypeId ==
                                                 buffer.GetInt(RecordDescriptor.TypeOffset(recordIndex))
                                                    ? "aborted"
                                                    : "committed"));
        }
    }
}