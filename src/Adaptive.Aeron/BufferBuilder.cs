/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.CompilerServices;
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.LogBuffer.LogBufferDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;
using static Adaptive.Aeron.Protocol.HeaderFlyweight;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Reusable Builder for appending a sequence of buffers that grows internal capacity as needed.
    /// 
    /// Similar in concept to <see cref="StringBuilder"/>
    /// </summary>
    public sealed class BufferBuilder
    {
        internal const int MAX_CAPACITY = Int32.MaxValue - 8;
        internal const int INIT_MIN_CAPACITY = 4096;

        private int _limit;
        private int _nextTermOffset = Aeron.NULL_VALUE;
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer();
        readonly UnsafeBuffer headerBuffer = new UnsafeBuffer();
        readonly Header completeHeader = new Header(0, 0);


        /// <summary>
        /// Construct a buffer builder with an initial capacity of zero and isDirect false.
        /// </summary>
        public BufferBuilder() : this(0)
        {
        }

        /// <summary>
        /// Construct a buffer builder with an initial capacity that will be rounded up to the nearest power of 2.
        /// </summary>
        /// <param name="initialCapacity"> at which the capacity will start. </param>
        public BufferBuilder(int initialCapacity)
        {
            if (initialCapacity < 0 || initialCapacity > MAX_CAPACITY)
            {
                throw new ArgumentException("initialCapacity outside range 0 - " + MAX_CAPACITY +
                                            ": initialCapacity=" + initialCapacity);
            }

            if (initialCapacity > 0)
            {
                _buffer.Wrap(new byte[initialCapacity]);
            }

            headerBuffer.Wrap(new byte[HEADER_LENGTH]);
        }

        /// <summary>
        /// The current capacity of the buffer.
        /// </summary>
        /// <returns> the current capacity of the buffer. </returns>
        public int Capacity()
        {
            return _buffer.Capacity;
        }

        /// <summary>
        /// The current limit of the buffer that has been used by append operations.
        /// </summary>
        /// <returns> the current limit of the buffer that has been used by append operations. </returns>
        public int Limit()
        {
            return _limit;
        }

        /// <summary>
        /// Set this limit for this buffer as the position at which the next append operation will occur.
        /// </summary>
        /// <param name="limit"> to be the new value. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Limit(int limit)
        {
            if (limit < 0 || limit >= _buffer.Capacity)
            {
                ThrowHelper.ThrowArgumentException(
                    $"limit outside range: capacity={_buffer.Capacity:D} limit={limit:D}");
            }

            _limit = limit;
        }

        /// <summary>
        /// Get the value which the next term offset for a fragment to be assembled should begin at.
        /// </summary>
        /// <returns> the value which the next term offset for a fragment to be assembled should begin at. </returns>
        public int NextTermOffset()
        {
            return _nextTermOffset;
        }

        /// <summary>
        /// Set the value which the next term offset for a fragment to be assembled should begin at.
        /// </summary>
        /// <param name="offset"> which the next term offset for a fragment to be assembled should begin at. </param>
        public void NextTermOffset(int offset)
        {
            _nextTermOffset = offset;
        }

        /// <summary>
        /// The <seealso cref="IMutableDirectBuffer"/> that encapsulates the internal buffer.
        /// </summary>
        /// <returns> the <seealso cref="IMutableDirectBuffer"/> that encapsulates the internal buffer. </returns>
        public IMutableDirectBuffer Buffer()
        {
            return _buffer;
        }

        /// <summary>
        /// Reset the builder to restart append operations. The internal buffer does not shrink.
        /// </summary>
        /// <returns> the builder for fluent API usage. </returns>
        public BufferBuilder Reset()
        {
            _limit = 0;
            _nextTermOffset = Aeron.NULL_VALUE;
            completeHeader.Context = null;
            completeHeader.FragmentedFrameLength = Aeron.NULL_VALUE;
            return this;
        }

        /// <summary>
        /// Compact the buffer to reclaim unused space above the limit.
        /// </summary>
        /// <returns> the builder for fluent API usage. </returns>
        public BufferBuilder Compact()
        {
            int newCapacity = Math.Max(INIT_MIN_CAPACITY, _limit);
            if (newCapacity < _buffer.Capacity)
            {
                Resize(newCapacity);
            }

            return this;
        }

        /// <summary>
        /// Append a source buffer to the end of the internal buffer, resizing the internal buffer when required.
        /// </summary>
        /// <param name="srcBuffer"> from which to copy. </param>
        /// <param name="srcOffset"> in the source buffer from which to copy. </param>
        /// <param name="length"> in bytes to copy from the source buffer. </param>
        /// <returns> the builder for fluent API usage. </returns>
        public BufferBuilder Append(IDirectBuffer srcBuffer, int srcOffset, int length)
        {
            EnsureCapacity(length);

            srcBuffer.GetBytes(srcOffset, _buffer, _limit, length);
            _limit += length;

            return this;
        }

        /// <summary>
        /// Capture information available in the header of the very first frame.
        /// </summary>
        /// <param name="header"> of the first frame. </param>
        /// <returns> the builder for fluent API usage. </returns>
        public BufferBuilder CaptureHeader(Header header)
        {
            completeHeader.InitialTermId = header.InitialTermId;
            completeHeader.PositionBitsToShift = header.PositionBitsToShift;
            completeHeader.Offset = 0;
            completeHeader.Buffer = headerBuffer;

            headerBuffer.PutBytes(0, header.Buffer, header.Offset, HEADER_LENGTH);
            return this;
        }

        /// <summary>
        /// Use the information from the header of the last frame to create a header for the assembled message, i.e. fixups
        /// the flags and the frame length.
        /// </summary>
        /// <param name="header"> of the last frame. </param>
        /// <returns> complete message header. </returns>
        public Header CompleteHeader(Header header)
        {
            int firstFrameLength = headerBuffer.GetInt(FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LittleEndian);
            int fragmentedFrameLength = ComputeFragmentedFrameLength(_limit, firstFrameLength - HEADER_LENGTH);
            completeHeader.Context = header.Context;
            completeHeader.FragmentedFrameLength = fragmentedFrameLength;

            headerBuffer.PutInt(FRAME_LENGTH_FIELD_OFFSET, HEADER_LENGTH + _limit, ByteOrder.LittleEndian);
            // compute complete flags
            headerBuffer.PutByte(FLAGS_OFFSET, (byte)(headerBuffer.GetByte(FLAGS_OFFSET) | header.Flags));
            // compute the `fragmented frame length` of the complete message

            return completeHeader;
        }

        private void EnsureCapacity(int additionalLength)
        {
            long requiredCapacity = (long)_limit + additionalLength;
            int capacity = _buffer.Capacity;

            if (requiredCapacity > capacity)
            {
                if (requiredCapacity > MAX_CAPACITY)
                {
                    throw new InvalidOperationException("insufficient capacity: maxCapacity=" + MAX_CAPACITY +
                                                        " limit=" + _limit + " additionalLength=" + additionalLength);
                }

                Resize(FindSuitableCapacity(capacity, requiredCapacity));
            }
        }

        private void Resize(int newCapacity)
        {
            _buffer.Wrap(CopyOf(_buffer.ByteArray, newCapacity));
        }

        private static T[] CopyOf<T>(T[] original, int newLength)
        {
            var dest = new T[newLength];
            Array.Copy(original, 0, dest, 0, Math.Min(original.Length, newLength));
            return dest;
        }

        internal static int FindSuitableCapacity(int capacity, long requiredCapacity)
        {
            long newCapacity = Math.Max(capacity, INIT_MIN_CAPACITY);

            while (newCapacity < requiredCapacity)
            {
                newCapacity = newCapacity + (newCapacity >> 1);
                if (newCapacity > MAX_CAPACITY)
                {
                    newCapacity = MAX_CAPACITY;
                    break;
                }
            }

            return (int)newCapacity;
        }
    }
}