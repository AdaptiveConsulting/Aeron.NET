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
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Reusable Builder for appending a sequence of buffers that grows internal capacity as needed.
    /// 
    /// Similar in concept to <see cref="StringBuilder"/>
    /// </summary>
    public class BufferBuilder
    {
        /// <summary>
        /// Maximum capcity to which the array can grow
        /// </summary>
        public const int MAX_CAPACITY = int.MaxValue - 8;

        /// <summary>
        /// Initial capcity for the internal buffer.
        /// </summary>
        public const int INITIAL_CAPACITY = 4096;

        private readonly UnsafeBuffer _mutableDirectBuffer;

        private byte[] _buffer;
        private int _limit;
        private int _capacity;

        /// <summary>
        /// Construct a buffer builder with a default growth increment of <seealso cref="INITIAL_CAPACITY"/>
        /// </summary>
        public BufferBuilder() : this(INITIAL_CAPACITY)
        {
        }

        /// <summary>
        /// Construct a buffer builder with an initial capacity that will be rounded up to the nearest power of 2.
        /// </summary>
        /// <param name="initialCapacity"> at which the capacity will start. </param>
        public BufferBuilder(int initialCapacity)
        {
            _capacity = BitUtil.FindNextPositivePowerOfTwo(initialCapacity);
            _buffer = new byte[_capacity];
            _mutableDirectBuffer = new UnsafeBuffer(_buffer);
        }

        /// <summary>
        /// The current capacity of the buffer.
        /// </summary>
        /// <returns> the current capacity of the buffer. </returns>
        public int Capacity()
        {
            return _capacity;
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
            if (limit < 0 || limit >= _capacity)
            {
                ThrowHelper.ThrowArgumentException($"Limit outside range: capacity={_capacity:D} limit={limit:D}");
            }

            _limit = limit;
        }

        /// <summary>
        /// The <seealso cref="IMutableDirectBuffer"/> that encapsulates the internal buffer.
        /// </summary>
        /// <returns> the <seealso cref="IMutableDirectBuffer"/> that encapsulates the internal buffer. </returns>
        public UnsafeBuffer Buffer()
        {
            return _mutableDirectBuffer;
        }

        /// <summary>
        /// Reset the builder to restart append operations. The internal buffer does not shrink.
        /// </summary>
        /// <returns> the builder for fluent API usage. </returns>
        public BufferBuilder Reset()
        {
            _limit = 0;
            return this;
            }

        /// <summary>
        /// Compact the buffer to reclaim unused space above the limit.
        /// </summary>
        /// <returns> the builder for fluent API usage. </returns>
	    public BufferBuilder Compact()
        {
            _capacity = Math.Max(INITIAL_CAPACITY, BitUtil.FindNextPositivePowerOfTwo(_limit));
            _buffer = CopyOf(_buffer, _capacity);
            _mutableDirectBuffer.Wrap(_buffer);

            return this;
        }

        /// <summary>
        /// Append a source buffer to the end of the internal buffer, resizing the internal buffer as required.
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

        private void EnsureCapacity(int additionalCapacity)
        {
            int requiredCapacity = _limit + additionalCapacity;

            if (requiredCapacity < 0)
            {
                string s = $"Insufficient capacity: limit={_limit:D} additional={additionalCapacity:D}";
                ThrowHelper.ThrowInvalidOperationException(s);
            }

            if (requiredCapacity > _capacity)
            {
                int newCapacity = FindSuitableCapacity(_capacity, requiredCapacity);
                byte[] newBuffer = CopyOf(_buffer, newCapacity);

                _capacity = newCapacity;
                _buffer = newBuffer;
                _mutableDirectBuffer.Wrap(newBuffer);
            }
        }

        private static int FindSuitableCapacity(int capacity, int requiredCapacity)
        {
            do
            {
                int newCapacity = capacity + (capacity >> 1);

                if (newCapacity < 0 || newCapacity > MAX_CAPACITY)
                {
                    if (capacity == MAX_CAPACITY)
                    {
                        ThrowHelper.ThrowInvalidOperationException("Max capacity reached: " + MAX_CAPACITY);
                    }

                    capacity = MAX_CAPACITY;
                }
                else
                {
                    capacity = newCapacity;
                }
            } while (capacity < requiredCapacity);

            return capacity;
        }

        internal static T[] CopyOf<T>(T[] original, int newLength)
        {
            T[] dest = new T[newLength];
            Array.Copy(original, 0, dest, 0, Math.Min(original.Length, newLength));
            return dest;
        }
    }
}