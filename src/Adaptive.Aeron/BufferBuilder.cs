﻿/*
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
        private readonly UnsafeBuffer _buffer;
        private int _limit;

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
            _buffer = new UnsafeBuffer(new byte[initialCapacity]);
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
                ThrowHelper.ThrowArgumentException($"limit outside range: capacity={_buffer.Capacity:D} limit={limit:D}");
            }

            _limit = limit;
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
            return this;
            }

        /// <summary>
        /// Compact the buffer to reclaim unused space above the limit.
        /// </summary>
        /// <returns> the builder for fluent API usage. </returns>
	    public BufferBuilder Compact()
        {
            Resize(Math.Max(BufferBuilderUtil.MIN_ALLOCATED_CAPACITY, _limit));
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
            long requiredCapacity = (long)_limit + additionalCapacity;

            if (requiredCapacity > BufferBuilderUtil.MAX_CAPACITY)
            {
                string s = $"max capacity exceeded: limit={_limit:D} required={requiredCapacity:D}";
                ThrowHelper.ThrowInvalidOperationException(s);
            }

            int capacity = _buffer.Capacity;
            if (requiredCapacity > capacity)
            {
                int newCapacity = BufferBuilderUtil.FindSuitableCapacity(capacity, (int)requiredCapacity);
                Resize(newCapacity);
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
    }
}