/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Vector into a <seealso cref="IDirectBuffer"/> to be used for gathering IO as and offset and length.
    /// </summary>
    public sealed class DirectBufferVector
    {
        private IDirectBuffer _buffer;
        private int _offset;
        private int _length;

        /// <summary>
        /// Default constructor so the fluent API can be used.
        /// </summary>
        public DirectBufferVector() { }

        /// <summary>
        /// Construct a new vector as a subset of a buffer.
        /// </summary>
        /// <param name="buffer"> which is the super set. </param>
        /// <param name="offset"> at which the vector begins. </param>
        /// <param name="length"> of the vector. </param>
        public DirectBufferVector(IDirectBuffer buffer, int offset, int length)
        {
            _buffer = buffer;
            _offset = offset;
            _length = length;
        }

        /// <summary>
        /// Reset the values.
        /// </summary>
        /// <param name="buffer"> which is the super set. </param>
        /// <param name="offset"> at which the vector begins. </param>
        /// <param name="length"> of the vector. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Reset(IDirectBuffer buffer, int offset, int length)
        {
            _buffer = buffer;
            _offset = offset;
            _length = length;

            return this;
        }

        /// <summary>
        /// The buffer which the vector applies to.
        /// </summary>
        /// <returns> buffer which the vector applies to. </returns>
        public IDirectBuffer Buffer()
        {
            return _buffer;
        }

        /// <summary>
        /// The buffer which the vector applies to.
        /// </summary>
        /// <param name="buffer"> which the vector applies to. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Buffer(IDirectBuffer buffer)
        {
            _buffer = buffer;
            return this;
        }

        /// <summary>
        /// Offset in the buffer at which the vector starts.
        /// </summary>
        /// <returns> offset in the buffer at which the vector starts. </returns>
        public int Offset()
        {
            return _offset;
        }

        /// <summary>
        /// Offset in the buffer at which the vector starts.
        /// </summary>
        /// <param name="offset"> in the buffer at which the vector starts. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Offset(int offset)
        {
            _offset = offset;
            return this;
        }

        /// <summary>
        /// Length of the vector in the buffer starting at the offset.
        /// </summary>
        /// <returns> length of the vector in the buffer starting at the offset. </returns>
        public int Length()
        {
            return _length;
        }

        /// <summary>
        /// Length of the vector in the buffer starting at the offset.
        /// </summary>
        /// <param name="length"> of the vector in the buffer starting at the offset. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Length(int length)
        {
            _length = length;
            return this;
        }

        /// <summary>
        /// Ensure the vector is valid for the buffer.
        /// </summary>
        /// <exception cref="NullReferenceException"> if the buffer is null. </exception>
        /// <exception cref="ArgumentException"> if the offset is out of range for the buffer. </exception>
        /// <exception cref="ArgumentException"> if the length is out of range for the buffer. </exception>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Validate()
        {
            int capacity = _buffer.Capacity;
            if (_offset < 0 || _offset >= capacity)
            {
                throw new ArgumentException("offset=" + _offset + " capacity=" + capacity);
            }

            if (_length < 0 || _length > (capacity - _offset))
            {
                throw new ArgumentException("offset=" + _offset + " capacity=" + capacity + " length=" + _length);
            }

            return this;
        }

        public override string ToString()
        {
            return "DirectBufferVector{" + "buffer=" + _buffer + ", offset=" + _offset + ", length=" + _length + '}';
        }

        /// <summary>
        /// Validate an array of vectors to make up a message and compute the total length.
        /// </summary>
        /// <param name="vectors"> to be validated summed. </param>
        /// <returns> the sum of the vector lengths. </returns>
        public static int ValidateAndComputeLength(DirectBufferVector[] vectors)
        {
            int messageLength = 0;
            foreach (DirectBufferVector vector in vectors)
            {
                vector.Validate();
                messageLength += vector._length;

                if (messageLength < 0)
                {
                    throw new InvalidOperationException("length overflow: " + string.Join("\n", vector));
                }
            }

            return messageLength;
        }
    }
}
