using System;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Vector into a <seealso cref="IDirectBuffer"/> to be used for gathering IO as and offset and length.
    /// </summary>
    public sealed class DirectBufferVector
    {
        public IDirectBuffer buffer;
        public int offset;
        public int length;

        /// <summary>
        /// Default constructor so the fluent API can be used.
        /// </summary>
        public DirectBufferVector()
        {
        }

        /// <summary>
        /// Construct a new vector as a subset of a buffer.
        /// </summary>
        /// <param name="buffer"> which is the super set. </param>
        /// <param name="offset"> at which the vector begins. </param>
        /// <param name="length"> of the vector. </param>
        public DirectBufferVector(IDirectBuffer buffer, int offset, int length)
        {
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
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
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;

            return this;
        }

        /// <summary>
        /// The buffer which the vector applies to.
        /// </summary>
        /// <returns> buffer which the vector applies to. </returns>
        public IDirectBuffer Buffer()
        {
            return buffer;
        }

        /// <summary>
        /// The buffer which the vector applies to.
        /// </summary>
        /// <param name="buffer"> which the vector applies to. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Buffer(IDirectBuffer buffer)
        {
            this.buffer = buffer;
            return this;
        }

        /// <summary>
        /// Offset in the buffer at which the vector starts.
        /// </summary>
        /// <returns> offset in the buffer at which the vector starts. </returns>
        public int Offset()
        {
            return offset;
        }

        /// <summary>
        /// Offset in the buffer at which the vector starts.
        /// </summary>
        /// <param name="offset"> in the buffer at which the vector starts. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Offset(int offset)
        {
            this.offset = offset;
            return this;
        }

        /// <summary>
        /// Length of the vector in the buffer starting at the offset.
        /// </summary>
        /// <returns> length of the vector in the buffer starting at the offset. </returns>
        public int Length()
        {
            return length;
        }

        /// <summary>
        /// Length of the vector in the buffer starting at the offset.
        /// </summary>
        /// <param name="length"> of the vector in the buffer starting at the offset. </param>
        /// <returns> this for a fluent API. </returns>
        public DirectBufferVector Length(int length)
        {
            this.length = length;
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
            int capacity = buffer.Capacity;
            if (offset < 0 || offset >= capacity)
            {
                throw new ArgumentException("offset=" + offset + " capacity=" + capacity);
            }

            if (length < 0 || length > (capacity - offset))
            {
                throw new ArgumentException("offset=" + offset + " capacity=" + capacity + " length=" + length);
            }

            return this;
        }

        public override string ToString()
        {
            return "DirectBufferVector{" +
                   "buffer=" + buffer +
                   ", offset=" + offset +
                   ", length=" + length +
                   '}';
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
                messageLength += vector.length;

                if (messageLength < 0)
                {
                    throw new InvalidOperationException("length overflow: " + string.Join("\n", vector));
                }
            }

            return messageLength;
        }
    }
}