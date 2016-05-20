using System;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
    /// <para>
    /// The claimed space is in <seealso cref="Buffer()"/> between <seealso cref="Offset()"/> and <seealso cref="Offset()"/> + <seealso cref="Length()"/>.
    /// When the buffer is filled with message data, use <seealso cref="Commit()"/> to make it available to subscribers.
    /// </para>
    /// <para>
    /// If the claimed space is no longer required it can be aborted by calling <seealso cref="Abort()"/>.
    /// </para>
    /// </summary>
    public class BufferClaim
    {
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(IntPtr.Zero, 0);

        /// <summary>
        /// Wrap a region of an underlying log buffer so can can represent a claimed space for use by a publisher.
        /// </summary>
        /// <param name="buf"> to be wrapped. </param>
        /// <param name="offset"> at which the claimed region begins including space for the header. </param>
        /// <param name="length"> length of the underlying claimed region including space for the header. </param>
        public void Wrap(UnsafeBuffer buf, int offset, int length)
        {
            _buffer.Wrap(buf, offset, length);
        }

        /// <summary>
        /// The referenced buffer to be used.
        /// </summary>
        /// <returns> the referenced buffer to be used.. </returns>
        public UnsafeBuffer Buffer => _buffer;

        /// <summary>
        /// The offset in the buffer at which the claimed range begins.
        /// </summary>
        /// <returns> offset in the buffer at which the range begins. </returns>
        public int Offset => DataHeaderFlyweight.HEADER_LENGTH;


        /// <summary>
        /// The length of the claimed range in the buffer.
        /// </summary>
        /// <returns> length of the range in the buffer. </returns>
        public int Length => _buffer.Capacity - DataHeaderFlyweight.HEADER_LENGTH;


        /// <summary>
        /// Get the value stored in the reserve space at the end of a data frame header.
        /// 
        /// Note: The value is in <seealso cref="ByteOrder.LittleEndian"/> format.
        /// </summary>
        /// <returns> the value stored in the reserve space at the end of a data frame header. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public long ReservedValue()
        {
            return _buffer.GetLong(DataHeaderFlyweight.RESERVED_VALUE_OFFSET);
        }

        /// <summary>
        /// Write the provided value into the reserved space at the end of the data frame header.
        /// 
        /// Note: The value will be written in <seealso cref="ByteOrder.LittleEndian"/> format.
        /// </summary>
        /// <param name="value"> to be stored in the reserve space at the end of a data frame header. </param>
        /// <returns> this for fluent API semantics. </returns>
        /// <seealso cref="DataHeaderFlyweight" />
        public BufferClaim ReservedValue(long value)
        {
            _buffer.PutLong(DataHeaderFlyweight.RESERVED_VALUE_OFFSET, value);
            return this;
        }

        /// <summary>
        /// Commit the message to the log buffer so that is it available to subscribers.
        /// </summary>
        public void Commit()
        {
            var frameLength = _buffer.Capacity;

            _buffer.PutIntOrdered(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        }

        /// <summary>
        /// Abort a claim of the message space to the log buffer so that the log can progress by ignoring this claim.
        /// </summary>
        public void Abort()
        {
            var frameLength = _buffer.Capacity;

            _buffer.PutShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short) HeaderFlyweight.HDR_TYPE_PAD);
            _buffer.PutIntOrdered(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        }
    }
}