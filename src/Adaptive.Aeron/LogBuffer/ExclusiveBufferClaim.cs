using System;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
    /// 
    /// <seealso cref="ExclusiveBufferClaim"/>s offer additional functionality over standard <seealso cref="BufferClaim"/>s in that the header
    /// can be manipulated for setting flags and type. This allows the user to implement things such as their own
    /// fragmentation policy.
    /// 
    /// The claimed space is in <seealso cref="Buffer()"/> between <seealso cref="Offset()"/> and <seealso cref="Offset()"/> + <seealso cref="Length()"/>.
    /// When the buffer is filled with message data, use <seealso cref="Commit()"/> to make it available to subscribers.
    /// 
    /// If the claimed space is no longer required it can be aborted by calling <seealso cref="Abort()"/>.
    /// 
    /// <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-frame">Data Frame</a>
    /// </summary>
    /// <seealso cref="ExclusivePublication.TryClaim(int, ExclusiveBufferClaim)"/>
    public class ExclusiveBufferClaim
    {
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(IntPtr.Zero, 0);

        /// <summary>
        /// Wrap a region of an underlying log buffer so can can represent a claimed space for use by a publisher.
        /// </summary>
        /// <param name="buffer"> to be wrapped. </param>
        /// <param name="offset"> at which the claimed region begins including space for the header. </param>
        /// <param name="length"> length of the underlying claimed region including space for the header. </param>
        public void Wrap(UnsafeBuffer buffer, int offset, int length)
        {
            _buffer.Wrap(buffer, offset, length);
        }

        /// <summary>
        /// The referenced buffer to be used.
        /// </summary>
        /// <returns> the referenced buffer to be used.. </returns>
        public UnsafeBuffer Buffer()
        {
            return _buffer;
        }

        /// <summary>
        /// The offset in the buffer at which the claimed range begins.
        /// </summary>
        /// <returns> offset in the buffer at which the range begins. </returns>
        public int Offset()
        {
            return DataHeaderFlyweight.HEADER_LENGTH;
        }

        /// <summary>
        /// The length of the claimed range in the buffer.
        /// </summary>
        /// <returns> length of the range in the buffer. </returns>
        public int Length()
        {
            return _buffer.Capacity - DataHeaderFlyweight.HEADER_LENGTH;
        }

        /// <summary>
        /// Get the value of the flags field.
        /// </summary>
        /// <returns> the value of the header flags field. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public byte Flags()
        {
            return _buffer.GetByte(HeaderFlyweight.FLAGS_FIELD_OFFSET);
        }

        /// <summary>
        /// Set the value of the header flags field.
        /// </summary>
        /// <param name="flags"> value to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public ExclusiveBufferClaim Flags(byte flags)
        {
            _buffer.PutByte(HeaderFlyweight.FLAGS_FIELD_OFFSET, flags);

            return this;
        }

        /// <summary>
        /// Get the value of the header type field. The lower 16 bits are valid.
        /// </summary>
        /// <returns> the value of the header type field. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public int HeaderType()
        {
            return _buffer.GetShort(HeaderFlyweight.TYPE_FIELD_OFFSET) & 0xFFFF;
        }

        /// <summary>
        /// Set the value of the header type field. The lower 16 bits are valid.
        /// </summary>
        /// <param name="type"> value to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public ExclusiveBufferClaim HeaderType(int type)
        {
            _buffer.PutShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short)type);

            return this;
        }

        /// <summary>
        /// Get the value stored in the reserve space at the end of a data frame header.
        /// 
        /// Note: The value is in <seealso cref="ByteOrder#LITTLE_ENDIAN"/> format.
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
        /// Note: The value will be written in <seealso cref="ByteOrder#LITTLE_ENDIAN"/> format.
        /// </summary>
        /// <param name="value"> to be stored in the reserve space at the end of a data frame header. </param>
        /// <returns> this for fluent API semantics. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public ExclusiveBufferClaim ReservedValue(long value)
        {
            _buffer.PutLong(DataHeaderFlyweight.RESERVED_VALUE_OFFSET, value);
            return this;
        }

        /// <summary>
        /// Commit the message to the log buffer so that is it available to subscribers.
        /// </summary>
        public void Commit()
        {
            int frameLength = _buffer.Capacity;
            _buffer.PutIntOrdered(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        }

        /// <summary>
        /// Abort a claim of the message space to the log buffer so that the log can progress by ignoring this claim.
        /// </summary>
        public void Abort()
        {
            int frameLength = _buffer.Capacity;
            _buffer.PutShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short)HeaderFlyweight.HDR_TYPE_PAD);
            _buffer.PutIntOrdered(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        }
    }

}
