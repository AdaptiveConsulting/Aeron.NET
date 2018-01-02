using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;

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
    public class ExclusiveBufferClaim : BufferClaim
    {
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
            _buffer.PutLong(DataHeaderFlyweight.RESERVED_VALUE_OFFSET, value, ByteOrder.LittleEndian);
            return this;
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
            return _buffer.GetShort(HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LittleEndian) & 0xFFFF;
        }

        /// <summary>
        /// Set the value of the header type field. The lower 16 bits are valid.
        /// </summary>
        /// <param name="type"> value to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="DataHeaderFlyweight"/>
        public ExclusiveBufferClaim HeaderType(int type)
        {
            _buffer.PutShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short)type, ByteOrder.LittleEndian);

            return this;
        }
   }
}
