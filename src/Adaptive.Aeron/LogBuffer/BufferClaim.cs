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
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
    /// 
    /// The claimed space is in <seealso cref="Buffer"/> between <seealso cref="Offset"/> and <seealso cref="Offset"/> + <seealso cref="Length"/>.
    /// When the buffer is filled with message data, use <seealso cref="Commit()"/> to make it available to subscribers.
    /// 
    /// If the claimed space is no longer required it can be aborted by calling <seealso cref="Abort()"/>.
    /// 
    /// <see cref="Publication.TryClaim"/>
    /// </summary>
    public class BufferClaim
    {
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(IntPtr.Zero, 0);

        /// <summary>
        /// Wrap a region of an underlying log buffer so it can represent a claimed space for use by a publisher.
        /// </summary>
        /// <param name="buffer"> to be wrapped. </param>
        /// <param name="offset"> at which the claimed region begins including space for the header. </param>
        /// <param name="length"> length of the underlying claimed region including space for the header. </param>
        public void Wrap(IAtomicBuffer buffer, int offset, int length)
        {
            _buffer.Wrap(buffer, offset, length);
        }

        /// <summary>
        /// The referenced buffer to be used.
        /// </summary>
        /// <returns> the referenced buffer to be used.. </returns>
        public IMutableDirectBuffer Buffer => _buffer;

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
        /// Get the value of the header type field. The lower 16 bits are valid.
        /// </summary>
        /// <returns> the value of the header type field. </returns>
        /// <seealso cref="DataHeaderFlyweight"></seealso>
        public int HeaderType()
        {
            return _buffer.GetShort(HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LittleEndian) & 0xFFFF;
        }

        /// <summary>
        /// Get the value of the flags field.
        /// </summary>
        /// <returns> the value of the header flags field. </returns>
        /// <seealso cref="DataHeaderFlyweight"></seealso>
        public byte Flags()
        {
            return _buffer.GetByte(HeaderFlyweight.FLAGS_FIELD_OFFSET);
        }

        /// <summary>
        /// Set the value of the header flags field.
        /// </summary>
        /// <param name="flags"> value to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="DataHeaderFlyweight"></seealso>
        public BufferClaim Flags(byte flags)
        {
            _buffer.PutByte(HeaderFlyweight.FLAGS_FIELD_OFFSET, flags);
            return this;
        }

        /// <summary>
        /// Set the value of the header type field. The lower 16 bits are valid.
        /// </summary>
        /// <param name="type"> value to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        /// <seealso cref="DataHeaderFlyweight"></seealso>
        public BufferClaim HeaderType(int type)
        {
            _buffer.PutShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short) type, ByteOrder.LittleEndian);
            return this;
        }


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
        /// Put bytes into the claimed buffer space for a message. To write multiple parts then use <seealso cref="Buffer()"/>
        /// and <seealso cref="Offset()"/>.
        /// </summary>
        /// <param name="srcBuffer"> to copy into the claimed space. </param>
        /// <param name="srcIndex">  in the source buffer from which to copy. </param>
        /// <param name="length">    of the source buffer to copy. </param>
        /// <returns> this for a fluent API. </returns>
        public BufferClaim PutBytes(IDirectBuffer srcBuffer, int srcIndex, int length)
        {
            _buffer.PutBytes(DataHeaderFlyweight.HEADER_LENGTH, srcBuffer, srcIndex, length);
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

            _buffer.PutShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short) HeaderFlyweight.HDR_TYPE_PAD, ByteOrder.LittleEndian);
            _buffer.PutIntOrdered(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        }
    }
}