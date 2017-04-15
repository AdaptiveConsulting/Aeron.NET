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

namespace Adaptive.Agrona
{
    /// <summary>
    /// Abstraction over a range of buffer types that allows fields to be written in native typed fashion.
    /// </summary>
    public interface IMutableDirectBuffer : IDirectBuffer
    {
        /// <summary>
        /// Is this buffer expandable to accommodate putting data into it beyond the current capacity?
        /// </summary>
        /// <returns> true is the underlying storage can expand otherwise false. </returns>
        bool IsExpandable { get; }

        /// <summary>
        /// Set a region of memory to a given byte value.
        /// </summary>
        /// <param name="index">  at which to start. </param>
        /// <param name="length"> of the run of bytes to set. </param>
        /// <param name="value">  the memory will be set to. </param>
        void SetMemory(int index, int length, byte value);

        ///// <summary>
        ///// Put a value to a given index.
        ///// </summary>
        ///// <param name="index">     in bytes for where to put. </param>
        ///// <param name="value">     for at a given index </param>
        ///// <param name="byteOrder"> of the value when written </param>
        //void PutLong(int index, long value, ByteOrder byteOrder);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutLong(int index, long value);

        ///// <summary>
        ///// Put a value to a given index.
        ///// </summary>
        ///// <param name="index">     in bytes for where to put. </param>
        ///// <param name="value">     to be written </param>
        ///// <param name="byteOrder"> of the value when written </param>
        //void PutInt(int index, int value, ByteOrder byteOrder);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> for at a given index </param>
        void PutInt(int index, int value);

        ///// <summary>
        ///// Put a value to a given index.
        ///// </summary>
        ///// <param name="index">     in bytes for where to put. </param>
        ///// <param name="value">     to be written </param>
        ///// <param name="byteOrder"> of the value when written. </param>
        //void PutDouble(int index, double value, ByteOrder byteOrder);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> to be written </param>
        void PutDouble(int index, double value);

        ///// <summary>
        ///// Put a value to a given index.
        ///// </summary>
        ///// <param name="index">     in bytes for where to put. </param>
        ///// <param name="value">     to be written </param>
        ///// <param name="byteOrder"> of the value when written. </param>
        //void PutFloat(int index, float value, ByteOrder byteOrder);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> to be written </param>
        void PutFloat(int index, float value);

        ///// <summary>
        ///// Put a value to a given index.
        ///// </summary>
        ///// <param name="index">     in bytes for where to put. </param>
        ///// <param name="value">     to be written </param>
        ///// <param name="byteOrder"> of the value when written. </param>
        //void PutShort(int index, short value, ByteOrder byteOrder);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> to be written </param>
        void PutShort(int index, short value);

        ///// <summary>
        ///// Put a value to a given index.
        ///// </summary>
        ///// <param name="index">     in bytes for where to put. </param>
        ///// <param name="value">     to be written </param>
        ///// <param name="byteOrder"> of the value when written. </param>
        //void PutChar(int index, char value, ByteOrder byteOrder);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> to be written </param>
        void PutChar(int index, char value);

        /// <summary>
        /// Put a value to a given index.
        /// </summary>
        /// <param name="index"> in bytes for where to put. </param>
        /// <param name="value"> to be written </param>
        void PutByte(int index, byte value);

        /// <summary>
        /// Put an array of src into the underlying buffer.
        /// </summary>
        /// <param name="index"> in the underlying buffer to start from. </param>
        /// <param name="src">   to be copied to the underlying buffer. </param>
        void PutBytes(int index, byte[] src);

        /// <summary>
        /// Put an array into the underlying buffer.
        /// </summary>
        /// <param name="index">  in the underlying buffer to start from. </param>
        /// <param name="src">    to be copied to the underlying buffer. </param>
        /// <param name="offset"> in the supplied buffer to begin the copy. </param>
        /// <param name="length"> of the supplied buffer to copy. </param>
        void PutBytes(int index, byte[] src, int offset, int length);

        /// <summary>
        /// Put bytes from a source <seealso cref="IDirectBuffer"/> into this <seealso cref="IMutableDirectBuffer"/> at given indices.
        /// </summary>
        /// <param name="index">     in this buffer to begin putting the bytes. </param>
        /// <param name="srcBuffer"> from which the bytes will be copied. </param>
        /// <param name="srcIndex">  in the source buffer from which the byte copy will begin. </param>
        /// <param name="length">    of the bytes to be copied. </param>
        void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length);

        /// <summary>
        /// Encode a String as UTF-8 bytes to the buffer with a length prefix.
        /// </summary>
        /// <param name="index"> at which the String should be encoded. </param>
        /// <param name="value">  of the String to be encoded. </param>
        /// <returns> the number of bytes put to the buffer. </returns>
        int PutStringUtf8(int index, string value);

        /// <summary>
        /// Encode a String as ASCII bytes to the buffer with a length prefix.
        /// </summary>
        /// <param name="index"> at which the String should be encoded. </param>
        /// <param name="value">  of the String to be encoded. </param>
        /// <returns> the number of bytes put to the buffer. </returns>
        int PutStringAscii(int index, string value);

        ///// <summary>
        ///// Encode a String as UTF-8 bytes to the buffer with a length prefix.
        ///// </summary>
        ///// <param name="index">     at which the String should be encoded. </param>
        ///// <param name="value">     of the String to be encoded. </param>
        ///// <param name="byteOrder"> for the length prefix. </param>
        ///// <returns> the number of bytes put to the buffer. </returns>
        //int PutStringUtf8(int index, string value, ByteOrder byteOrder);

        /// <summary>
        /// Encode a String as UTF-8 bytes the buffer with a length prefix with a maximum encoded size check.
        /// </summary>
        /// <param name="index">          at which the String should be encoded. </param>
        /// <param name="value">          of the String to be encoded. </param>
        /// <param name="maxEncodedSize"> to be checked before writing to the buffer. </param>
        /// <returns> the number of bytes put to the buffer. </returns>
        /// <exception cref="ArgumentException"> if the encoded bytes are greater than maxEncodedSize. </exception>
        int PutStringUtf8(int index, string value, int maxEncodedSize);

        ///// <summary>
        ///// Encode a String as UTF-8 bytes the buffer with a length prefix with a maximum encoded size check.
        ///// </summary>
        ///// <param name="index">          at which the String should be encoded. </param>
        ///// <param name="value">          of the String to be encoded. </param>
        ///// <param name="byteOrder">      for the length prefix. </param>
        ///// <param name="maxEncodedSize"> to be checked before writing to the buffer. </param>
        ///// <returns> the number of bytes put to the buffer. </returns>
        ///// <exception cref="ArgumentException"> if the encoded bytes are greater than maxEncodedSize. </exception>
        //int PutStringUtf8(int index, string value, ByteOrder byteOrder, int maxEncodedSize);

        /// <summary>
        /// Encode a String as UTF-8 bytes in the buffer without a length prefix.
        /// </summary>
        /// <param name="index"> at which the String begins. </param>
        /// <param name="value"> of the String to be encoded. </param>
        /// <returns> the number of bytes encoded. </returns>
        int PutStringWithoutLengthUtf8(int index, string value);
    }
}