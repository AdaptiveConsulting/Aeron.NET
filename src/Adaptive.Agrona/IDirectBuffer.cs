﻿/*
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
using System.Text;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Abstraction over a range of buffer types that allows fields to be read in native typed fashion.
    /// </summary>
    public interface IDirectBuffer : IComparable<IDirectBuffer>
    {
        /// <summary>
        /// Attach a view to a byte[] for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        void Wrap(byte[] buffer);

        /// <summary>
        /// Attach a view to a byte[] for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        /// <param name="offset"> at which the view begins. </param>
        /// <param name="length"> of the buffer included in the view </param>
        void Wrap(byte[] buffer, int offset, int length);

        /// <summary>
        /// Attach a view to an existing <seealso cref="IDirectBuffer"/>
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        void Wrap(IDirectBuffer buffer);

        /// <summary>
        /// Attach a view to a <seealso cref="IDirectBuffer"/> for providing direct access.
        /// </summary>
        /// <param name="buffer"> to which the view is attached. </param>
        /// <param name="offset"> at which the view begins. </param>
        /// <param name="length"> of the buffer included in the view </param>
        void Wrap(IDirectBuffer buffer, int offset, int length);

        /// <summary>
        /// Attach a view to an off-heap memory region by address.
        /// </summary>
        /// <param name="pointer"> where the memory begins off-heap </param>
        /// <param name="length">  of the buffer from the given address </param>
        void Wrap(IntPtr pointer, int length);

        /// <summary>
        /// Attach a view to an off-heap memory region by address.
        /// </summary>
        /// <param name="pointer"> where the memory begins off-heap </param>
        /// <param name="offset"> at which the view begins. </param>
        /// <param name="length">  of the buffer from the given address </param>
        void Wrap(IntPtr pointer, int offset, int length);

        /// <summary>
        /// A pointer to the underlying buffer.
        /// </summary>
        IntPtr BufferPointer { get; }

        /// <summary>
        /// Get the underlying byte[] if one exists
        /// </summary>
        byte[] ByteArray { get; }

        /// <summary>
        /// Get the underlying <see cref="ByteBuffer"/> if one exists
        /// </summary>
        ByteBuffer ByteBuffer { get; }
        
        /// <summary>
        /// Get the capacity of the underlying buffer.
        /// </summary>
        /// <returns> the capacity of the underlying buffer in bytes. </returns>
        int Capacity { get; }

        /// <summary>
        /// Check that a given limit is not greater than the capacity of a buffer from a given offset.
        /// <para>
        /// Can be overridden in a DirectBuffer subclass to enable an extensible buffer or handle retry after a flush.
        /// 
        /// </para>
        /// </summary>
        /// <param name="limit"> up to which access is required. </param>
        /// <exception cref="IndexOutOfRangeException"> if limit is beyond buffer capacity. </exception>
        void CheckLimit(int limit);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index">     in bytes from which to get. </param>
        /// <param name="byteOrder"> of the value to be read. </param>
        /// <returns> the value for at a given index </returns>
        long GetLong(int index, ByteOrder byteOrder);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value for at a given index </returns>
        long GetLong(int index);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index">     in bytes from which to get. </param>
        /// <param name="byteOrder"> of the value to be read. </param>
        /// <returns> the value at a given index. </returns>
        int GetInt(int index, ByteOrder byteOrder);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value for at a given index </returns>
        int GetInt(int index);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value at a given index. </returns>
        double GetDouble(int index);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value at a given index. </returns>
        float GetFloat(int index);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index">     in bytes from which to get. </param>
        /// <param name="byteOrder"> of the value to be read. </param>
        /// <returns> the value at a given index. </returns>
        short GetShort(int index, ByteOrder byteOrder);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value at a given index. </returns>
        short GetShort(int index);
        
        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value at a given index. </returns>
        char GetChar(int index);

        /// <summary>
        /// Get the value at a given index.
        /// </summary>
        /// <param name="index"> in bytes from which to get. </param>
        /// <returns> the value at a given index. </returns>
        byte GetByte(int index);

        /// <summary>
        /// Get from the underlying buffer into a supplied byte array.
        /// This method will try to fill the supplied byte array.
        /// </summary>
        /// <param name="index"> in the underlying buffer to start from. </param>
        /// <param name="dst">   into which the dst will be copied. </param>
        void GetBytes(int index, byte[] dst);

        /// <summary>
        /// Get bytes from the underlying buffer into a supplied byte array.
        /// </summary>
        /// <param name="index">  in the underlying buffer to start from. </param>
        /// <param name="dst">    into which the bytes will be copied. </param>
        /// <param name="offset"> in the supplied buffer to start the copy </param>
        /// <param name="length"> of the supplied buffer to use. </param>
        void GetBytes(int index, byte[] dst, int offset, int length);

        /// <summary>
        /// Get bytes from this <seealso cref="IDirectBuffer"/> into the provided <seealso cref="IMutableDirectBuffer"/> at given indices. </summary>
        /// <param name="index">     in this buffer to begin getting the bytes. </param>
        /// <param name="dstBuffer"> to which the bytes will be copied. </param>
        /// <param name="dstIndex">  in the channel buffer to which the byte copy will begin. </param>
        /// <param name="length">    of the bytes to be copied. </param>
        void GetBytes(int index, IMutableDirectBuffer dstBuffer, int dstIndex, int length);

        /// <summary>
        /// Get a String from bytes encoded in UTF-8 format that is length prefixed.
        /// </summary>
        /// <param name="index">    at which the String begins. </param>
        /// <returns> the String as represented by the UTF-8 encoded bytes. </returns>
        string GetStringUtf8(int index);

        /// <summary>
        /// Get a String from bytes encoded in ASCII format that is length prefixed.
        /// </summary>
        /// <param name="index">    at which the String begins. </param>
        /// <returns> the String as represented by the ASCII encoded bytes. </returns>
        string GetStringAscii(int index);

        /// <summary>
        /// Get a String from bytes encoded in ASCII format that is length prefixed and append to an <seealso cref="StringBuilder"/>.
        /// </summary>
        /// <param name="index">      at which the String begins. </param>
        /// <param name="appendable"> to append the chars to. </param>
        /// <returns> the number of bytes copied. </returns>
        int GetStringAscii(int index, StringBuilder appendable);
        
        /// <summary>
        /// Get part of a String from bytes encoded in ASCII format that is length prefixed and append to an
        /// <seealso cref="StringBuilder"/>.
        /// </summary>
        /// <param name="index">      at which the String begins. </param>
        /// <param name="length">     of the String in bytes to decode. </param>
        /// <param name="appendable"> to append the chars to. </param>
        /// <returns> the number of bytes copied. </returns>
        int GetStringAscii(int index, int length, StringBuilder appendable);

        /// <summary>
        /// Get part of String from bytes encoded in UTF-8 format that is length prefixed.
        /// </summary>
        /// <param name="index"> at which the String begins. </param>
        /// <param name="length"> of the String in bytes to decode. </param>
        /// <returns> the String as represented by the UTF-8 encoded bytes. </returns>
        string GetStringUtf8(int index, int length);

        /// <summary>
        /// Get part of String from bytes encoded in ASCII format that is length prefixed.
        /// </summary>
        /// <param name="index"> at which the String begins. </param>
        /// <param name="length"> of the String in bytes to decode. </param>
        /// <returns> the String as represented by the ASCII encoded bytes. </returns>
        string GetStringAscii(int index, int length);

        /// <summary>
        /// Get an encoded UTF-8 String from the buffer that does not have a length prefix.
        /// </summary>
        /// <param name="index"> at which the String begins. </param>
        /// <param name="length"> of the String in bytes to decode. </param>
        /// <returns> the String as represented by the UTF-8 encoded bytes. </returns>
        string GetStringWithoutLengthUtf8(int index, int length);
        
        /// <summary>
        /// Get an encoded ASCII String from the buffer that does not have a length prefix.
        /// </summary>
        /// <param name="index">  at which the String begins. </param>
        /// <param name="length"> of the String in bytes to decode. </param>
        /// <returns> the String as represented by the Ascii encoded bytes. </returns>
        string GetStringWithoutLengthAscii(int index, int length);
        
        /// <summary>
        /// Check that a given length of bytes is within the bounds from a given index.
        /// </summary>
        /// <param name="index">  from which to check. </param>
        /// <param name="length"> in bytes of the range to check. </param>
        /// <exception cref="IndexOutOfRangeException"> if the length goes outside of the capacity range. </exception>
        void BoundsCheck(int index, int length);
    }
}