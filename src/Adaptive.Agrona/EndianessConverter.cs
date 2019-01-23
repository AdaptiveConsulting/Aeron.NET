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
using System.Net;
using System.Runtime.CompilerServices;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Utility class to manipulate endianess 
    /// </summary>
    public static class EndianessConverter
    {
        private static readonly ByteOrder NativeByteOrder = BitConverter.IsLittleEndian
            ? ByteOrder.LittleEndian
            : ByteOrder.BigEndian;

        /// <summary>
        /// Applies the specified endianess to an int16 (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short ApplyInt16(ByteOrder byteOrder, short value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return (short)((value & 0xFFU) << 8 | (value & 0xFF00U) >> 8);
        }

        /// <summary>
        /// Applies the specified endianess to an uint16 (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort ApplyUint16(ByteOrder byteOrder, ushort value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return (ushort)((value & 0xFFU) << 8 | (value & 0xFF00U) >> 8);
        }

        /// <summary>
        /// Applies the specified endianess to an int32 (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ApplyInt32(ByteOrder byteOrder, int value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return (int)((value & 0x000000FFU) << 24 | (value & 0x0000FF00U) << 8 |
                   (value & 0x00FF0000U) >> 8 | (value & 0xFF000000U) >> 24);
        }

        /// <summary>
        /// Applies the specified endianess to an uint32 (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ApplyUint32(ByteOrder byteOrder, uint value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return (value & 0x000000FFU) << 24 | (value & 0x0000FF00U) << 8 |
                   (value & 0x00FF0000U) >> 8 | (value & 0xFF000000U) >> 24;
        }

        /// <summary>
        /// Applies the specified endianess to an uint64 (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ApplyUint64(ByteOrder byteOrder, ulong value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return (value & 0x00000000000000FFUL) << 56 | (value & 0x000000000000FF00UL) << 40 |
                    (value & 0x0000000000FF0000UL) << 24 | (value & 0x00000000FF000000UL) << 8 |
                    (value & 0x000000FF00000000UL) >> 8 | (value & 0x0000FF0000000000UL) >> 24 |
                    (value & 0x00FF000000000000UL) >> 40 | (value & 0xFF00000000000000UL) >> 56;
        }

        /// <summary>
        /// Applies the specified endianess to an int64 (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ApplyInt64(ByteOrder byteOrder, long value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return IPAddress.HostToNetworkOrder(value);
        }

        /// <summary>
        /// Applies the specified endianess to a double (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double ApplyDouble(ByteOrder byteOrder, double value)
        {
            if (byteOrder == NativeByteOrder) return value;

            return BitConverter.Int64BitsToDouble(IPAddress.HostToNetworkOrder(BitConverter.DoubleToInt64Bits(value)));
        }

        /// <summary>
        /// Applies the specified endianess to an float (reverse bytes if input endianess is different from system's endianess)
        /// </summary>
        /// <param name="byteOrder">the endianess to apply</param>
        /// <param name="value">the value to be converted</param>
        /// <returns>The value with applied endianess</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static float ApplyFloat(ByteOrder byteOrder, float value)
        {
            if (byteOrder == NativeByteOrder) return value;

            int valueInt = *(int*) &value;
            int applied = ApplyInt32(byteOrder, valueInt);

            return *(float*) &applied;
        }
    }
}