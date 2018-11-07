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

using System.Runtime.CompilerServices;

namespace Adaptive.Agrona.Util
{
    public static class IntUtil
    {
        /// <summary>
        /// Returns the number of zero bits following the lowest-order ("rightmost")
        /// one-bit in the two's complement binary representation of the specified
        /// {@code int} value.  Returns 32 if the specified value has no
        /// one-bits in its two's complement representation, in other words if it is
        /// equal to zero.
        /// </summary>
        /// <param name="i"> the value whose number of trailing zeros is to be computed </param>
        /// <returns> the number of zero bits following the lowest-order ("rightmost")
        ///     one-bit in the two's complement binary representation of the
        ///     specified {@code int} value, or 32 if the value is equal
        ///     to zero.
        /// @since 1.5 </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NumberOfTrailingZeros(int i)
        {
            // HD, Figure 5-14
            int y;
            if (i == 0)
            {
                return 32;
            }
            int n = 31;
            y = i << 16;
            if (y != 0)
            {
                n = n - 16;
                i = y;
            }
            y = i << 8;
            if (y != 0)
            {
                n = n - 8;
                i = y;
            }
            y = i << 4;
            if (y != 0)
            {
                n = n - 4;
                i = y;
            }
            y = i << 2;
            if (y != 0)
            {
                n = n - 2;
                i = y;
            }
            return n - ((int)((uint)(i << 1) >> 31));
        }


        /// <summary>
        /// Note Olivier: Direct port of the Java method Integer.NumberOfLeadingZeros
        /// 
        /// Returns the number of zero bits preceding the highest-order
        /// ("leftmost") one-bit in the two's complement binary representation
        /// of the specified {@code int} value.  Returns 32 if the
        /// specified value has no one-bits in its two's complement representation,
        /// in other words if it is equal to zero.
        /// 
        /// <para>Note that this method is closely related to the logarithm base 2.
        /// For all positive {@code int} values x:
        /// <ul>
        /// <li>floor(log<sub>2</sub>(x)) = {@code 31 - numberOfLeadingZeros(x)}</li>
        /// <li>ceil(log<sub>2</sub>(x)) = {@code 32 - numberOfLeadingZeros(x - 1)}</li>
        /// </ul>
        /// 
        /// </para>
        /// </summary>
        /// <param name="i"> the value whose number of leading zeros is to be computed </param>
        /// <returns> the number of zero bits preceding the highest-order
        ///     ("leftmost") one-bit in the two's complement binary representation
        ///     of the specified {@code int} value, or 32 if the value
        ///     is equal to zero.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NumberOfLeadingZeros(int i)
        {
            // HD, Figure 5-6
            if (i == 0)
            {
                return 32;
            }
            int n = 1;
            if ((int)((uint)i >> 16) == 0)
            {
                n += 16;
                i <<= 16;
            }
            if ((int)((uint)i >> 24) == 0)
            {
                n += 8;
                i <<= 8;
            }
            if ((int)((uint)i >> 28) == 0)
            {
                n += 4;
                i <<= 4;
            }
            if ((int)((uint)i >> 30) == 0)
            {
                n += 2;
                i <<= 2;
            }
            n -= (int)((uint)i >> 31);
            return n;
        }
    }
}