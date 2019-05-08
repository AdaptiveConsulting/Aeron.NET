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

using Adaptive.Agrona.Util;
using System;
using System.Text;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Common functions for buffer implementations.
    /// </summary>
    public class BufferUtil
    {
        public static readonly byte[] NullBytes = Encoding.UTF8.GetBytes("null");

        /// <summary>
        /// Bounds check the access range and throw a <seealso cref="IndexOutOfRangeException"/> if exceeded.
        /// </summary>
        /// <param name="buffer"> to be checked. </param>
        /// <param name="index">  at which the access will begin. </param>
        /// <param name="length"> of the range accessed. </param>
        public static void BoundsCheck(byte[] buffer, long index, int length)
        {
            var capacity = buffer.Length;
            var resultingPosition = index + length;
            if (index < 0 || resultingPosition > capacity)
            {
                ThrowHelper.ThrowIndexOutOfRangeException($"index={index:D}, length={length:D}, capacity={capacity:D}");
            }
        }
        
        public static ByteBuffer AllocateDirectAligned(int capacity, int alignment)
        {
            return new ByteBuffer(capacity, alignment);
        }

        public static ByteBuffer AllocateDirect(int capacity)
        {
            return new ByteBuffer(capacity, BitUtil.SIZE_OF_LONG);
        }
    }
}