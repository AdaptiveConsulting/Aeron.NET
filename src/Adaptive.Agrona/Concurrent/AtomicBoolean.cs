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
using System.Runtime.CompilerServices;
using System.Threading;
using Adaptive.Agrona.Util;

namespace Adaptive.Agrona.Concurrent
{
    public class AtomicBoolean
    {
        private int _value;

        private const int TRUE = 1;
        private const int FALSE = 0;

        public AtomicBoolean(bool initialValue)
        {
            Interlocked.Exchange(ref _value, initialValue ? TRUE : FALSE);
        }

        /// <summary>
        /// Atomically set the value to the given updated value if the current value equals the comparand
        /// </summary>
        /// <param name="newValue">The new value</param>
        /// <param name="comparand">The comparand (expected value)</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompareAndSet(bool comparand, bool newValue)
        {
            var newValueInt = ToInt(newValue);
            var comparandInt = ToInt(comparand);

            return Interlocked.CompareExchange(ref _value, newValueInt, comparandInt) == comparandInt;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get()
        {
            return ToBool(Volatile.Read(ref _value));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator bool(AtomicBoolean value)
        {
            return value.Get();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ToBool(int value)
        {
            if (value != FALSE && value != TRUE)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(nameof(value));
            }

            return value == TRUE;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ToInt(bool value)
        {
            return value ? TRUE : FALSE;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(bool value)
        {
            Volatile.Write(ref _value, ToInt(value));
        }
    }
}