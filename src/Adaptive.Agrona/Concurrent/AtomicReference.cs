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

using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class AtomicReference<T> where T : class
    {
        private T _value;

        /// <summary>
        /// Gets the current value.
        /// </summary>
        /// <returns> the current value.</returns>
        public T Get()
        {
            return Volatile.Read(ref _value);
        }

        /// <summary>
        /// Eventually sets to the given value.
        /// </summary>
        /// <param name="newValue"> the new value.</param>
        public void LazySet(T newValue)
        {
            Volatile.Write(ref _value, newValue);
        }

        public T GetAndSet(T value)
        {
            return Interlocked.Exchange(ref _value, value);
        }

        public bool CompareAndSet(T compareValue, T newValue)
        {
            var original = Interlocked.CompareExchange(ref _value, newValue, compareValue);

            return original == compareValue;
        }
    }
}