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
    public class AtomicLong
    {
        private long _long;

        /// <summary>
        /// Gets the current value.
        /// </summary>
        /// <returns> the current value.</returns>
        public long Get()
        {
            return Interlocked.Read(ref _long);
        }

        /// <summary>
        /// Eventually sets to the given value.
        /// </summary>
        /// <param name="newValue"> the new value.</param>
        public void LazySet(long newValue)
        {
            Interlocked.Exchange(ref _long, newValue);
        }

        public void Set(long value)
        {
            Interlocked.Exchange(ref _long, value);
        }

        public void Add(long add)
        {
            Interlocked.Add(ref _long, add);
        }
        
        /// <summary>
        /// Atomically increments the current value
        /// </summary>
        /// <returns> the udpated value.</returns>
        public long IncrementAndGet()
        {
            return Interlocked.Increment(ref _long);
        }
    }
}