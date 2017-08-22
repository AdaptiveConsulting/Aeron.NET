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

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Atomic counter that is backed by an <seealso cref="IAtomicBuffer"/> that can be read across threads and processes.
    /// </summary>
    public class AtomicCounter : IDisposable
    {
        public int Id { get; }
        private readonly int _offset;
        private readonly IAtomicBuffer _buffer;
        private readonly CountersManager _countersManager;

        internal AtomicCounter(IAtomicBuffer buffer, int counterId, CountersManager countersManager)
        {
            _buffer = buffer;
            Id = counterId;
            _countersManager = countersManager;
            _offset = CountersReader.CounterOffset(counterId);
            buffer.PutLong(_offset, 0);
        }

        /// <summary>
        /// Perform an atomic increment that will not lose updates across threads.
        /// </summary>
        /// <returns> the previous value of the counter </returns>
        public long Increment()
        {
            return _buffer.GetAndAddLong(_offset, 1);
        }

        /// <summary>
        /// Perform an atomic increment that is not safe across threads.
        /// </summary>
        /// <returns> the previous value of the counter </returns>
        public long OrderedIncrement()
        {
            return _buffer.AddLongOrdered(_offset, 1);
        }

        /// <summary>
        /// Set the counter with volatile semantics.
        /// </summary>
        /// <param name="value"> to be set with volatile semantics. </param>
        public void Set(long value)
        {
            _buffer.PutLongVolatile(_offset, value);
        }

        /// <summary>
        /// Set the counter with ordered semantics.
        /// </summary>
        /// <param name="value"> to be set with ordered semantics. </param>
        public long Ordered
        {
            set { _buffer.PutLongOrdered(_offset, value); }
        }

        /// <summary>
        /// Add an increment to the counter that will not lose updates across threads.
        /// </summary>
        /// <param name="increment"> to be added. </param>
        /// <returns> the previous value of the counter </returns>
        public long Add(long increment)
        {
            return _buffer.GetAndAddLong(_offset, increment);
        }

        /// <summary>
        /// Add an increment to the counter with ordered store semantics.
        /// </summary>
        /// <param name="increment"> to be added with ordered store semantics. </param>
        /// <returns> the previous value of the counter </returns>
        public long AddOrdered(long increment)
        {
            return _buffer.AddLongOrdered(_offset, increment);
        }

        /// <summary>
        /// Get the latest value for the counter.
        /// </summary>
        /// <returns> the latest value for the counter. </returns>
        public long Get()
        {
            return _buffer.GetLongVolatile(_offset);
        }

        /// <summary>
        /// Get the value of the counter using weak ordering semantics. This is the same a standard read of a field.
        /// </summary>
        /// <returns> the  value for the counter. </returns>
        public long Weak => _buffer.GetLong(_offset);

        /// <summary>
        /// Free the counter slot for reuse.
        /// </summary>
        public void Dispose()
        {
            _countersManager.Free(Id);
        }
    }
}