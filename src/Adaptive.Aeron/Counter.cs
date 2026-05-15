/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Counter stored in a file managed by the media driver which can be observed with AeronStat.
    /// </summary>
    public sealed class Counter : AtomicCounter
    {
        private readonly ClientConductor _clientConductor;
        private readonly bool _clientOwned;
        private AtomicBoolean _isClosed = new AtomicBoolean(false);

        internal Counter(
            long correlationId,
            long registrationId,
            bool clientOwned,
            ClientConductor clientConductor,
            IAtomicBuffer buffer,
            int counterId)
            : base(buffer, counterId)
        {
            CorrelationId = correlationId;
            RegistrationId = registrationId;
            _clientOwned = clientOwned;
            _clientConductor = clientConductor;
        }

        /// <summary>
        /// Construct a read-write view of an existing counter.
        /// </summary>
        /// <param name="countersReader"> for getting access to the buffers. </param>
        /// <param name="counterId">      for the counter to be viewed. </param>
        /// <exception cref="AeronException"> if the id has for the counter has not been allocated. </exception>
        /// <remarks>Since 1.51.0 — the <c>registrationId</c> parameter was removed; it is now read from the
        /// <see cref="CountersReader"/>.</remarks>
        public Counter(CountersReader countersReader, int counterId)
            : base(countersReader.ValuesBuffer, counterId)
        {
            if (countersReader.GetCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
            {
                throw new AeronException("Counter id is not allocated: " + counterId);
            }

            CorrelationId = Aeron.NULL_VALUE;
            RegistrationId = countersReader.GetCounterRegistrationId(counterId);
            _clientOwned = true;
            _clientConductor = null;
        }

        /// <summary>
        /// The correlation id of the counter creation command sent to the media driver, or
        /// <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if unknown.
        /// </summary>
        /// <remarks>Since 1.51.0</remarks>
        public long CorrelationId { get; }

        /// <summary>
        /// Return the registration id used to register this counter with the media driver. Can also be retrieved by
        /// calling <see cref="CountersReader.GetCounterRegistrationId(int)"/>.
        /// <para>
        /// For non-static counters this is the same as <see cref="CorrelationId"/>. For static counters this will be a
        /// user-defined value specified at creation time.
        /// </para>
        /// </summary>
        /// <value> the registration id used to register this counter with the media driver. </value>
        public long RegistrationId { get; }

        /// <summary>
        /// Close the counter, releasing the resource managed by the media driver if this was the creator of the
        /// Counter.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
        public override void Dispose()
        {
            if (_isClosed.CompareAndSet(false, true))
            {
                base.Dispose();

                _clientConductor?.RemoveCounter(this);
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public override bool IsClosed => _isClosed;

        internal void InternalClose()
        {
            base.Dispose();
            _isClosed.Set(true);
        }

        internal bool ClientOwned()
        {
            return _clientOwned;
        }
    }
}
