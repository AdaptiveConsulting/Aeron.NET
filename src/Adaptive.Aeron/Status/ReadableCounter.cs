using System;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Readonly View of an associated <seealso cref="Counter"/>.
    /// <para>
    /// <b>Note:</b>The user should call <seealso cref="IsClosed()"/> and ensure the result is false to avoid a race on reading a
    /// closed counter.
    /// 
    /// </para>
    /// </summary>
    public class ReadableCounter : IDisposable
    {
        private readonly int addressOffset;
        private readonly long registrationId;
        private readonly int counterId;
        private volatile bool isClosed = false;
        private readonly byte[] buffer;
        private readonly CountersReader countersReader;
        private readonly IAtomicBuffer valuesBuffer;

        /// <summary>
        /// Construct a view of an existing counter.
        /// </summary>
        /// <param name="countersReader"> for getting access to the buffers. </param>
        /// <param name="registrationId"> assigned by the driver for the counter or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not known. </param>
        /// <param name="counterId">      for the counter to be viewed. </param>
        /// <exception cref="InvalidOperationException"> if the id has for the counter has not been allocated. </exception>
        public ReadableCounter(CountersReader countersReader, long registrationId, int counterId)
        {
            if (countersReader.GetCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
            {
                throw new InvalidOperationException("Counter id has not been allocated: " + counterId);
            }

            this.countersReader = countersReader;
            this.counterId = counterId;
            this.registrationId = registrationId;

            valuesBuffer = countersReader.ValuesBuffer;
            var counterOffset = CountersReader.CounterOffset(counterId);
            valuesBuffer.BoundsCheck(counterOffset, BitUtil.SIZE_OF_LONG);

            buffer = valuesBuffer.ByteArray;
            addressOffset = counterOffset;
        }

        /// <summary>
        /// Construct a view of an existing counter.
        /// </summary>
        /// <param name="countersReader"> for getting access to the buffers. </param>
        /// <param name="counterId">      for the counter to be viewed. </param>
        /// <exception cref="InvalidOperationException"> if the id has for the counter has not been allocated. </exception>
        public ReadableCounter(CountersReader countersReader, int counterId) : this(countersReader, Aeron.NULL_VALUE, counterId)
        {
        }

        /// <summary>
        /// Return the registration Id for the counter.
        /// </summary>
        /// <returns> registration Id. </returns>
        public long RegistrationId()
        {
            return registrationId;
        }

        /// <summary>
        /// Return the counter Id.
        /// </summary>
        /// <returns> counter Id. </returns>
        public int CounterId()
        {
            return counterId;
        }

        /// <summary>
        /// Return the state of the counter.
        /// </summary>
        /// <seealso cref="CountersReader.RECORD_ALLOCATED"/>
        /// <seealso cref="CountersReader.RECORD_RECLAIMED"/>
        /// <seealso cref="CountersReader.RECORD_UNUSED"/>
        /// <returns> state for the counter. </returns>
        public int State()
        {
            return countersReader.GetCounterState(counterId);
        }

        /// <summary>
        /// Return the counter label.
        /// </summary>
        /// <returns> the counter label. </returns>
        public string Label()
        {
            return countersReader.GetCounterLabel(counterId);
        }

        /// <summary>
        /// Get the latest value for the counter with volatile semantics.
        /// <para>
        /// <b>Note:</b>The user should call <seealso cref="IsClosed()"/> and ensure the result is false to avoid a race on reading
        /// a closed counter.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the latest value for the counter. </returns>
        public long Get()
        {
            // return UnsafeAccess.UNSAFE.getLongVolatile(buffer, addressOffset);
            
            return valuesBuffer.GetLongVolatile(addressOffset);
        }

        /// <summary>
        /// Get the value of the counter using weak ordering semantics. This is the same a standard read of a field.
        /// </summary>
        /// <returns> the  value for the counter. </returns>
        public long GetWeak()
        {
            // UnsafeAccess.UNSAFE.getLong(buffer, addressOffset);
            
            return valuesBuffer.GetLong(addressOffset);
        }

        /// <summary>
        /// Close this counter. This has no impact on the <seealso cref="Counter"/> it is viewing.
        /// </summary>
        public void Dispose()
        {
            isClosed = true;
        }

        /// <summary>
        /// Has this counters been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool IsClosed()
        {
            return isClosed;
        }
    }
}