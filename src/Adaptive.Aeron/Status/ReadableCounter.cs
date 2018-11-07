using System;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Readonly View of an associated <seealso cref="Counter"/>.
    /// <para>
    /// <b>Note:</b>The user should call <seealso cref="IsClosed"/> and ensure the result is false to avoid a race on reading a
    /// closed counter.
    /// 
    /// </para>
    /// </summary>
    public class ReadableCounter : IDisposable
    {
        private readonly int _addressOffset;
        private readonly long _registrationId;
        private readonly int _counterId;
        private volatile bool _isClosed = false;
        private readonly byte[] _buffer;
        private readonly CountersReader _countersReader;
        private readonly IAtomicBuffer _valuesBuffer;

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

            _countersReader = countersReader;
            _counterId = counterId;
            _registrationId = registrationId;

            _valuesBuffer = countersReader.ValuesBuffer;
            var counterOffset = CountersReader.CounterOffset(counterId);
            _valuesBuffer.BoundsCheck(counterOffset, BitUtil.SIZE_OF_LONG);

            _buffer = _valuesBuffer.ByteArray;
            _addressOffset = counterOffset;
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
        public long RegistrationId => _registrationId;

        /// <summary>
        /// Return the counter Id.
        /// </summary>
        /// <returns> counter Id. </returns>
        public int CounterId => _counterId;

        /// <summary>
        /// Return the state of the counter.
        /// </summary>
        /// <seealso cref="CountersReader.RECORD_ALLOCATED"/>
        /// <seealso cref="CountersReader.RECORD_RECLAIMED"/>
        /// <seealso cref="CountersReader.RECORD_UNUSED"/>
        /// <returns> state for the counter. </returns>
        public int State => _countersReader.GetCounterState(_counterId);

        /// <summary>
        /// Return the counter label.
        /// </summary>
        /// <returns> the counter label. </returns>
        public string Label => _countersReader.GetCounterLabel(_counterId);

        /// <summary>
        /// Get the latest value for the counter with volatile semantics.
        /// <para>
        /// <b>Note:</b>The user should call <seealso cref="IsClosed"/> and ensure the result is false to avoid a race on reading
        /// a closed counter.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the latest value for the counter. </returns>
        public long Get()
        {
            // return UnsafeAccess.UNSAFE.getLongVolatile(buffer, addressOffset);
            
            return _valuesBuffer.GetLongVolatile(_addressOffset);
        }

        /// <summary>
        /// Get the value of the counter using weak ordering semantics. This is the same a standard read of a field.
        /// </summary>
        /// <returns> the  value for the counter. </returns>
        public long GetWeak()
        {
            // UnsafeAccess.UNSAFE.getLong(buffer, addressOffset);
            
            return _valuesBuffer.GetLong(_addressOffset);
        }

        /// <summary>
        /// Close this counter. This has no impact on the <seealso cref="Counter"/> it is viewing.
        /// </summary>
        public void Dispose()
        {
            _isClosed = true;
        }

        /// <summary>
        /// Has this counters been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool IsClosed => _isClosed;
    }
}