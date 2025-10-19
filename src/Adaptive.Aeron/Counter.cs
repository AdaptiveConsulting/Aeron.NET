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
        private AtomicBoolean _isClosed = new AtomicBoolean(false);

        internal Counter(long registrationId, ClientConductor clientConductor, IAtomicBuffer buffer, int counterId) :
            base(buffer, counterId)
        {
            RegistrationId = registrationId;
            _clientConductor = clientConductor;
        }

        /// <summary>
        /// Construct a read-write view of an existing counter.
        /// </summary>
        /// <param name="countersReader"> for getting access to the buffers. </param>
        /// <param name="registrationId"> assigned by the driver for the counter or <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not known. </param>
        /// <param name="counterId">      for the counter to be viewed. </param>
        /// <exception cref="AeronException"> if the id has for the counter has not been allocated. </exception>
        public Counter(CountersReader countersReader, long registrationId, int counterId)
            : base(countersReader.ValuesBuffer, counterId)
        {
            if (countersReader.GetCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
            {
                throw new AeronException("Counter id is not allocated: " + counterId);
            }

            RegistrationId = registrationId;
            _clientConductor = null;
        }

        /// <summary>
        /// Return the registration id used to register this counter with the media driver.
        /// </summary>
        /// <value> the registration id used to register this counter with the media driver. </value>
        public long RegistrationId { get; }

        /// <summary>
        /// Close the counter, releasing the resource managed by the media driver if this was the creator of the Counter.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
        public override void Dispose()
        {
            if (_isClosed.CompareAndSet(false, true))
            {
                base.Dispose();

                _clientConductor?.ReleaseCounter(this);
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
        
        internal ClientConductor ClientConductor()
        {
            return _clientConductor;
        }
    }
}