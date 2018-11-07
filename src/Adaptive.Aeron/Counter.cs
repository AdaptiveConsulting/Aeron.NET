using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Counter stored in the counters file managed by the media driver which can be read with AeronStat.
    /// </summary>
    public class Counter : AtomicCounter
    {
        private readonly ClientConductor _clientConductor;
        private volatile bool _isClosed;

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
        internal Counter(CountersReader countersReader, long registrationId, int counterId)
            : base(countersReader.ValuesBuffer, counterId)
        {
            if (countersReader.GetCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
            {
                throw new AeronException("Counter id has not been allocated: " + counterId);
            }

            RegistrationId = registrationId;
            _clientConductor = null;
        }

        /// <summary>
        /// Return the registration id used to register this counter with the media driver.
        /// </summary>
        /// <value> registration id </value>
        public long RegistrationId { get; }

        /// <summary>
        /// Close the counter, releasing the resource managed by the media driver if this was the creator of the Counter.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
        public override void Dispose()
        {
            if (!_isClosed)
            {
                if (null != _clientConductor)
                {
                    _clientConductor.ReleaseCounter(this);
                }
                else
                {
                    _isClosed = true;
                }
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
            _isClosed = true;
        }
    }
}