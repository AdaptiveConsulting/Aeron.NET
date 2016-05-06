using System;

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Atomic counter that is backed by an <seealso cref="AtomicBuffer"/> that can be read across threads and processes.
    /// </summary>
    public class AtomicCounter : IDisposable
    {
        private readonly int CounterId;
        private readonly int Offset;
        private readonly IAtomicBuffer Buffer;
        private readonly CountersManager CountersManager;

        internal AtomicCounter(IAtomicBuffer buffer, int counterId, CountersManager countersManager)
        {
            Buffer = buffer;
            CounterId = counterId;
            CountersManager = countersManager;
            Offset = CountersManager.CounterOffset(counterId);
            buffer.PutLong(Offset, 0);
        }

        /// <summary>
        /// Perform an atomic increment that will not lose updates across threads.
        /// </summary>
        /// <returns> the previous value of the counter </returns>
        public long Increment()
        {
            return Buffer.GetAndAddLong(Offset, 1);
        }

        /// <summary>
        /// Perform an atomic increment that is not safe across threads.
        /// </summary>
        /// <returns> the previous value of the counter </returns>
        public long OrderedIncrement()
        {
            return Buffer.AddLongOrdered(Offset, 1);
        }

        /// <summary>
        /// Set the counter with volatile semantics.
        /// </summary>
        /// <param name="value"> to be set with volatile semantics. </param>
        public void Set(long value)
        {
            Buffer.PutLongVolatile(Offset, value);
        }

        /// <summary>
        /// Set the counter with ordered semantics.
        /// </summary>
        /// <param name="value"> to be set with ordered semantics. </param>
        public long Ordered
        {
            set { Buffer.PutLongOrdered(Offset, value); }
        }

        /// <summary>
        /// Add an increment to the counter that will not lose updates across threads.
        /// </summary>
        /// <param name="increment"> to be added. </param>
        /// <returns> the previous value of the counter </returns>
        public long Add(long increment)
        {
            return Buffer.GetAndAddLong(Offset, increment);
        }

        /// <summary>
        /// Add an increment to the counter with ordered store semantics.
        /// </summary>
        /// <param name="increment"> to be added with ordered store semantics. </param>
        /// <returns> the previous value of the counter </returns>
        public long AddOrdered(long increment)
        {
            return Buffer.AddLongOrdered(Offset, increment);
        }

        /// <summary>
        /// Get the latest value for the counter.
        /// </summary>
        /// <returns> the latest value for the counter. </returns>
        public long Get()
        {
            return Buffer.GetLongVolatile(Offset);
        }

        /// <summary>
        /// Get the value of the counter using weak ordering semantics. This is the same a standard read of a field.
        /// </summary>
        /// <returns> the  value for the counter. </returns>
        public long Weak
        {
            get { return Buffer.GetLong(Offset); }
        }

        /// <summary>
        /// Free the counter slot for reuse.
        /// </summary>
        public void Dispose()
        {
            CountersManager.Free(CounterId);
        }
    }
}