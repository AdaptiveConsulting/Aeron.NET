namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Reports on how far through a buffer some component has progressed.
    /// 
    /// Threadsafe to write to from a single writer.
    /// </summary>
    public interface IPosition : IReadablePosition
    {
        /// <summary>
        /// Get the current position of a component without memory ordering semantics.
        /// </summary>
        /// <returns> the current position of a component </returns>
        long Get();

        /// <summary>
        /// Sets the current position of the component without memory ordering semantics.
        /// </summary>
        /// <param name="value"> the current position of the component. </param>
        void Set(long value);

        /// <summary>
        /// Sets the current position of the component with ordered atomic memory semantics.
        /// </summary>
        /// <param name="value"> the current position of the component. </param>
        void SetOrdered(long value);

        /// <summary>
        /// Set the position to the new proposedValue if it is greater than the current proposedValue memory ordering semantics.
        /// </summary>
        /// <param name="proposedValue"> for the new max. </param>
        /// <returns> true if a new max as been set otherwise false. </returns>
        bool ProposeMax(long proposedValue);

        /// <summary>
        /// Set the position to the new proposedValue if it is greater than the current proposedValue with ordered atomic
        /// memory semantics.
        /// </summary>
        /// <param name="proposedValue"> for the new max. </param>
        /// <returns> true if a new max as been set otherwise false. </returns>
        bool ProposeMaxOrdered(long proposedValue);
    }

    public class AtomicLongPosition : IPosition
    {
        private AtomicLong value = new AtomicLong();

        public void Dispose()
        {
        }

        public int Id()
        {
            return 0;
        }

        public long Volatile
        {
            get { return value.Get(); }
        }

        public long Get()
        {
            return value.Get();
        }

        public void Set(long value)
        {
            this.value.Set(value);
        }

        public void SetOrdered(long value)
        {
            this.value.Set(value);
        }

        public bool ProposeMax(long proposedValue)
        {
            return ProposeMaxOrdered(proposedValue);
        }

        public bool ProposeMaxOrdered(long proposedValue)
        {
            bool updated = false;

            if (Get() < proposedValue)
            {
                SetOrdered(proposedValue);
                updated = true;
            }

            return updated;
        }
    }
}