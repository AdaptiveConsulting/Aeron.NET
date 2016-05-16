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
}