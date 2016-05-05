using System;

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Indicates how far through an abstract task a component has progressed as a counter value.
    /// </summary>
    public interface IReadablePosition : IDisposable
    {
        /// <summary>
        /// Identifier for this position.
        /// </summary>
        /// <returns> the identifier for this position. </returns>
        int Id();

        /// <summary>
        /// Get the current position of a component with volatile semantics
        /// </summary>
        /// <returns> the current position of a component with volatile semantics </returns>
        long Volatile { get; }
    }
}