namespace Adaptive.Agrona.Concurrent
{
    public abstract class StatusIndicatorReader
    {
        /// <summary>
        /// Identifier for this status indicator.
        /// </summary>
        /// <value> the identifier for this status indicator. </value>
        public abstract int Id { get; }

        /// <summary>
        /// Get the current status indication of a component with volatile semantics.
        /// </summary>
        /// <returns> the current status indication of a component with volatile semantics. </returns>
        public abstract long GetVolatile();
    }
}