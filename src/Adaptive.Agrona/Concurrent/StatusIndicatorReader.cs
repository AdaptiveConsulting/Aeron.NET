namespace Adaptive.Agrona.Concurrent
{
    public abstract class StatusIndicatorReader
    {
        /// <summary>
        /// Identifier for this status indicator.
        /// </summary>
        /// <returns> the identifier for this status indicator. </returns>
        public abstract int Id();

        /// <summary>
        /// Get the current status indication of a component with volatile semantics.
        /// </summary>
        /// <returns> the current status indication of a component with volatile semantics. </returns>
        public abstract long GetVolatile();
    }
}