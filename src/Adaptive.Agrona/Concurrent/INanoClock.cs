namespace Adaptive.Agrona.Concurrent
{

    /// <summary>
    /// Functional interface for return the current time as system wide monotonic tick of 1 nanosecond precision.
    /// </summary>
    public interface INanoClock
    {
        /// <summary>
        /// The number of ticks in nanoseconds the clock has advanced since starting.
        /// </summary>
        /// <returns> number of ticks in nanoseconds the clock has advanced since starting.</returns>
        long NanoTime();
    }

}