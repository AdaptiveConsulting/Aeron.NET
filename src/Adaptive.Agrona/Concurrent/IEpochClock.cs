namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Retrieves the number of milliseconds since 1 Jan 1970 UTC.
    /// </summary>
    public interface IEpochClock
    {
        /// <summary>
        /// Time in milliseconds since the 1 Jan 1970 UTC.
        /// </summary>
        /// <returns> the number of milliseconds since the 1 Jan 1970 UTC.</returns>
        long Time();
    }
}