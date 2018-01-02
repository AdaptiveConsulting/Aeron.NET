namespace Adaptive.Agrona.Concurrent
{
    /// <inheritdoc />
    public class NullEpochClock : IEpochClock
    {
        /// <inheritdoc />
        public long Time()
        {
            return 0;
        }
    }
}