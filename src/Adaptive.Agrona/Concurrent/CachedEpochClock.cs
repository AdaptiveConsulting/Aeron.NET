namespace Adaptive.Agrona.Concurrent
{
    public class CachedEpochClock : IEpochClock
    {
        private long timeMs;
        
        public long Time()
        {
            return timeMs;
        }

        public void Update(long timeMs)
        {
            this.timeMs = timeMs;
        }
    }
}