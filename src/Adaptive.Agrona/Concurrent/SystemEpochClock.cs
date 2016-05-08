using System;

namespace Adaptive.Agrona.Concurrent
{
    public class SystemEpochClock : IEpochClock
    {
        private static readonly DateTime Jan1st1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public virtual long Time()
        {
            return (long)(DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
        }
    }
}