using System;

namespace Adaptive.Agrona.Util
{
    public static class UnixTimeConverter
    {
        private static readonly DateTime Jan1st1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long CurrentUnixTimeMillis()
        {
            return (long)(DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
        }

        public static DateTime FromUnixTimeMillis(long epoch)
        {
            return Jan1st1970.AddMilliseconds(epoch);
        }
    }
}