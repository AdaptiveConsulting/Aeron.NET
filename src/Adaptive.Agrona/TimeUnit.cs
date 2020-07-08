using System;

namespace Adaptive.Agrona
{
    public class TimeUnit
    {
        public static readonly TimeUnit NANOSECONDS = new TimeUnit();
        public static readonly TimeUnit MILLIS = new TimeUnit();

        private TimeUnit()
        {
        }

        public long Convert(long sourceValue, TimeUnit sourceTimeUnit)
        {
            if (sourceTimeUnit == NANOSECONDS)
            {
                if (this == MILLIS)
                {
                    return sourceValue / 1000000;
                }

                if (this == NANOSECONDS)
                {
                    return sourceValue;
                }
            }

            if (sourceTimeUnit == MILLIS)
            {
                if (this == MILLIS)
                {
                    return sourceValue;
                }

                if (this == NANOSECONDS)
                {
                    return sourceValue * 1000000;
                }
            }
            
            throw new ArgumentException();
        }

        public long toMillis(long value)
        {
            return Convert(value, MILLIS);
        }
    }
}