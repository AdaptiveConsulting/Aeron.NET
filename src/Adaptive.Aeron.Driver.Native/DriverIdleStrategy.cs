using System;

namespace Adaptive.Aeron.Driver.Native
{
    /// <summary>
    /// Idle strategies for the native media driver.
    /// </summary>
    public class DriverIdleStrategy
    {
        public readonly string Name;

        private DriverIdleStrategy(string name)
        {
            Name = name;
        }

        public static readonly DriverIdleStrategy SLEEPING = new DriverIdleStrategy("sleeping");
        public static readonly DriverIdleStrategy YIELDING = new DriverIdleStrategy("yielding");
        public static readonly DriverIdleStrategy SPINNING = new DriverIdleStrategy("spinning");
        public static readonly DriverIdleStrategy NOOP = new DriverIdleStrategy("noop");
        public static readonly DriverIdleStrategy BACKOFF = new DriverIdleStrategy("backoff");

        public override string ToString()
        {
            return Name;
        }

        public static DriverIdleStrategy FromString(string name)
        {
            if (name.StartsWith("sleep", StringComparison.OrdinalIgnoreCase))
                return SLEEPING;
            if (name.StartsWith("yield", StringComparison.OrdinalIgnoreCase))
                return YIELDING;
            if (name.StartsWith("spin", StringComparison.OrdinalIgnoreCase))
                return SPINNING;
            if (name.StartsWith("noop", StringComparison.OrdinalIgnoreCase))
                return NOOP;

            return BACKOFF;
        }
    }
}