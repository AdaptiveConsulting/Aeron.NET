namespace Adaptive.Aeron.Driver.Native
{
    public class IdleStrategy
    {
        public readonly string Name;

        private IdleStrategy(string name)
        {
            Name = name;
        }

        public static readonly IdleStrategy Sleeping = new IdleStrategy("sleeping");
        public static readonly IdleStrategy Yielding = new IdleStrategy("yielding");
        public static readonly IdleStrategy Spinning = new IdleStrategy("spinning");
        public static readonly IdleStrategy NoOp = new IdleStrategy("noop");
        public static readonly IdleStrategy Backoff = new IdleStrategy("backoff");
    }
}