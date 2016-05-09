using System.Diagnostics;

namespace Adaptive.Agrona.Concurrent
{
    public class SystemNanoClock : INanoClock
    {
        private readonly Stopwatch _stopwatch;

        public SystemNanoClock()
        {
            _stopwatch = Stopwatch.StartNew();
        }

        public long NanoTime()
        {
            return _stopwatch.ElapsedMilliseconds*1000*1000;
        }
    }
}