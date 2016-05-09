using System.Diagnostics;

namespace Adaptive.Agrona.Concurrent
{
    public class StopwatchClock : INanoClock
    {
        private readonly Stopwatch _stopwatch;

        public StopwatchClock()
        {
            _stopwatch = Stopwatch.StartNew();
        }

        public long NanoTime()
        {
            return _stopwatch.ElapsedTicks/Stopwatch.Frequency*1000000000;
        }
    }
}