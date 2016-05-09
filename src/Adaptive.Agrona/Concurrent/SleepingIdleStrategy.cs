using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// When idle this strategy is to sleep for a specified period.
    /// </summary>
    public sealed class SleepingIdleStrategy : IIdleStrategy
    {
        private readonly int _sleepPeriodMs;

        /// <summary>
        /// Constructed a new strategy that will sleep for a given period when idle.
        /// </summary>
        /// <param name="sleepPeriodMs"> period in millisecond for which the strategy will sleep when work count is 0. </param>

        public SleepingIdleStrategy(int sleepPeriodMs)
        {
            _sleepPeriodMs = sleepPeriodMs;
        }

        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Thread.Sleep(_sleepPeriodMs);
        }

        public void Idle()
        {
            Thread.Sleep(_sleepPeriodMs);
        }

        public void Reset()
        {
        }
    }

}