using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// When idle this strategy is to sleep for a specified period.
    /// </summary>
    public sealed class SleepingIdleStrategy : IIdleStrategy
    {
        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Thread.Sleep(1);
        }

        public void Idle()
        {
            Thread.Sleep(1);
        }

        public void Reset()
        {
        }
    }

}