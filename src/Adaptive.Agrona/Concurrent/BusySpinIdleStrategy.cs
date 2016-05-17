using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Busy spin strategy targeted at lowest possible latency. This strategy will monopolise a thread to achieve the lowest
    /// possible latency. Useful for creating bubbles in the execution pipeline of tight busy spin loops with no other logic than
    /// status checks on progress.
    /// </summary>
    public sealed class BusySpinIdleStrategy : IIdleStrategy
    {
        /// <summary>
        /// <b>Note</b>: this implementation will result in no safepoint poll once inlined.
        /// </summary>
        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Thread.SpinWait(0);
        }

        public void Idle()
        {
            Thread.SpinWait(0);
        }

        public void Reset()
        {
        }
    }
}