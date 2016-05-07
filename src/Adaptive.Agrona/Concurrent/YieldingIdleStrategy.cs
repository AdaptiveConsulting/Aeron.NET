using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// <seealso cref="IIdleStrategy"/> that will call <seealso cref="Thread.Yield"/> when the work count is zero.
    /// </summary>
    public sealed class YieldingIdleStrategy : IIdleStrategy
    {
        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Thread.Yield();
        }

        public void Idle()
        {
            Thread.Yield();
        }

        public void Reset()
        {
        }
    }
}