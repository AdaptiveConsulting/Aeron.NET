using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// <see cref="IIdleStrategy"/> which uses a <see cref="SpinWait"/>.
    /// </summary>
    public class SpinWaitIdleStrategy : IIdleStrategy
    {
        private SpinWait _spinWait = new SpinWait();

        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            _spinWait.SpinOnce();
        }

        public void Idle()
        {
            _spinWait.SpinOnce();
        }

        public void Reset()
        {
            _spinWait.Reset();
        }
    }
}