using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class ReentrantLock : ILock
    {
        private readonly object _lockObj = new object();

        public void Lock()
        {
            Monitor.Enter(_lockObj);
        }

        public void Unlock()
        {
            Monitor.Exit(_lockObj);
        }

        public bool TryLock()
        {
            return Monitor.TryEnter(_lockObj);
        }
    }
}