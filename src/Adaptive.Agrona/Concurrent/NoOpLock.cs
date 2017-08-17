namespace Adaptive.Agrona.Concurrent
{
    public class NoOpLock : ILock
    {
        public void Lock()
        {
        }

        public void Unlock()
        {
        }

        public bool TryLock()
        {
            return true;
        }
    }
}
