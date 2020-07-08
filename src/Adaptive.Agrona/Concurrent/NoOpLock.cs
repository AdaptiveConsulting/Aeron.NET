namespace Adaptive.Agrona.Concurrent
{
    public class NoOpLock : ILock
    {
        public static readonly NoOpLock Instance = new NoOpLock();
        
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
