namespace Adaptive.Agrona.Concurrent
{
    public interface ILock
    {
        void Lock();
        void Unlock();
        bool TryLock();
    }
}