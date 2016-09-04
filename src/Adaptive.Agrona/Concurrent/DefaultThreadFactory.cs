using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class DefaultThreadFactory : IThreadFactory
    {
        public Thread NewThread(ThreadStart runner)
        {
            return new Thread(runner);
        }
    }
}