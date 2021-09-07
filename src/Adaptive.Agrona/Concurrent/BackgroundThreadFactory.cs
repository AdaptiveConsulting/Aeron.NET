using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class BackgroundThreadFactory : IThreadFactory
    {
        public static readonly BackgroundThreadFactory Instance = new BackgroundThreadFactory();

        public Thread NewThread(ThreadStart runner)
        {
            return new Thread(runner)
            {
                IsBackground = true
            };
        }
    }
}