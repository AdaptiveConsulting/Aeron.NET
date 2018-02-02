using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class BackgoundThreadFactory : IThreadFactory
    {
        public static readonly BackgoundThreadFactory Instance = new BackgoundThreadFactory();

        public Thread NewThread(ThreadStart runner)
        {
            return new Thread(runner)
            {
                IsBackground = true
            };
        }
    }
}