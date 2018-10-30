using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// One time barrier for blocking one or more threads until a SIGINT or SIGTERM signal is received from the operating
    /// system or by programmatically calling <seealso cref="Signal"/>. Useful for shutting down a service.
    /// </summary>
    public class ShutdownSignalBarrier
    {
        private readonly ManualResetEventSlim _latch = new ManualResetEventSlim(false);

        /// <summary>
        /// Programmatically signal awaiting threads.
        /// </summary>
        public void Signal()
        {
            _latch.Set();
        }

        /// <summary>
        /// Await the reception of the shutdown signal.
        /// </summary>
        public void Await()
        {
            try
            {
                _latch.Wait();
            }
            catch (ThreadInterruptedException)
            {
                Thread.CurrentThread.Interrupt();
            }
        }
    }
}