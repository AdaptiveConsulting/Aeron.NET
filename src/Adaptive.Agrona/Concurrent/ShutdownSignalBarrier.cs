using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// One time barrier for blocking one or more threads until a SIGINT or SIGTERM signal is received from the operating
    /// system or by programmatically calling <seealso cref="#signal()"/>. Useful for shutting down a service.
    /// </summary>
    public class ShutdownSignalBarrier
    {
        private readonly CountdownEvent _latch = new CountdownEvent(1);

        /// <summary>
        /// Programmatically signal awaiting threads.
        /// </summary>
        public void Signal()
        {
            _latch.Signal();
        }

        /// <summary>
        /// Await the reception of the shutdown signal.
        /// </summary>
        public virtual void Await()
        {
            try
            {
                _latch.Wait();
            }
            catch (ThreadInterruptedException)
            {
            }
        }
    }
}