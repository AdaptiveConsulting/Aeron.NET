using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    // TODO JPW needs some more research
    public class AtomicLong
    {
        private long _long;

        /// <summary>
        /// Gets the current value.
        /// </summary>
        /// <returns> the current value.</returns>
        public long Get()
        {
            return Volatile.Read(ref _long);
        }

        /// <summary>
        /// Eventually sets to the given value.
        /// </summary>
        /// <param name="newValue"> the new value.</param>
        public void LazySet(long newValue)
        {
            Volatile.Write(ref _long, newValue);
        }
    }
}