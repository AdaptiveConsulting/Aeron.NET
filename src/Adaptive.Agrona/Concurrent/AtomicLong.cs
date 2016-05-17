using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class AtomicLong
    {
        private long _long;

        /// <summary>
        /// Gets the current value.
        /// </summary>
        /// <returns> the current value.</returns>
        public long Get()
        {
            return Interlocked.Read(ref _long);
        }

        /// <summary>
        /// Eventually sets to the given value.
        /// </summary>
        /// <param name="newValue"> the new value.</param>
        public void LazySet(long newValue)
        {
            Interlocked.Exchange(ref _long, newValue);
        }

        public void Set(long value)
        {
            Interlocked.Exchange(ref _long, value);
        }

        public void Add(long add)
        {
            Interlocked.Add(ref _long, add);
        }
    }
}