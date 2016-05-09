using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class AtomicReference<T> where T : class
    {
        private T _value;

        /// <summary>
        /// Gets the current value.
        /// </summary>
        /// <returns> the current value.</returns>
        public T Get()
        {
            return Volatile.Read(ref _value);
        }

        /// <summary>
        /// Eventually sets to the given value.
        /// </summary>
        /// <param name="newValue"> the new value.</param>
        public void LazySet(T newValue)
        {
            Volatile.Write(ref _value, newValue);
        }

        public T GetAndSet(T value)
        {
            var thread = Volatile.Read(ref _value);
            Volatile.Write(ref _value, value);
            return thread;
        }

        public bool CompareAndSet(T compareValue, T newValue)
        {
            return Interlocked.CompareExchange(ref _value, newValue, compareValue) == newValue;
        }
    }
}