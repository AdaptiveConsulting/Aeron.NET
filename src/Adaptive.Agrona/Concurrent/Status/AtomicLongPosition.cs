using System.Runtime.CompilerServices;

namespace Adaptive.Agrona.Concurrent.Status
{
    public class AtomicLongPosition : IPosition
    {
        private readonly AtomicLong _value = new AtomicLong();

        public void Dispose()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Id()
        {
            return 0;
        }

        public long Volatile => _value.Get();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Get()
        {
            return _value.Get();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long value)
        {
            _value.Set(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetOrdered(long value)
        {
            _value.Set(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProposeMax(long proposedValue)
        {
            return ProposeMaxOrdered(proposedValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ProposeMaxOrdered(long proposedValue)
        {
            bool updated = false;

            if (Get() < proposedValue)
            {
                SetOrdered(proposedValue);
                updated = true;
            }

            return updated;
        }
    }
}