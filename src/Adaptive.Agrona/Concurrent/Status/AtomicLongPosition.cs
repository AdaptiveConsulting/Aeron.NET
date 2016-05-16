namespace Adaptive.Agrona.Concurrent.Status
{
    public class AtomicLongPosition : IPosition
    {
        private readonly AtomicLong _value = new AtomicLong();

        public void Dispose()
        {
        }

        public int Id()
        {
            return 0;
        }

        public long Volatile => _value.Get();

        public long Get()
        {
            return _value.Get();
        }

        public void Set(long value)
        {
            _value.Set(value);
        }

        public void SetOrdered(long value)
        {
            _value.Set(value);
        }

        public bool ProposeMax(long proposedValue)
        {
            return ProposeMaxOrdered(proposedValue);
        }

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