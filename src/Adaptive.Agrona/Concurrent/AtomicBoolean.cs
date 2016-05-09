using System;
using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    public class AtomicBoolean
    {
        private int _value;

        private const int TRUE = 1;
        private const int FALSE = 0;

        public AtomicBoolean(bool initialValue)
        {
            Interlocked.Exchange(ref _value, initialValue ? TRUE : FALSE);
        }
        
        /// <summary>
        /// Atomically set the value to the given updated value if the current value equals the comparand
        /// </summary>
        /// <param name="newValue">The new value</param>
        /// <param name="comparand">The comparand (expected value)</param>
        /// <returns></returns>
        public bool CompareAndSet(bool comparand, bool newValue)
        {
            var newValueInt = ToInt(newValue);
            var comparandInt = ToInt(comparand);

            return Interlocked.CompareExchange(ref _value, newValueInt, comparandInt) == comparandInt;
        }

        private static bool ToBool(int value)
        {
            if (value != FALSE && value != TRUE)
            {
                throw new ArgumentOutOfRangeException(nameof(value));
            }

            return value == TRUE;
        }

        private static int ToInt(bool value)
        {
            return value ? TRUE : FALSE;
        }
    }
}