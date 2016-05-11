using System;
using System.Threading;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Samples.StreamingPublisher
{
    internal class IntSupplier
    {
        private readonly ThreadLocal<Random> _random = new ThreadLocal<Random>(() => new Random());
        private readonly int _max;
        private readonly bool _isRandom;

        public IntSupplier(bool random, int max)
        {
            _isRandom = random;
            _max = max;
        }

        public int AsInt
        {
            get
            {
                if (_isRandom)
                {
                    return _random.Value.Next(BitUtil.SIZE_OF_LONG, _max);
                }

                return _max;
            }
        }
    }
}