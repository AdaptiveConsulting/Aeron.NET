using System;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    internal static class BufferBuilderUtil
    {
        /// <summary>
        /// Maximum capacity to which the buffer can grow.
        /// </summary>
        internal const int MAX_CAPACITY = int.MaxValue - 8;

        /// <summary>
        /// Initial minimum capacity for the internal buffer when used, zero if not used.
        /// </summary>
        internal const int MIN_ALLOCATED_CAPACITY = 4096;

        internal static int FindSuitableCapacity(int capacity, int requiredCapacity)
        {
            do
            {
                var candidateCapacity = capacity + (capacity >> 1);
                int newCapacity = Math.Max(candidateCapacity, MIN_ALLOCATED_CAPACITY);

                if (candidateCapacity < 0 || newCapacity > MAX_CAPACITY)
                {
                    if (capacity == MAX_CAPACITY)
                    {
                        ThrowHelper.ThrowInvalidOperationException("Max capacity reached: " + MAX_CAPACITY);
                    }

                    capacity = MAX_CAPACITY;
                }
                else
                {
                    capacity = newCapacity;
                }
            } while (capacity < requiredCapacity);

            return capacity;
        }
    }
}