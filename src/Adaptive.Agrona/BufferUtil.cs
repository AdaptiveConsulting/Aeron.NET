using System;
using System.Text;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Common functions for buffer implementations.
    /// </summary>
    public class BufferUtil
    {
        public static readonly byte[] NullBytes = Encoding.UTF8.GetBytes("null");

        /// <summary>
        /// Bounds check the access range and throw a <seealso cref="IndexOutOfRangeException"/> if exceeded.
        /// </summary>
        /// <param name="buffer"> to be checked. </param>
        /// <param name="index">  at which the access will begin. </param>
        /// <param name="length"> of the range accessed. </param>
        public static void BoundsCheck(byte[] buffer, long index, int length)
        {
#if SHOULD_BOUNDS_CHECK
            var capacity = buffer.Length;
            var resultingPosition = index + length;
            if (index < 0 || resultingPosition > capacity)
            {
                throw new IndexOutOfRangeException($"index={index:D}, length={length:D}, capacity={capacity:D}");
            }
#endif
        }
    }
}
