using System;

namespace Adaptive.Agrona.Concurrent.RingBuffer
{
    /// <summary>
    /// Layout description for the underlying buffer used by a <seealso cref="RingBuffer"/>. The buffer consists
    /// of a ring of messages which is a power of 2 in size, followed by a trailer section containing state
    /// information for the producers and consumers of the ring.
    /// </summary>
    public class RingBufferDescriptor
    {
        /// <summary>
        /// Offset within the trailer for where the tail value is stored. 
        /// </summary>
        public static readonly int TailPositionOffset;

        /// <summary>
        /// Offset within the trailer for where the head cache value is stored. 
        /// </summary>
        public static readonly int HeadCachePositionOffset;

        /// <summary>
        /// Offset within the trailer for where the head value is stored. 
        /// </summary>
        public static readonly int HeadPositionOffset;

        /// <summary>
        /// Offset within the trailer for where the correlation counter value is stored. 
        /// </summary>
        public static readonly int CorrelationCounterOffset;

        /// <summary>
        /// Offset within the trailer for where the consumer heartbeat time value is stored. 
        /// </summary>
        public static readonly int ConsumerHeartbeatOffset;

        /// <summary>
        /// Total length of the trailer in bytes. 
        /// </summary>
        public static readonly int TrailerLength;

        static RingBufferDescriptor()
        {
            var offset = 0;
            offset += BitUtil.CACHE_LINE_LENGTH*2;
            TailPositionOffset = offset;

            offset += BitUtil.CACHE_LINE_LENGTH*2;
            HeadCachePositionOffset = offset;

            offset += BitUtil.CACHE_LINE_LENGTH*2;
            HeadPositionOffset = offset;

            offset += BitUtil.CACHE_LINE_LENGTH*2;
            CorrelationCounterOffset = offset;

            offset += BitUtil.CACHE_LINE_LENGTH*2;
            ConsumerHeartbeatOffset = offset;

            offset += BitUtil.CACHE_LINE_LENGTH*2;
            TrailerLength = offset;
        }

        /// <summary>
        /// Check the the buffer capacity is the correct size (a power of 2 + <seealso cref="TrailerLength"/>).
        /// </summary>
        /// <param name="capacity"> to be checked. </param>
        /// <exception cref="InvalidOperationException"> if the buffer capacity is incorrect. </exception>
        public static void CheckCapacity(int capacity)
        {
            if (!BitUtil.IsPowerOfTwo(capacity - TrailerLength))
            {
                var msg = "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=" + capacity;
                throw new InvalidOperationException(msg);
            }
        }
    }
}