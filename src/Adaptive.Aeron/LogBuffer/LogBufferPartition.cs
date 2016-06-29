using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    using System;

    /// <summary>
    /// Log buffer implementation containing common functionality for dealing with log partition terms.
    /// </summary>
    public class LogBufferPartition
    {
        private readonly UnsafeBuffer _termBuffer;
        private readonly UnsafeBuffer _metaDataBuffer;

        public LogBufferPartition(UnsafeBuffer termBuffer, UnsafeBuffer metaDataBuffer)
        {
            _termBuffer = termBuffer;
            _metaDataBuffer = metaDataBuffer;
        }

        /// <summary>
        /// The log of messages for a term.
        /// </summary>
        /// <returns> the log of messages for a term. </returns>
        public UnsafeBuffer TermBuffer()
        {
            return _termBuffer;
        }

        /// <summary>
        /// The meta data describing the term.
        /// </summary>
        /// <returns> the meta data describing the term. </returns>
        public UnsafeBuffer MetaDataBuffer()
        {
            return _metaDataBuffer;
        }
        
        /// <summary>
        /// Get the current tail value in a volatile memory ordering fashion. If raw tail is greater than
        /// <seealso cref="TermBuffer()"/>.<seealso cref="IDirectBuffer.Capacity"/> then capacity will be returned.
        /// </summary>
        /// <returns> the current tail value. </returns>
        public int TailOffsetVolatile()
        {
            long tail = _metaDataBuffer.GetLongVolatile(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET) & 0xFFFFFFFFL;

            return (int)Math.Min(tail, _termBuffer.Capacity);
        }

        /// <summary>
        /// Get the raw value for the tail containing both termId and offset.
        /// </summary>
        /// <returns> the raw value for the tail containing both termId and offset. </returns>
        public long RawTailVolatile()
        {
            return _metaDataBuffer.GetLongVolatile(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET);
        }

        /// <summary>
        /// Set the value of the term id into the tail counter.
        /// </summary>
        /// <param name="termId"> for the tail counter </param>
        public void TermId(int termId)
        {
            _metaDataBuffer.PutLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, ((long)termId) << 32);
        }

        /// <summary>
        /// Get the value of the term id into the tail counter.
        /// </summary>
        /// <returns> the current term id. </returns>
        public int TermId()
        {
            long rawTail = _metaDataBuffer.GetLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET);

            return (int)((long)((ulong)rawTail >> 32));
        }
    }
}