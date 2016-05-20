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
        /// Clean down the buffers for reuse by zeroing them out.
        /// </summary>
        public void Clean()
        {
            _termBuffer.SetMemory(0, _termBuffer.Capacity, 0);
            _metaDataBuffer.PutInt(LogBufferDescriptor.TERM_STATUS_OFFSET, LogBufferDescriptor.CLEAN);
        }

        /// <summary>
        /// What is the current status of the buffer.
        /// </summary>
        /// <returns> the status of buffer as described in <seealso cref="LogBufferDescriptor"/> </returns>
        public int Status()
        {
            return _metaDataBuffer.GetIntVolatile(LogBufferDescriptor.TERM_STATUS_OFFSET);
        }

        /// <summary>
        /// Set the status of the log buffer with StoreStore memory ordering semantics.
        /// </summary>
        /// <param name="status"> to be set for the log buffer. </param>
        public void StatusOrdered(int status)
        {
            _metaDataBuffer.PutIntOrdered(LogBufferDescriptor.TERM_STATUS_OFFSET, status);
        }

        /// <summary>
        /// Get the current tail value in a volatile memory ordering fashion. If raw tail is greater than
        /// <seealso cref="TermBuffer()"/>.<seealso cref="UnsafeBuffer.Capacity"/> then capacity will be returned.
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