using System.Runtime.CompilerServices;
using System.Threading;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Utility for applying a header to a message in a term buffer.
    /// 
    /// This class is designed to be thread safe to be used across multiple producers and makes the header
    /// visible in the correct order for consumers.
    /// </summary>
    public sealed class HeaderWriter
    {
        private readonly int _versionFlagsType;
        private readonly long _sessionId;
        private readonly long _streamId;


        public HeaderWriter(UnsafeBuffer defaultHeader)
        {
            _versionFlagsType = defaultHeader.GetInt(HeaderFlyweight.VERSION_FIELD_OFFSET);
            _sessionId = (long)defaultHeader.GetInt(DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET) << 32;
            _streamId = defaultHeader.GetInt(DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET) & 0xFFFFFFFFL;
        }

        /// <summary>
        /// Write a header to the term buffer in <seealso cref="ByteOrder.LittleEndian"/> format using the minimum instructions.
        /// </summary>
        /// <param name="termBuffer"> to be written to. </param>
        /// <param name="offset">     at which the header should be written. </param>
        /// <param name="length">     of the fragment including the header. </param>
        /// <param name="termId">     of the current term buffer. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Write(UnsafeBuffer termBuffer, int offset, int length, int termId)
        {
            var termOffsetSessionId = _sessionId | (uint)offset;
            var streamAndTermIds = _streamId | ((long)termId << 32);

            *(int*)(termBuffer.BufferPointer + offset + HeaderFlyweight.VERSION_FIELD_OFFSET) = _versionFlagsType;
            *(long*)(termBuffer.BufferPointer + offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET) = termOffsetSessionId;
            *(long*)(termBuffer.BufferPointer + offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET) = streamAndTermIds;
        }
    }
}