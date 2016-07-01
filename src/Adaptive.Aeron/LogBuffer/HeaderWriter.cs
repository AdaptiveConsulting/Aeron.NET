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
    public class HeaderWriter
    {
        private readonly long _versionFlagsType;
        private readonly long _sessionId;
        private readonly long _streamId;

        public HeaderWriter()
        {
        }

        public HeaderWriter(IDirectBuffer defaultHeader)
        {
            _versionFlagsType = (long)defaultHeader.GetInt(HeaderFlyweight.VERSION_FIELD_OFFSET) << 32;
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
        public virtual void Write(IAtomicBuffer termBuffer, int offset, int length, int termId)
        {
            var lengthVersionFlagsType = _versionFlagsType | (-length & 0xFFFFFFFFL);
            var termOffsetSessionId = _sessionId | (uint)offset;
            var streamAndTermIds = _streamId | ((long)termId << 32);

            termBuffer.PutLongOrdered(offset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, lengthVersionFlagsType);

            termBuffer.PutLongOrdered(offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, termOffsetSessionId);
            termBuffer.PutLong(offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, streamAndTermIds);
        }
    }
}