using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    namespace io.aeron.logbuffer
    {
        /// <summary>
        /// Fills a gap in a term with a padding record.
        /// </summary>
        public class TermGapFiller
        {
            /// <summary>
            /// Try to gap fill the current term at a given offset if the gap contains no data.
            /// 
            /// Note: the gap offset plus gap length must end on a <seealso cref="FrameDescriptor.FRAME_ALIGNMENT"/> boundary.
            /// </summary>
            /// <param name="logMetaDataBuffer"> containing the default headers </param>
            /// <param name="termBuffer">        to gap fill </param>
            /// <param name="termId">            for the current term. </param>
            /// <param name="gapOffset">         to fill from </param>
            /// <param name="gapLength">         to length of the gap. </param>
            /// <returns> true if the gap has been filled with a padding record or false if data found. </returns>
            public static bool TryFillGap(UnsafeBuffer logMetaDataBuffer, UnsafeBuffer termBuffer, int termId, int gapOffset, int gapLength)
            {
                int offset = gapOffset + gapLength - FrameDescriptor.FRAME_ALIGNMENT;

                while (offset >= gapOffset)
                {
                    if (0 != termBuffer.GetInt(offset))
                    {
                        return false;
                    }

                    offset -= FrameDescriptor.FRAME_ALIGNMENT;
                }

                LogBufferDescriptor.ApplyDefaultHeader(logMetaDataBuffer, termBuffer, gapOffset);
                FrameDescriptor.FrameType(termBuffer, gapOffset, HeaderFlyweight.HDR_TYPE_PAD);
                FrameDescriptor.FrameTermOffset(termBuffer, gapOffset);
                FrameDescriptor.FrameTermId(termBuffer, gapOffset, termId);
                FrameDescriptor.FrameLengthOrdered(termBuffer, gapOffset, gapLength);

                return true;
            }
        }
    }
}