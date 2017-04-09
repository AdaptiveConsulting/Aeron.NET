using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Rebuild a term buffer from received frames which can be out-of-order. The resulting data structure will only
    /// monotonically increase in state.
    /// </summary>
    public class TermRebuilder
    {
        /// <summary>
        /// Insert a packet of frames into the log at the appropriate offset as indicated by the term termOffset header.
        /// </summary>
        /// <param name="termBuffer"> into which the packet should be inserted. </param>
        /// <param name="termOffset">     offset in the term at which the packet should be inserted. </param>
        /// <param name="packet">     containing a sequence of frames. </param>
        /// <param name="length">     of the sequence of frames in bytes. </param>
        public static void Insert(IAtomicBuffer termBuffer, int termOffset, UnsafeBuffer packet, int length)
        {
            termBuffer.PutBytes(termOffset + DataHeaderFlyweight.HEADER_LENGTH, packet, DataHeaderFlyweight.HEADER_LENGTH, length - DataHeaderFlyweight.HEADER_LENGTH);

            termBuffer.PutLong(termOffset + 24, packet.GetLong(24));
            termBuffer.PutLong(termOffset + 16, packet.GetLong(16));
            termBuffer.PutLong(termOffset + 8, packet.GetLong(8));

            termBuffer.PutLongOrdered(termOffset, packet.GetLong(0));
        }
    }
}