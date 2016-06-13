using System.Threading;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Rebuild a term buffer based on incoming frames that can be out-of-order.
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
            var firstFrameLength = packet.GetInt(0); // LITTLE_ENDIAN
            packet.PutIntOrdered(0, 0);
            Thread.MemoryBarrier(); // UnsafeAccess.UNSAFE.storeFence();

            termBuffer.PutBytes(termOffset, packet, 0, length);
            FrameDescriptor.FrameLengthOrdered(termBuffer, termOffset, firstFrameLength);
        }
    }
}