using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Function for handling a block of message fragments scanned from the log.
    /// </summary>
    public interface IBlockHandler
    {
        /// <summary>
        /// Callback for handling a block of messages being read from a log.
        /// </summary>
        /// <param name="buffer">    containing the block of message fragments. </param>
        /// <param name="offset">    at which the block begins, including any frame headers. </param>
        /// <param name="length">    of the block in bytes, including any frame headers that is aligned up to
        ///                  <seealso cref="FrameDescriptor.FRAME_ALIGNMENT"/>. </param>
        /// <param name="sessionId"> of the stream containing this block of message fragments. </param>
        /// <param name="termId">    of the stream containing this block of message fragments. </param>
        void OnBlock(UnsafeBuffer buffer, int offset, int length, int sessionId, int termId);
    }
}