namespace Adaptive.Aeron.LogBuffer
{

    // TODO
    ///// <summary>
    ///// Function for handling a block of fragments from the log that are contained in the underlying file.
    ///// </summary>
    //public interface IFileBlockHandler
    //{
    //    ///// <summary>
    //    ///// Notification of an available block of fragments.
    //    ///// </summary>
    //    ///// <param name="fileChannel"> containing the block of fragments. </param>
    //    ///// <param name="offset">      at which the block begins, including any frame headers. </param>
    //    ///// <param name="length">      of the block in bytes, including any frame headers that is aligned up to
    //    /////                    <seealso cref="io.aeron.logbuffer.FrameDescriptor#FRAME_ALIGNMENT"/>. </param>
    //    ///// <param name="sessionId">   of the stream of fragments. </param>
    //    ///// <param name="termId">      of the stream of fragments. </param>
    //    void OnBlock(FileChannel fileChannel, long offset, int length, int sessionId, int termId);
    //}
}