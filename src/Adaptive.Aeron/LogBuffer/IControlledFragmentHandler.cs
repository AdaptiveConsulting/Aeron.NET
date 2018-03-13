using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    public interface IControlledFragmentHandler
    {
        /// <summary>
        /// Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
        /// or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.
        /// </summary>
        /// <param name="buffer"> containing the data. </param>
        /// <param name="offset"> at which the data begins. </param>
        /// <param name="length"> of the data in bytes. </param>
        /// <param name="header"> representing the meta data for the data. </param>
        /// <returns> The action to be taken with regard to the stream position after the callback. </returns>
        ControlledFragmentHandlerAction OnFragment(UnsafeBuffer buffer, int offset, int length, Header header);
    }
}