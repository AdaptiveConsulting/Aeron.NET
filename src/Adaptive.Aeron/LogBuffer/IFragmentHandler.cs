using Adaptive.Agrona;

namespace Adaptive.Aeron.LogBuffer
{
    public interface IFragmentHandler
    {
        /// <summary>
        /// Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
        /// or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.
        /// </summary>
        /// <param name="buffer"> containing the data. </param>
        /// <param name="offset"> at which the data begins. </param>
        /// <param name="length"> of the data in bytes. </param>
        /// <param name="header"> representing the meta data for the data. </param>
        void OnFragment(IDirectBuffer buffer, int offset, int length, Header header);
    }
}