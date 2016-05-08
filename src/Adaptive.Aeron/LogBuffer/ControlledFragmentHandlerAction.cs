using Adaptive.Agrona;

namespace Adaptive.Aeron.LogBuffer
{
    public enum ControlledFragmentHandlerAction
    {
        /// <summary>
        /// Abort the current polling operation and do not advance the position for this fragment.
        /// </summary>
        Abort,

        /// <summary>
        /// Break from the current polling operation and commit the position as of the end of the current fragment
        /// being handled.
        /// </summary>
        Break,

        /// <summary>
        /// Continue processing but commit the position as of the end of the current fragment so that
        /// flow control is applied to this point.
        /// </summary>
        Commit,

        /// <summary>
        /// Continue processing taking the same approach as the in
        /// <seealso cref="IFragmentHandler.OnFragment(IDirectBuffer, int, int, Header)"/>.
        /// </summary>
        Continue,
    }
}