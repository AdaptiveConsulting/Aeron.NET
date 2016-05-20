using Adaptive.Agrona;

namespace Adaptive.Aeron.LogBuffer
{
    public enum ControlledFragmentHandlerAction
    {
        /// <summary>
        /// Abort the current polling operation and do not advance the position for this fragment.
        /// </summary>
        ABORT,

        /// <summary>
        /// Break from the current polling operation and commit the position as of the end of the current fragment
        /// being handled.
        /// </summary>
        BREAK,

        /// <summary>
        /// Continue processing but commit the position as of the end of the current fragment so that
        /// flow control is applied to this point.
        /// </summary>
        COMMIT,

        /// <summary>
        /// Continue processing taking the same approach as the in
        /// <seealso cref="FragmentHandler.OnFragment(UnsafeBuffer, int, int, Header)"/>.
        /// </summary>
        CONTINUE,
    }
}