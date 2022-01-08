using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Status of the Aeron media channel for a <seealso cref="Publication"/> or <seealso cref="Subscription"/>.
    /// </summary>
    public static class ChannelEndpointStatus
    {
        /// <summary>
        /// Channel is being initialized.
        /// </summary>
        public const long INITIALIZING = 0;

        /// <summary>
        /// Channel has errored. Check error log for information.
        /// </summary>
        public const long ERRORED = -1;

        /// <summary>
        /// Channel has finished initialization successfully and is active.
        /// </summary>
        public const long ACTIVE = 1;

        /// <summary>
        /// Channel is being closed.
        /// </summary>
        public const long CLOSING = 2;

        /// <summary>
        /// No counter ID is allocated yet.
        /// </summary>
        public const int NO_ID_ALLOCATED = Aeron.NULL_VALUE;

        /// <summary>
        /// Offset in the key metadata for the channel of the counter.
        /// </summary>
        public const int CHANNEL_OFFSET = 0;
    }
}