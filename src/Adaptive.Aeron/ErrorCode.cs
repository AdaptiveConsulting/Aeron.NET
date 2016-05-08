namespace Adaptive.Aeron
{
    /// <summary>
    /// Error codes between media driver and client and the on-wire protocol.
    /// </summary>
    public enum ErrorCode
    {
        /// <summary>
        /// Aeron encountered an error condition. </summary>
        GENERIC_ERROR = 0,
        /// <summary>
        /// A failure occurred creating a new channel or parsing the channel string. </summary>
        INVALID_CHANNEL = 1,
        /// <summary>
        /// Attempted to remove a subscription, but it was not found </summary>
        UNKNOWN_SUBSCRIPTION = 2,
        /// <summary>
        /// Attempted to remove a publication, but it was not found. </summary>
        UNKNOWN_PUBLICATION = 3
    }
}