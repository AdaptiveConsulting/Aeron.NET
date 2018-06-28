namespace Adaptive.Aeron.Exceptions
{
    public class ChannelEndpointException : AeronException
    {
        public ChannelEndpointException(int statusIndicatorId, string message) : base(message)
        {
            StatusIndicatorId = statusIndicatorId;
        }
        
        /// <summary>
        /// Return the id for the counter associated with the channel endpoint.
        /// </summary>
        /// <returns>counter id associated with the channel endpoint</returns>
        public int StatusIndicatorId { get; }
    }
}