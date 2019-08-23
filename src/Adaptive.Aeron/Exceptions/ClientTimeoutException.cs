namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Client timeout event received from the driver for this client.
    /// </summary>
    public class ClientTimeoutException : AeronTimeoutException
    {
        public ClientTimeoutException(string message) : base(message, Category.FATAL)
        {
        }
    }
}