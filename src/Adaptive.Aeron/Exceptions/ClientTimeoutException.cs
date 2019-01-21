using System;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Client timeout event received from the driver for this client.
    /// </summary>
    public class ClientTimeoutException : TimeoutException
    {
        public ClientTimeoutException(string message) : base(message)
        {
        }
    }
}