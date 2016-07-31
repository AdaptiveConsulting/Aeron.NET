using System;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// A timeout has occurred while waiting on the media driver responding to an operation.
    /// </summary>
    public class DriverTimeoutException : Exception
    {
        public DriverTimeoutException(string message) : base(message)
        {
        }
    }
}