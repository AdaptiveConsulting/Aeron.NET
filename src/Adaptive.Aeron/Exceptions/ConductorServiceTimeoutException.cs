using System;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// A timeout has occurred between service calls for the client conductor.
    /// </summary>
    public class ConductorServiceTimeoutException : Exception
    {
        public ConductorServiceTimeoutException(string message) : base(message)
        {
        }
    }
}