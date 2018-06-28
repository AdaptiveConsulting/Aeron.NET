using System;
using System.Runtime.Serialization;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Base Aeron exception for catching all Aeron specific errors.
    /// </summary>
    public class AeronException : Exception
    {
        public AeronException()
        {
        }

        protected AeronException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public AeronException(string message) : base(message)
        {
        }

        public AeronException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}