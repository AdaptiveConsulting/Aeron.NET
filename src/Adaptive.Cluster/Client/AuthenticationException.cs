using System;
using System.Runtime.Serialization;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Used to indicated a failed authentication attempt when connecting to the cluster.
    /// </summary>
    public class AuthenticationException : AeronException
    {
        public AuthenticationException()
        {
        }

        protected AuthenticationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public AuthenticationException(string message) : base(message)
        {
        }

        public AuthenticationException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}