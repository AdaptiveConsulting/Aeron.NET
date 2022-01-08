using System;
using System.Runtime.Serialization;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Used to indicate a failed authentication attempt when connecting to a system.
    /// </summary>
    public class AuthenticationException : AeronException
    {
        /// <summary>
        /// Authentication exception with provided message and <seealso cref="Category.ERROR"/>.
        /// </summary>
        /// <param name="message"> to detail the exception. </param>
        public AuthenticationException(string message) : base(message)
        {
        }
    }
}