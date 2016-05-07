using System;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Caused when a error occurs during addition or release of <seealso cref="Publication"/>s
    /// or <seealso cref="Subscription"/>s
    /// </summary>
    public class RegistrationException : Exception
    {
        private readonly ErrorCode _code;

        public RegistrationException(ErrorCode code, string msg) : base(msg)
        {
            this._code = code;
        }

        /// <summary>
        /// Get the <seealso cref="ErrorCode"/> for the specific exception.
        /// </summary>
        /// <returns> the <seealso cref="ErrorCode"/> for the specific exception. </returns>
        public virtual ErrorCode ErrorCode()
        {
            return _code;
        }
    }
}