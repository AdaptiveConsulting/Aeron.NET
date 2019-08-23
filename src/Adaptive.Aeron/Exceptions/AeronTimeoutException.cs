using System;

namespace Adaptive.Aeron.Exceptions
{
    public class AeronTimeoutException : AeronException
    {
        public AeronTimeoutException()
        {
        }

        public AeronTimeoutException(string message) : base(message)
        {
        }

        public AeronTimeoutException(string message, Category category) : base(message, category)
        {
        }

        public AeronTimeoutException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public AeronTimeoutException(string message, Exception innerException, Category category) : base(message,
            innerException, category)
        {
        }
    }
}