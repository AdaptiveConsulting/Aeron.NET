using System;
using System.Runtime.Serialization;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Base Aeron exception for catching all Aeron specific errors.
    /// </summary>
    public class AeronException : Exception
    {
        public Category Category { get; }

        public AeronException()
        {
            Category = Category.ERROR;
        }

        public AeronException(Category category)
        {
            Category = category;
        }

        protected AeronException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            Category = Category.ERROR;
        }

        public AeronException(string message) : base(message)
        {
            Category = Category.ERROR;
        }

        public AeronException(string message, Category category) : base(message)
        {
            Category = category;
        }

        public AeronException(string message, Exception innerException) : base(message, innerException)
        {
            Category = Category.ERROR;
        }

        public AeronException(string message, Exception innerException, Category category) : base(message,
            innerException)
        {
            Category = category;
        }
    }
}