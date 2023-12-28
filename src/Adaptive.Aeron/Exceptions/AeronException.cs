using System;
using System.Runtime.Serialization;

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Base Aeron exception for catching all Aeron specific errors.
    /// </summary>
    public class AeronException : Exception
    {
        /// <summary>
        /// <seealso cref="Exceptions.Category"/> of the exception to help the client decide how they should proceed.
        /// </summary>
        public Category Category { get; }

        /// <summary>
        /// Default Aeron exception as <seealso cref="Exceptions.Category.ERROR"/>
        /// </summary>
        public AeronException()
        {
            Category = Category.ERROR;
        }

        /// <summary>
        /// Default Aeron exception with provided <seealso cref="Exceptions.Category"/>.
        /// </summary>
        /// <param name="category"> of this exception. </param>
        public AeronException(Category category)
        {
            Category = category;
        }

        public AeronException(Exception cause) : base(cause?.ToString(), cause)
        {
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

        /// <summary>
        /// Determines if a <seealso cref="System.Exception"/> is a <seealso cref="Category.FATAL"/> if an <seealso cref="AeronException"/>.
        /// </summary>
        /// <param name="t"> throwable to check if fatal. </param>
        /// <returns> true if this is an AeronException with a category set to <seealso cref="Category.FATAL"/>,
        /// false otherwise. </returns>
        public static bool IsFatal(in Exception t)
        {
            return t is AeronException exception && Category.FATAL == exception.Category;
        }
    }
}