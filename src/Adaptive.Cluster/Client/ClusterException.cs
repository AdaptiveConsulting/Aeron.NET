using Adaptive.Aeron.Exceptions;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Exceptions specific to Cluster operation. 
    /// </summary>
    public class ClusterException : AeronException
    {
        /// <summary>
        /// Cluster exception with provided message and <seealso cref="Category.ERROR"/>.
        /// </summary>
        /// <param name="message"> to detail the exception. </param>
        public ClusterException(string message) : base(message)
        {
        }

        /// <summary>
        /// Cluster exception with a detailed message and provided <seealso cref="Category"/>.
        /// </summary>
        /// <param name="message">  providing detail on the error. </param>
        /// <param name="category"> of the exception. </param>
        public ClusterException(string message, Category category) : base(message, category)
        {
        }
    }
}