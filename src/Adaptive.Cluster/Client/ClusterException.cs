using System;
using System.Runtime.Serialization;
using Adaptive.Aeron.Exceptions;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Exceptions specific to Cluster operation. 
    /// </summary>
    public class ClusterException : AeronException
    {
        public ClusterException()
        {
        }

        public ClusterException(string message, Category category) : base(message, category)
        {
        }

        protected ClusterException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public ClusterException(string message) : base(message)
        {
        }

        public ClusterException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}