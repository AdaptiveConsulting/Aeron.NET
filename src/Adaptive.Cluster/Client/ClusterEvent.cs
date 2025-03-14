using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona.Concurrent.Errors;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// A means to capture a Cluster event of significance that does not require a stack trace, so it can be lighter-weight
    /// and take up less space in a <seealso cref="DistinctErrorLog"/>.
    /// </summary>
    public class ClusterEvent : AeronEvent
    {
        public ClusterEvent(string message) : base(message)
        {
        }

        public ClusterEvent(string message, Category category) : base(message, category)
        {
        }
    }
}