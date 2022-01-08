using Adaptive.Agrona.Concurrent;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Used to terminate the <seealso cref="IAgent"/> within a cluster in an expected fashion.
    /// </summary>
    public class ClusterTerminationException : AgentTerminationException
    {
    }
}