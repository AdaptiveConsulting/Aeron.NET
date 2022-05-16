using Adaptive.Agrona.Concurrent;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Used to terminate the <seealso cref="IAgent"/> within a cluster in an expected fashion.
    /// </summary>
    public class ClusterTerminationException : AgentTerminationException
    {
        private readonly bool _isExpected;

        /// <summary>
        /// Construct an exception used to terminate the cluster with {@link #isExpected()} set to true.
        /// </summary>
        public ClusterTerminationException() : this(true)
        {
        }

        /// <summary>
        /// Construct an exception used to terminate the cluster.
        /// </summary>
        /// <param name="isExpected"> true if the termination is expected, i.e. it was requested. </param>
        public ClusterTerminationException(bool isExpected) : base(isExpected ? "expected termination" : "unexpected termination")
        {
            _isExpected = isExpected;
        }
        
        /// <summary>
        /// Whether the termination is expected.
        /// </summary>
        /// <returns> true if expected otherwise false. </returns>
        public bool Expected
        {
            get
            {
                return _isExpected;
            }
        }

    }
}