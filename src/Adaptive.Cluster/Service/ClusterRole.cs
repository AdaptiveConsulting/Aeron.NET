namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Role of the node in the cluster.
    /// </summary>
    public enum ClusterRole : long
    {
        /// <summary>
        /// The cluster node is a follower in the current leadership term.
        /// </summary>
        Follower = 0,
        
        /// <summary>
        /// The cluster node is a candidate to become a leader in an election.
        /// </summary>
        Candidate = 1,

        /// <summary>
        /// The cluster node is the leader for the current leadership term.
        /// </summary>
        Leader = 2
    }
}