namespace Adaptive.Cluster.Service
{
    public enum ClusterRole : long
    {
        /// <summary>
        /// The cluster node is a follower of the current leader.
        /// </summary>
        Follower = 0,
        
        /// <summary>
        /// The cluster node is a candidate to become a leader in an election.
        /// </summary>
        Candidate = 1,

        /// <summary>
        /// The cluster node is the leader of the current leadership term.
        /// </summary>
        Leader = 2
    }
}