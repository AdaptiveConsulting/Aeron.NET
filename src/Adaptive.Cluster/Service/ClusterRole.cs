namespace Adaptive.Cluster.Service
{
    public enum ClusterRole : long
    {
        /// <summary>
        /// The cluster node is a candidate in a cluster election.
        /// </summary>
        Candidate = 0,

        /// <summary>
        /// The cluster node is the leader of the current leadership term.
        /// </summary>
        Leader = 1,

        /// <summary>
        /// The cluster node is a follower of the current leader.
        /// </summary>
        Follower = 2
    }
}