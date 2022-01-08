namespace Adaptive.Cluster.Service
{
    public static class ClientSessionConstants
    {
        /// <summary>
        /// Return value to indicate egress to a session is mocked out by the cluster when in follower mode.
        /// </summary>
        public const long MOCKED_OFFER = 1;
    }
}