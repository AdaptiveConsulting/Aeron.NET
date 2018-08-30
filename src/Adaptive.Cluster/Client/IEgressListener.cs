using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages coming from the cluster that also include administrative events.
    /// </summary>
    public interface IEgressListener : IEgressMessageListener
    {
        void SessionEvent(
            long correlationId,
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            EventCode code,
            string detail);
        
        void NewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, string memberEndpoints);
    }
}