using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Interface for consuming messages coming from the cluster that also include administrative events.
    /// </summary>
    public interface IEgressListener : ISessionMessageListener
    {
        void SessionEvent(long correlationId, long clusterSessionId, int leaderMemberId, EventCode code, string detail);
        
        void NewLeader(long clusterSessionId, int leaderMemberId, string memberEndpoints);
    }
}