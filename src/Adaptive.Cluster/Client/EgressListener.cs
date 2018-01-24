using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    public interface IEgressListener
    {
        void SessionEvent(
            long correlationId,
            long clusterSessionId,
            EventCode code,
            string detail);

        void NewLeader(
            long correlationId,
            long clusterSessionId,
            long lastMessageTimestamp,
            long leadershipTimestamp,
            long leadershipTermId,
            int leaderMemberId,
            string memberEndpoints);

        void OnMessage(
            long correlationId,
            long clusterSessionId,
            long timestamp,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header);
    }
}