using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Aeron.Samples.ClusterClient
{
    internal class MessageListener : IEgressListener
    {
        public void OnMessage(long clusterSessionId, long timestampMs, IDirectBuffer buffer, int offset, int length, Header header)
        {
            Console.WriteLine($"OnMessage: sessionId={clusterSessionId}, timestamp={timestampMs}, length={length}");

            Console.WriteLine("Received Message: " + buffer.GetStringWithoutLengthUtf8(offset, length));
        }

        public void OnSessionEvent(long correlationId, long clusterSessionId, long leadershipTermId, int leaderMemberId, EventCode code, string detail)
        {
            Console.WriteLine($"Session Event:  leadershipTermId={leadershipTermId}, leaderMemberId={leaderMemberId}, code={code}, detail={detail}");
        }

        public void OnNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, string memberEndpoints)
        {
            Console.WriteLine($"New Leader:  leadershipTermId={leadershipTermId}, leaderMemberId={leaderMemberId}, memberEndpoints={memberEndpoints}");
        }

        public void OnAdminResponse(long clusterSessionId, long correlationId, AdminRequestType requestType,
            AdminResponseCode responseCode, string message, IDirectBuffer payload, int payloadOffset, int payloadLength)
        {
            Console.WriteLine($"OnAdminResponse:  clusterSessionId={clusterSessionId}, correlationId={correlationId}, requestType={requestType}, responseCode={responseCode}");
        }
    }
}