using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;

namespace Adaptive.Aeron.Samples.ClusterClient
{
    internal class MessageListener : IEgressMessageListener
    {
        public void OnMessage(long correlationId, long clusterSessionId, long timestampMs, IDirectBuffer buffer,
            int offset, int length, Header header)
        {
            Console.WriteLine($"OnMessage: sessionId={clusterSessionId}, timestamp={timestampMs}, correlationId={correlationId}, length={length}");

            Console.WriteLine("Received Message: " + buffer.GetStringWithoutLengthUtf8(offset, length));
        }
    }
}