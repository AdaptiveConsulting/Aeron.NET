using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;
using Adaptive.Cluster.Service;

namespace Adaptive.Aeron.Samples.ClusterService
{
    public class EchoService : IClusteredService
    {
        private ICluster _cluster;

        public void OnStart(ICluster cluster, Image snapshotImage)
        {
            Console.WriteLine("OnStart");
            _cluster = cluster;
        }

        public void OnSessionOpen(ClientSession session, long timestampMs)
        {
            Console.WriteLine($"OnSessionOpen: sessionId={session.Id}, timestamp={timestampMs}");
        }

        public void OnSessionClose(ClientSession session, long timestampMs, CloseReason closeReason)
        {
            Console.WriteLine($"OnSessionClose: sessionId={session.Id}, timestamp={timestampMs}");
        }

        public void OnSessionMessage(ClientSession session, long timestampMs, IDirectBuffer buffer, int offset, int length, Header header)
        {
            Console.WriteLine($"OnSessionMessage: sessionId={session.Id}, timestamp={timestampMs}, length={length}");

            Console.WriteLine("Received Message: " + buffer.GetStringWithoutLengthUtf8(offset, length));
            
            while (session.Offer(buffer, offset, length) <= 0)
            {
                _cluster.Idle();
            }
        }

        public void OnTimerEvent(long correlationId, long timestampMs)
        {
            Console.WriteLine($"OnTimerEvent: correlationId={correlationId}, timestamp={timestampMs}");
        }

        public void OnTakeSnapshot(Publication snapshotPublication)
        {
            Console.WriteLine("OnTakeSnapshot");
        }

        public void OnLoadSnapshot(Image snapshotImage)
        {
            Console.WriteLine("OnLoadSnapshot");
        }

        public void OnRoleChange(ClusterRole newRole)
        {
            Console.WriteLine($"OnRoleChange: newRole={newRole}");
        }

        public void OnTerminate(ICluster cluster)
        {
            Console.WriteLine("OnTerminate");
        }
    }
}