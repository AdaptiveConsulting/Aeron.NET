using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;

namespace Adaptive.Aeron.Samples.ClusterClient
{
    class Program
    {
        static void Main()
        {
            var ctx = new AeronCluster.Context()
                .EgressListener(new MessageListener());
            
            using (var c = AeronCluster.Connect(ctx))
            {
                var idleStrategy = ctx.IdleStrategy();
                var msgBuffer = new UnsafeBuffer(new byte[100]);
                var len = msgBuffer.PutStringWithoutLengthUtf8(0, "Hello World!");

                while (c.Offer(msgBuffer, 0, len) < 0)
                {
                    idleStrategy.Idle();
                }

                while (c.PollEgress() <= 0)
                {
                    idleStrategy.Idle();
                }
            }
        }
    }
}