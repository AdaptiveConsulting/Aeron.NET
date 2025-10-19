using System;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Service;

namespace Adaptive.Aeron.Samples.ClusterService
{
    class Program
    {
        static void Main(string[] args)
        {
            var context = new ClusteredServiceContainer.Context()
                .ClusteredService(new EchoService());

            using (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier())
            using (ClusteredServiceContainer.Launch(context.TerminationHook(() => barrier.SignalAll())))
            {
                Console.WriteLine("Started Service Container...");

                barrier.Await();

                Console.WriteLine("Stopping Service Container...");
            }

            Console.WriteLine("Stopped.");
        }
    }
}