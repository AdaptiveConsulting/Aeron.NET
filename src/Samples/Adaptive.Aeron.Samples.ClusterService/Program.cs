using System;
using Adaptive.Cluster.Service;

namespace Adaptive.Aeron.Samples.ClusterService
{
    class Program
    {
        static void Main(string[] args)
        {
            var context = new ClusteredServiceContainer.Context()
                .ClusteredService(new EchoService());
            
            using (ClusteredServiceContainer.Launch(context))
            {
                Console.WriteLine("Started Service Container...");
                
                context.ShutdownSignalBarrier().Await();
                
                Console.WriteLine("Stopping Service Container...");
            }
            
            Console.WriteLine("Stopped.");
        }
    }
}