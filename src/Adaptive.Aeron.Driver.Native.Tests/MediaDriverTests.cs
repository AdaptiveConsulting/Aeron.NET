using System;
using System.Threading;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;
using static Adaptive.Aeron.Driver.Native.Tests.DriverConfigUtil;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    [TestFixture]
    public class MediaDriverTests
    {
        [Test]
        public void RunManyMediaDrivers()
        {
            var driverCount = 4;
            MediaDriverConfig[] configs = new MediaDriverConfig[driverCount];
            AeronConnection[] connections = new AeronConnection[driverCount];
            Subscription[] subs = new Subscription[driverCount];
            Publication[] pubs = new Publication[driverCount];

            for (int i = 0; i < driverCount; i++)
            {
                var config = CreateMediaDriverConfig();
                config.DirDeleteOnStart = true;
                config.DirDeleteOnShutdown = true;

                configs[i] = config;
                var ac = new AeronConnection(config);
                connections[i] = ac;
            }

            for (int i = 1; i < driverCount; i++)
            {
                var i1 = i;
                subs[i] = connections[0].Aeron.AddSubscription(AeronUtils.RemoteChannel("127.0.0.1", 44500 + i), 1);
                connections[0].ImageAvailable += image => { Console.WriteLine($"Available image {i1}"); };
            }

            for (int i = 1; i < driverCount; i++)
            {
                pubs[i] = connections[i].Aeron.AddPublication(AeronUtils.RemoteChannel("127.0.0.1", 44500 + i), 1);
            }

            for (int i = 1; i < driverCount; i++)
            {
                var ub = new UnsafeBuffer(new byte[10], 0, 10);
                while (pubs[i].Offer(ub) < 0)
                {
                    Thread.Sleep(0);
                }

                Console.WriteLine($"Offered to {i}");
            }


            for (int i = 1; i < driverCount; i++)
            {
                while (subs[i].Poll((buffer, offset, length, header) => { Console.WriteLine($"Received on {i}"); },
                    64) <= 0)
                {
                    Thread.Sleep(0);
                }
            }

            for (int i = 0; i < driverCount; i++)
            {
                subs[i]?.Dispose();
                pubs[i]?.Dispose();
                connections[i].Dispose();
                Assert.IsFalse(MediaDriver.IsDriverActive(configs[i].Dir, configs[i].DriverTimeout));
            }
        }
    }
}