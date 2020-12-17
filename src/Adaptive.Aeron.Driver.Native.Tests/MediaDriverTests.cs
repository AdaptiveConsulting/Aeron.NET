using System;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;
using static Adaptive.Aeron.Driver.Native.Tests.DriverContextUtil;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    [TestFixture]
    public class MediaDriverTests
    {
        [Test]
        public void RunManyMediaDrivers()
        {
            var driverCount = 4;
            AeronDriver.DriverContext[] driverContexts = new AeronDriver.DriverContext[driverCount];
            AeronDriver[] aeronDrivers = new AeronDriver[driverCount];
            Subscription[] subs = new Subscription[driverCount];
            Publication[] pubs = new Publication[driverCount];

            for (int i = 0; i < driverCount; i++)
            {
                var driverCtx = CreateDriverCtx();
                driverContexts[i] = driverCtx;
                var md = AeronDriver.Start(driverCtx);
                aeronDrivers[i] = md;
            }

            for (int i = 1; i < driverCount; i++)
            {
                var i1 = i;
                int port = 44500 + i;
                subs[i] = aeronDrivers[0].AddSubscription($"aeron:udp?endpoint={"127.0.0.1"}:{port}", 1,
                    image => { Console.WriteLine($"Available image {i1}"); },
                    image => { Console.WriteLine($"Unavailable image {i1}"); });
            }

            for (int i = 1; i < driverCount; i++)
            {
                int port = 44500 + i;
                pubs[i] = aeronDrivers[i].AddPublication($"aeron:udp?endpoint={"127.0.0.1"}:{port}", 1);
            }

            for (int i = 1; i < driverCount; i++)
            {
                var ub = new UnsafeBuffer(new byte[10], 0, 10);
                var c = 0;
                while (pubs[i].Offer(ub) < 0 && ++c < 1_000_000)
                {
                    Thread.Sleep(0);
                }
                Assert.AreNotEqual(1_000_000, c, "Could Offer");

                Console.WriteLine($"Offered to {i}");
            }


            for (int i = 1; i < driverCount; i++)
            {
                void FragmentHandler(IDirectBuffer buffer, int offset, int length, Header header)
                {
                    Console.WriteLine($"Received on {i}");
                }

                var c = 0;
                while (subs[i].Poll(FragmentHandler, 64) <= 0 && ++c < 1_000_000)
                {
                    Thread.Sleep(0);
                }
                Assert.AreNotEqual(1_000_000, c, "Could Poll");
            }

            for (int i = 0; i < driverCount; i++)
            {
                subs[i]?.Dispose();
                pubs[i]?.Dispose();
                aeronDrivers[i].Dispose();
                Assert.IsFalse(AeronDriver.IsDriverActive(driverContexts[i].Ctx.AeronDirectoryName(), driverContexts[i].Ctx.DriverTimeoutMs()));
            }
        }
    }
}