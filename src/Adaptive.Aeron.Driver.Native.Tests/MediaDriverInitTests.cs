using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using static Adaptive.Aeron.Driver.Native.Tests.DriverContextUtil;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    [TestFixture]
    public class MediaDriverInitTests
    {
        [Test]
        [NonParallelizable]
        public void CouldStartAndStopMediaDriverEmbedded()
        {
            var driverCtx = CreateDriverCtx();

            var md = AeronDriver.Start(driverCtx);

            Assert.IsTrue(AeronDriver.IsDriverActive(driverCtx.AeronDirectoryName()));

            md.Dispose();

            Assert.IsFalse(AeronDriver.IsDriverActive(driverCtx.AeronDirectoryName(), driverCtx.DriverTimeoutMs()));
            Assert.IsTrue(driverCtx.DirDeleteOnShutdown(), "Should delete dir on shutdown setting is on");
            Assert.IsFalse(Directory.Exists(driverCtx.AeronDirectoryName()),
                $"Dir exists [{Directory.Exists(driverCtx.AeronDirectoryName())}]: {driverCtx.AeronDirectoryName()}");
        }

        [Test]
        [NonParallelizable]
        public void CouldReusePort()
        {
            int port;
            using (var udpc = new UdpClient(0))
            {
                port = ((IPEndPoint) udpc.Client.LocalEndPoint).Port;
                Assert.IsTrue(port > 0);
                udpc.Close();
            }

            var driverCtx = CreateDriverCtx();
            var md = AeronDriver.Start(driverCtx);

            using var aeron = Aeron.Connect(new Aeron.Context().AeronDirectoryName(driverCtx.AeronDirectoryName()));
            using var _ = aeron.AddSubscription($"aeron:udp?endpoint=0.0.0.0:{port}", 1);
        }
    }
}