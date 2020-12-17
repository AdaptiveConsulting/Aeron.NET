using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;
using static Adaptive.Aeron.Driver.Native.Tests.DriverContextUtil;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    [TestFixture]
    public class MediaDriverInitTests
    {
        [Test]
        public void CouldStartAndStopMediaDriverEmbedded()
        {
            var driverCtx = CreateDriverCtx();
            
            var md = AeronDriver.Start(driverCtx);

            Assert.IsTrue(AeronDriver.IsDriverActive(driverCtx.Ctx.AeronDirectoryName()));
            
            md.Dispose();

            Assert.IsFalse(AeronDriver.IsDriverActive(driverCtx.Ctx.AeronDirectoryName(), driverCtx.Ctx.DriverTimeoutMs()));
            Assert.IsTrue(driverCtx.DirDeleteOnShutdown(), "Should delete dir on shutdown setting is on");
            Assert.IsFalse(Directory.Exists(driverCtx.Ctx.AeronDirectoryName()), $"Dir exists [{Directory.Exists(driverCtx.Ctx.AeronDirectoryName())}]: {driverCtx.Ctx.AeronDirectoryName()}");
        }

        [Test]
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
            
            using var _ = md.AddSubscription($"aeron:udp?endpoint=0.0.0.0:{port}", 1);
        }
    }
}