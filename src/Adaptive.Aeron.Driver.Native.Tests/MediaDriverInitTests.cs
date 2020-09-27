using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using static Adaptive.Aeron.Driver.Native.Tests.DriverConfigUtil;

namespace Adaptive.Aeron.Driver.Native.Tests
{
    [TestFixture]
    public class MediaDriverInitTests
    {
        [Test]
        public void CouldStartAndStopMediaDriverEmbedded()
        {
            var config = CreateMediaDriverConfig();
            config.DirDeleteOnStart = true;
            config.DirDeleteOnShutdown = true;

            var md = MediaDriver.Start(config);

            Assert.IsTrue(MediaDriver.IsDriverActive(config.Dir));

            var counter = md.GetNewClientStreamId();

            Assert.AreEqual(counter + 1, md.GetNewClientStreamId());

            Console.WriteLine($"Counter: {counter + 1}");

            md.Dispose();

            Assert.IsFalse(MediaDriver.IsDriverActive(config.Dir, config.DriverTimeout));

            Assert.IsTrue(config.DirDeleteOnShutdown, "Should delete dir on shutdown setting is on");
            // TODO when https://github.com/real-logic/aeron/issues/1108 is fixed,
            // this should print false and we should assert that.
            Console.WriteLine($"Dir exists [{Directory.Exists(config.Dir)}]: {config.Dir}");
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

            var config = CreateMediaDriverConfig();
            using var c = new AeronConnection(config);

            using var _ = c.Aeron.AddSubscription($"aeron:udp?endpoint=0.0.0.0:{port}", 1);
        }
    }
}