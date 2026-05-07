using System;
using System.Text;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class SystemTest
    {
        private EmbeddedMediaDriver _driver;

        [SetUp]
        public void StartDriver() => _driver = new EmbeddedMediaDriver();

        [TearDown]
        public void StopDriver() => _driver?.Dispose();

        [Test]
        public void BasicMessageTest()
        {
            using var aeron = Aeron.Connect();
            var publication = aeron.AddPublication("aeron:ipc", 1);
            var subscription = aeron.AddSubscription("aeron:ipc", 1);
            Await(() => publication.IsConnected);

            var testBytes = Encoding.ASCII.GetBytes("Hello World!");
            var buffer = new UnsafeBuffer(testBytes);
            Await(() => publication.Offer(buffer, 0, buffer.Capacity) > 0);

            bool messageReceived = false;

            void FragmentHandler(IDirectBuffer directBuffer, int offset, int length, Header header)
            {
                if ("Hello World!" == directBuffer.GetStringWithoutLengthAscii(offset, length))
                {
                    messageReceived = true;
                }
            }

            Await(() =>
            {
                subscription.Poll(FragmentHandler, 10);
                return messageReceived;
            });
        }

        private static void Await(Func<bool> predicate)
        {
            var clock = new SystemEpochClock();
            var deadline = clock.Time() + 15_000L;

            while (!predicate())
            {
                if (deadline < clock.Time())
                {
                    throw new TimeoutException();
                }

                Thread.Sleep(10);
            }
        }
    }
}
