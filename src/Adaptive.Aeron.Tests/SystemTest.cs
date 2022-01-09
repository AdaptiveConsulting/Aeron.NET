using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        [Test]
        public void BasicMessageTest()
        {
            var rootDir = GetSolutionDirectory(TestContext.CurrentContext.TestDirectory).Parent;

            if (rootDir == null)
            {
                throw new FileNotFoundException("could not find root directory of project");
            }

            var jarPath = Path.Combine(rootDir.ToString(), "driver", "media-driver.jar");

            var psi = new ProcessStartInfo
            {
                FileName = "java",
                Arguments =
                    $"-cp \"{jarPath}\" -Daeron.driver.termination.validator=io.aeron.driver.DefaultAllowTerminationValidator io.aeron.driver.MediaDriver"
            };

            var driver = Process.Start(psi);

            try
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
            finally
            {
                var token = new UnsafeBuffer(Array.Empty<byte>());
                Aeron.Context.RequestDriverTermination(
                    new DirectoryInfo(Aeron.Context.GetAeronDirectoryName()),
                    token,
                    0,
                    0
                );

                driver.WaitForExit();
            }
        }

        private void Await(Func<bool> predicate)
        {
            var clock = new SystemEpochClock();
            var deadline = clock.Time() + 5000L;

            while (!predicate())
            {
                if (deadline < clock.Time())
                {
                    throw new TimeoutException();
                }

                Thread.Sleep(10);
            }
        }

        private static DirectoryInfo GetSolutionDirectory(string currentPath = null)
        {
            var directory = new DirectoryInfo(currentPath ?? Directory.GetCurrentDirectory());
            while (directory != null && !directory.GetFiles("*.sln").Any())
            {
                directory = directory.Parent;
            }

            return directory;
        }
    }
}