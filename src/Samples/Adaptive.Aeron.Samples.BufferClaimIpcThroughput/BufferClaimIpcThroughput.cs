using System;
using System.Diagnostics;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.BufferClaimIpcThroughput
{
    public class BufferClaimIpcThroughput
    {
        public const int BURST_LENGTH = 1000000;
        public static readonly int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
        public static readonly int MESSAGE_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
        public static readonly string CHANNEL = Aeron.Context.IPC_CHANNEL;
        public static readonly int STREAM_ID = SampleConfiguration.STREAM_ID;

        public static void Main()
        {
            var running = new AtomicBoolean(true);
            Console.CancelKeyPress += (_, e) => running.Set(false);

            using (var aeron = Aeron.Connect())
            using (var publication = aeron.AddPublication(CHANNEL, STREAM_ID))
            using (var subscription = aeron.AddSubscription(CHANNEL, STREAM_ID))
            {
                var subscriber = new Subscriber(running, subscription);
                var subscriberThread = new Thread(subscriber.Run) {Name = "subscriber"};
                var publisherThread = new Thread(new Publisher(running, publication).Run) {Name = "publisher"};
                var rateReporterThread = new Thread(new RateReporter(running, subscriber).Run) {Name = "rate-reporter"};

                rateReporterThread.Start();
                subscriberThread.Start();
                publisherThread.Start();

                subscriberThread.Join();
                publisherThread.Join();
                rateReporterThread.Join();
            }
        }

        public sealed class RateReporter
        {
            internal readonly AtomicBoolean Running;
            internal readonly Subscriber Subscriber;
            private readonly Stopwatch _stopwatch;

            public RateReporter(AtomicBoolean running, Subscriber subscriber)
            {
                Running = running;
                Subscriber = subscriber;
                _stopwatch = Stopwatch.StartNew();
            }

            public void Run()
            {
                long lastTotalBytes = Subscriber.TotalBytes();

                while (Running.Get())
                {
                    Thread.Sleep(1000);

                    var newTotalBytes = Subscriber.TotalBytes();
                    var duration = _stopwatch.ElapsedMilliseconds;
                    var bytesTransferred = newTotalBytes - lastTotalBytes;
                    Console.WriteLine($"Duration {duration}ms - {bytesTransferred / MESSAGE_LENGTH} messages - {bytesTransferred} bytes");

                    _stopwatch.Restart();
                    lastTotalBytes = newTotalBytes;
                }
            }
        }

        public sealed class Publisher
        {
            internal readonly AtomicBoolean Running;
            internal readonly Publication Publication;

            public Publisher(AtomicBoolean running, Publication publication)
            {
                Running = running;
                Publication = publication;
            }

            public void Run()
            {
                var publication = Publication;
                var bufferClaim = new BufferClaim();
                long backPressureCount = 0;
                long totalMessageCount = 0;

                while (Running.Get())
                {
                    for (var i = 0; i < BURST_LENGTH; i++)
                    {
                        while (publication.TryClaim(MESSAGE_LENGTH, bufferClaim) <= 0)
                        {
                            ++backPressureCount;
                            if (!Running.Get())
                            {
                                break;
                            }
                        }

                        var offset = bufferClaim.Offset;
                        bufferClaim.Buffer.PutInt(offset, i); // Example field write
                                                                // Real app would write whatever fields are required via a flyweight like SBE

                        bufferClaim.Commit();

                        ++totalMessageCount;
                    }
                }

                
                var backPressureRatio = backPressureCount / (double)totalMessageCount;
                Console.WriteLine($"Publisher back pressure ratio: {backPressureRatio}");
            }
        }

        public sealed class Subscriber
        {
            internal readonly AtomicBoolean Running;
            internal readonly Subscription Subscription;
            private readonly AtomicLong _totalBytes = new AtomicLong();

            public Subscriber(AtomicBoolean running, Subscription subscription)
            {
                Running = running;
                Subscription = subscription;
            }

            public long TotalBytes()
            {
                return _totalBytes.Get();
            }

            public void Run()
            {
                while (Subscription.ImageCount == 0)
                {
                    // wait for an image to be ready
                    Thread.Yield();
                }

                var image = Subscription.Images[0];

                long failedPolls = 0;
                long successfulPolls = 0;

                while (Running.Get())
                {
                    var fragmentsRead = image.Poll(OnFragment, MESSAGE_COUNT_LIMIT);
                    if (0 == fragmentsRead)
                    {
                        ++failedPolls;
                    }
                    else
                    {
                        ++successfulPolls;
                    }
                }

                var failureRatio = failedPolls / (double)(successfulPolls + failedPolls);
                Console.WriteLine($"Subscriber poll failure ratio: {failureRatio}");
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _totalBytes.Add(length);
            }
        }
    }
}
