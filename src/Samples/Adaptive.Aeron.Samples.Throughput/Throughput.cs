using System;
using System.Threading;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.Throughput
{
    public class Throughput
    {
        private static readonly string Channel = SampleConfiguration.CHANNEL;
        private static readonly int StreamID = SampleConfiguration.STREAM_ID;
        private static readonly int MessageLength = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly long NumberOfMessages = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly long LingerTimeoutMs = SampleConfiguration.LINGER_TIMEOUT_MS;
        private static readonly int FragmentCountLimit = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        private static readonly BusySpinIdleStrategy OfferIdleStrategy = new BusySpinIdleStrategy();

        private static volatile bool _printingActive = true;

        public static void Main()
        {
            ComputerSpecifications.Dump();


            var reporter = new RateReporter(1000, PrintRate);
            var rateReporterHandler = SamplesUtil.RateReporterHandler(reporter);
            var context = new Aeron.Context();

            var running = new AtomicBoolean(true);

            var reportThread = new Thread(reporter.Run);
            var subscribeThread = new Thread(subscription => SamplesUtil.SubscriberLoop(rateReporterHandler, FragmentCountLimit, running)((Subscription) subscription));

            using (var aeron = Aeron.Connect(context))
            using (var publication = aeron.AddPublication(Channel, StreamID))
            using (var subscription = aeron.AddSubscription(Channel, StreamID))
            using (var buffer = new UnsafeBuffer(new byte[MessageLength]))
            {
                reportThread.Start();
                subscribeThread.Start(subscription);

                do
                {
                    Console.WriteLine("Streaming {0:G} messages of size {1:G} bytes to {2} on stream Id {3}", NumberOfMessages, MessageLength, Channel, StreamID);

                    _printingActive = true;

                    long backPressureCount = 0;
                    for (long i = 0; i < NumberOfMessages; i++)
                    {
                        buffer.PutLong(0, i);

                        OfferIdleStrategy.Reset();
                        while (publication.Offer(buffer, 0, buffer.Capacity) < 0)
                        {
                            OfferIdleStrategy.Idle();
                            backPressureCount++;
                        }
                    }

                    Console.WriteLine("Done streaming. backPressureRatio=" + (double) backPressureCount/NumberOfMessages);

                    if (0 < LingerTimeoutMs)
                    {
                        Console.WriteLine("Lingering for " + LingerTimeoutMs + " milliseconds...");
                        Thread.Sleep((int) LingerTimeoutMs);
                    }

                    _printingActive = false;
                } while (Console.ReadLine() != "x");

                reporter.Halt();
                running.Set(false);

                if (!subscribeThread.Join(5000))
                {
                    Console.WriteLine("Warning: not all tasks completed promptly");
                }
            }
        }

        public static void PrintRate(double messagesPerSec, double bytesPerSec, long totalFragments, long totalBytes)
        {
            if (_printingActive)
            {
                Console.WriteLine("{0:#,0} msgs/sec, {1} bytes/sec, totals {2} messages {3} MB", messagesPerSec, bytesPerSec, totalFragments, totalBytes/(1024*1024));
            }
        }
    }
}