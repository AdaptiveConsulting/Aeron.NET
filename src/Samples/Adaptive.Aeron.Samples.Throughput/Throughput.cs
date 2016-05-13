using System;
using System.Threading;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.Throughput
{
    public class Throughput
    {
        private static readonly string CHANNEL = SampleConfiguration.CHANNEL;
        private static readonly int STREAM_ID = SampleConfiguration.STREAM_ID;
        private static readonly int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;
        private static readonly int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        private static readonly UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
        private static readonly BusySpinIdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();

        private static volatile bool PrintingActive = true;

        public static void Main(string[] args)
        {
            var reporter = new RateReporter(1000, PrintRate);
            var rateReporterHandler = SamplesUtil.RateReporterHandler(reporter);
            var context = new Aeron.Context();

            var running = new AtomicBoolean(true);

            var reportThread = new Thread(reporter.Run);
            var subscribeThread = new Thread(subscription => SamplesUtil.SubscriberLoop(rateReporterHandler, FRAGMENT_COUNT_LIMIT, running)((Subscription) subscription));

            using (var aeron = Aeron.Connect(context))
            using (var publication = aeron.AddPublication(CHANNEL, STREAM_ID))
            using (var subscription = aeron.AddSubscription(CHANNEL, STREAM_ID))
            {
                reportThread.Start();
                subscribeThread.Start(subscription);

                do
                {
                    Console.WriteLine("Streaming {0:G} messages of size {1:G} bytes to {2} on stream Id {3}", NUMBER_OF_MESSAGES, MESSAGE_LENGTH, CHANNEL, STREAM_ID);

                    PrintingActive = true;

                    long backPressureCount = 0;
                    for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
                    {
                        ATOMIC_BUFFER.PutLong(0, i);

                        OFFER_IDLE_STRATEGY.Reset();
                        while (publication.Offer(ATOMIC_BUFFER, 0, ATOMIC_BUFFER.Capacity) < 0)
                        {
                            OFFER_IDLE_STRATEGY.Idle();
                            backPressureCount++;
                        }
                    }

                    Console.WriteLine("Done streaming. backPressureRatio=" + (double) backPressureCount/NUMBER_OF_MESSAGES);

                    if (0 < LINGER_TIMEOUT_MS)
                    {
                        Console.WriteLine("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                        Thread.Sleep((int) LINGER_TIMEOUT_MS);
                    }

                    PrintingActive = false;
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
            if (PrintingActive)
            {
                Console.WriteLine("{0:#,0} msgs/sec, {1} bytes/sec, totals {2} messages {3} MB", messagesPerSec, bytesPerSec, totalFragments, totalBytes/(1024*1024));
            }
        }
    }
}