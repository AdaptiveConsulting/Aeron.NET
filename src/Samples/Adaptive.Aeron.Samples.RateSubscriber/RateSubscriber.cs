using System;
using System.Threading;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.RateSubscriber
{
    /// <summary>
    /// Example that displays current rate while receiving data
    /// </summary>
    public class RateSubscriber
    {
        private static readonly int STREAM_ID = SampleConfiguration.STREAM_ID;
        private static readonly string CHANNEL = Aeron.Context.IPC_CHANNEL;
        private static readonly int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        public static void Main(string[] args)
        {
            Console.WriteLine("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

            var ctx = new Aeron.Context()
                .AvailableImageHandler(SamplesUtil.PrintUnavailableImage)
                .UnavailableImageHandler(SamplesUtil.PrintUnavailableImage);


            var reporter = new RateReporter(1000, SamplesUtil.PrintRate);
            var fragmentAssembler = new FragmentAssembler(SamplesUtil.RateReporterHandler(reporter));
            var running = new AtomicBoolean(true);

            var t = new Thread(subscription => SamplesUtil.SubscriberLoop(fragmentAssembler.OnFragment, FRAGMENT_COUNT_LIMIT, running)((Subscription) subscription));
            var report = new Thread(reporter.Run);


            using (var aeron = Aeron.Connect(ctx))
            using (var subscription = aeron.AddSubscription(CHANNEL, STREAM_ID))
            {
                t.Start(subscription);
                report.Start();

                Console.ReadLine();
                Console.WriteLine("Shutting down...");
                running.Set(false);
            }

            reporter.Halt();


            if (!t.Join(5000))
            {
                Console.WriteLine("Warning: not all tasks completed promptly");
            }
        }
    }
}