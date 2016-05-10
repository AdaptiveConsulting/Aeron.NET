using System;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.BasicSubscriber
{
    /// <summary>
    /// This is a Basic Aeron subscriber application
    /// The application subscribes to a default channel and stream ID.  These defaults can
    /// be overwritten by changing their value in <seealso cref="SampleConfiguration"/> or by
    /// setting their corresponding Java system properties at the command line, e.g.:
    /// -Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20
    /// This application only handles non-fragmented data. A DataHandler method is called
    /// for every received message or message fragment.
    /// For an example that implements reassembly of large, fragmented messages, see
    /// {link@ MultipleSubscribersWithFragmentAssembly}.
    /// </summary>
    public class BasicSubscriber
    {
        private static readonly int STREAM_ID = SampleConfiguration.STREAM_ID;
        private static readonly string CHANNEL = SampleConfiguration.CHANNEL;
        private static readonly int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        public static void Main(string[] args)
        {
            Console.WriteLine("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

            var ctx = new Aeron.Context()
                .AvailableImageHandler(SamplesUtil.PrintAvailableImage)
                .UnavailableImageHandler(SamplesUtil.PrintUnavailableImage);


            var fragmentHandler = SamplesUtil.PrintStringMessage(STREAM_ID);
            var running = new AtomicBoolean(true);

            // Register a SIGINT handler for graceful shutdown.
            Console.CancelKeyPress += (s, e) => running.Set(false);

            // Create an Aeron instance using the configured Context and create a
            // Subscription on that instance that subscribes to the configured
            // channel and stream ID.
            // The Aeron and Subscription classes implement "AutoCloseable" and will automatically
            // clean up resources when this try block is finished
            using (var aeron = Aeron.Connect(ctx))
            using (var subscription = aeron.AddSubscription(CHANNEL, STREAM_ID))
            {
                SamplesUtil.SubscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running)(subscription);
                Console.WriteLine("Shutting down...");
            }

            Console.WriteLine();
        }
    }
}