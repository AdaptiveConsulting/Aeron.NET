using System;
using System.Diagnostics;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using HdrHistogram;

namespace Adaptive.Aeron.Samples.Ping
{
    public class Ping
    {
        private static string PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
        private static string PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;
        private static int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
        private static int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
        private static int NUMBER_OF_MESSAGES = 100;
        private static int WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
        private static int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
        private static int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
        private static int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        private static UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
        private static LongHistogram HISTOGRAM = new LongHistogram(10000000000, 3);
        private static CountdownEvent LATCH = new CountdownEvent(1);
        private static IIdleStrategy POLLING_IDLE_STRATEGY = new SpinWaitIdleStrategy();

        public static void Main(String[] args)
        {
            if (args == null) throw new ArgumentNullException(nameof(args));
            Aeron.Context ctx = new Aeron.Context().AvailableImageHandler(availablePongImageHandler);
            IFragmentHandler fragmentHandler = new FragmentAssembler(new DelegateFragmentHandler(pongHandler));
            Console.WriteLine("Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
            Console.WriteLine("Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
            Console.WriteLine("Message length of " + MESSAGE_LENGTH + " bytes");

            using (Aeron aeron = Aeron.Connect(ctx))
            {
                Console.WriteLine("Warming up... " + WARMUP_NUMBER_OF_ITERATIONS + " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

                using (Publication publication = aeron.AddPublication(PING_CHANNEL, PING_STREAM_ID))
                using (Subscription subscription = aeron.AddSubscription(PONG_CHANNEL, PONG_STREAM_ID))
                {
                    LATCH.Wait();

                    for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
                    {
                        RoundTripMessages(fragmentHandler, publication, subscription, WARMUP_NUMBER_OF_MESSAGES);
                    }

                    Thread.Sleep(100);
                
                    do
                    {
                        HISTOGRAM.Reset();
                        Console.WriteLine("Pinging " + NUMBER_OF_MESSAGES + " messages");

                        RoundTripMessages(fragmentHandler, publication, subscription, NUMBER_OF_MESSAGES);
                        Console.WriteLine("Histogram of RTT latencies in microseconds.");

                        HISTOGRAM.OutputPercentileDistribution(Console.Out);
                    } while (Console.Read() == 'y');
                }
            }
        }


        private static void RoundTripMessages(
            IFragmentHandler fragmentHandler, Publication publication, Subscription subscription, int count)
        {
            for (int i = 0; i < count; i++)
            {
                do
                {
                    ATOMIC_BUFFER.PutLong(0, Stopwatch.GetTimestamp());
                } while (publication.Offer(ATOMIC_BUFFER, 0, MESSAGE_LENGTH) < 0L);

                Console.WriteLine("Blah");

                POLLING_IDLE_STRATEGY.Reset();
                while (subscription.Poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) <= 0)
                {
                    POLLING_IDLE_STRATEGY.Idle();
                }
            }
        }

        private static void pongHandler(IDirectBuffer buffer, int offset, int length, Header header)
        {
            long pingTimestamp = buffer.GetLong(offset);
            long rttNs = Stopwatch.GetTimestamp() - pingTimestamp;

            HISTOGRAM.RecordValue(rttNs/Stopwatch.Frequency*1000000000);
        }

        private static void availablePongImageHandler(Image image)
        {
            Subscription subscription = image.Subscription();
            Console.WriteLine("Available image: channel={0} streamId={0} session={0}\n", subscription.Channel(), subscription.StreamId(), image.SessionId());

            if (PONG_STREAM_ID == subscription.StreamId() && PONG_CHANNEL.Equals(subscription.Channel()))
            {
                LATCH.Signal();
            }
        }


    }
}