using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.Pong
{
    /// <summary>
    /// Pong component of Ping-Pong.
    /// <para>
    /// Echoes back messages
    /// </para>
    /// </summary>
    public class Pong
    {
        private static readonly int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
        private static readonly int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
        private static readonly string PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
        private static readonly string PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;
        private static readonly int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
        private static readonly bool INFO_FLAG = SampleConfiguration.INFO_FLAG;

        private static readonly IIdleStrategy PING_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();

        public static void Main(string[] args)
        {
            var ctx = new Aeron.Context();

            if (INFO_FLAG)
            {
                ctx.AvailableImageHandler(SamplesUtil.PrintAvailableImage);
                ctx.UnavailableImageHandler(SamplesUtil.PrintUnavailableImage);
            }

            IIdleStrategy idleStrategy = new BusySpinIdleStrategy();

            Console.WriteLine("Subscribing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
            Console.WriteLine("Publishing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);

            var running = new AtomicBoolean(true);
            Console.CancelKeyPress += (_, e) => running.Set(false);

            using (var aeron = Aeron.Connect(ctx))
            using (var pongPublication = aeron.AddPublication(PONG_CHANNEL, PONG_STREAM_ID))
            using (var pingSubscription = aeron.AddSubscription(PING_CHANNEL, PING_STREAM_ID))
            {
                FragmentHandler dataHandler = (buffer, offset, length, header) => PingHandler(pongPublication, buffer, offset, length);

                while (running.Get())
                {
                    idleStrategy.Idle(pingSubscription.Poll(dataHandler, FRAME_COUNT_LIMIT));
                }

                Console.WriteLine("Shutting down...");
            }
        }

        public static void PingHandler(Publication pongPublication, IDirectBuffer buffer, int offset, int length)
        {
            if (pongPublication.Offer(buffer, offset, length) > 0L)
            {
                return;
            }

            PING_HANDLER_IDLE_STRATEGY.Reset();

            while (pongPublication.Offer(buffer, offset, length) < 0L)
            {
                PING_HANDLER_IDLE_STRATEGY.Idle();
            }
        }
    }
}