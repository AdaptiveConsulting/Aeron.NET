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
        private static readonly int PingStreamID = SampleConfiguration.PING_STREAM_ID;
        private static readonly int PongStreamID = SampleConfiguration.PONG_STREAM_ID;
        private static readonly string PingChannel = SampleConfiguration.PING_CHANNEL;
        private static readonly string PongChannel = SampleConfiguration.PONG_CHANNEL;
        private static readonly int FrameCountLimit = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        private static readonly IIdleStrategy PingHandlerIdleStrategy = new BusySpinIdleStrategy();

        public static void Main()
        {
            var ctx = new Aeron.Context()
                .AvailableImageHandler(SamplesUtil.PrintAvailableImage)
                .UnavailableImageHandler(SamplesUtil.PrintUnavailableImage);
            
            IIdleStrategy idleStrategy = new BusySpinIdleStrategy();

            Console.WriteLine("Subscribing Ping at " + PingChannel + " on stream Id " + PingStreamID);
            Console.WriteLine("Publishing Pong at " + PongChannel + " on stream Id " + PongStreamID);

            var running = new AtomicBoolean(true);
            Console.CancelKeyPress += (_, e) => running.Set(false);

            using (var aeron = Aeron.Connect(ctx))
            using (var pongPublication = aeron.AddPublication(PongChannel, PongStreamID))
            using (var pingSubscription = aeron.AddSubscription(PingChannel, PingStreamID))
            {
                FragmentHandler dataHandler = (buffer, offset, length, header) => PingHandler(pongPublication, buffer, offset, length);

                while (running.Get())
                {
                    idleStrategy.Idle(pingSubscription.Poll(dataHandler, FrameCountLimit));
                }

                Console.WriteLine("Shutting down...");
            }
        }

        public static void PingHandler(Publication pongPublication, UnsafeBuffer buffer, int offset, int length)
        {
            if (pongPublication.Offer(buffer, offset, length) > 0L)
            {
                return;
            }

            PingHandlerIdleStrategy.Reset();

            while (pongPublication.Offer(buffer, offset, length) < 0L)
            {
                PingHandlerIdleStrategy.Idle();
            }
        }
    }
}