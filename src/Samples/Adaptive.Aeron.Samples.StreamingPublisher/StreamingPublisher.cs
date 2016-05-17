using System;
using System.Threading;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.StreamingPublisher
{
    /// <summary>
    /// Publisher that sends as fast as possible a given number of messages at a given length.
    /// </summary>
    public class StreamingPublisher
    {
        private static readonly int StreamID = SampleConfiguration.STREAM_ID;
        private static readonly string Channel = SampleConfiguration.CHANNEL;
        private static readonly int MessageLength = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly long NumberOfMessages = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly long LingerTimeoutMs = SampleConfiguration.LINGER_TIMEOUT_MS;
        private static readonly bool RandomMessageLength = SampleConfiguration.RANDOM_MESSAGE_LENGTH;
        private static readonly IIdleStrategy OfferIdleStrategy = new BusySpinIdleStrategy();
        private static readonly IntSupplier LengthGenerator = new IntSupplier(RandomMessageLength, MessageLength);
        private static Thread _reporterThread;

        private static volatile bool _printingActive = true;

        public static void Main()
        {
            if (MessageLength < BitUtil.SIZE_OF_LONG)
            {
                throw new ArgumentException($"Message length must be at least {BitUtil.SIZE_OF_LONG:D} bytes");
            }

            ComputerSpecifications.Dump();

            var context = new Aeron.Context();
            var reporter = new RateReporter(1000, PrintRate);

            _reporterThread = new Thread(_ => reporter.Run());
            _reporterThread.Start();

            // Connect to media driver and add publication to send messages on the configured channel and stream ID.
            // The Aeron and Publication classes implement AutoCloseable, and will automatically
            // clean up resources when this try block is finished.
            using (var aeron = Aeron.Connect(context))
            using (var publication = aeron.AddPublication(Channel, StreamID))
            using (var buffer = new UnsafeBuffer(new byte[MessageLength]))
            {
                do
                {
                    _printingActive = true;

                    Console.WriteLine($"Streaming {NumberOfMessages} messages of {(RandomMessageLength ? " random" : "")} size {MessageLength} bytes to {Channel} on stream Id {StreamID}");

                    long backPressureCount = 0;

                    for (long i = 0; i < NumberOfMessages; i++)
                    {
                        var length = LengthGenerator.AsInt;

                        buffer.PutLong(0, i);
                        OfferIdleStrategy.Reset();
                        while (publication.Offer(buffer, 0, length) < 0L)
                        {
                            // The offer failed, which is usually due to the publication
                            // being temporarily blocked.  Retry the offer after a short
                            // spin/yield/sleep, depending on the chosen IdleStrategy.
                            backPressureCount++;
                            OfferIdleStrategy.Idle();
                        }

                        reporter.OnMessage(1, length);
                    }

                    Console.WriteLine("Done streaming. Back pressure ratio " + (double) backPressureCount/NumberOfMessages);

                    if (0 < LingerTimeoutMs)
                    {
                        Console.WriteLine("Lingering for " + LingerTimeoutMs + " milliseconds...");
                        Thread.Sleep((int) LingerTimeoutMs);
                    }

                    _printingActive = false;

                    Console.WriteLine("Execute again?");
                } while (Console.ReadLine() == "y");
            }

            reporter.Halt();
            _reporterThread.Join();
        }

        public static void PrintRate(double messagesPerSec, double bytesPerSec, long totalFragments, long totalBytes)
        {
            if (_printingActive)
            {
                Console.WriteLine($"{messagesPerSec:g02} msgs/sec, {bytesPerSec:g02} bytes/sec, totals {totalFragments:D} messages {totalBytes/(1024*1024):D} MB, GC0 {GC.CollectionCount(0)}, GC1 {GC.CollectionCount(1)}, GC2 {GC.CollectionCount(2)}");
            }
        }
    }
}