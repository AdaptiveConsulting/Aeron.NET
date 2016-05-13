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
        private static readonly int STREAM_ID = SampleConfiguration.STREAM_ID;
        private static readonly string CHANNEL = SampleConfiguration.CHANNEL;
        private static readonly int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;
        private static readonly bool RANDOM_MESSAGE_LENGTH = SampleConfiguration.RANDOM_MESSAGE_LENGTH;
        private static readonly UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
        private static readonly IIdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();
        private static readonly IntSupplier LENGTH_GENERATOR = new IntSupplier(RANDOM_MESSAGE_LENGTH, MESSAGE_LENGTH);
        private static Thread _reporterThread;

        private static volatile bool _printingActive = true;

        public static void Main(string[] args)
        {
            if (MESSAGE_LENGTH < BitUtil.SIZE_OF_LONG)
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
            using (var publication = aeron.AddPublication(CHANNEL, STREAM_ID))
            {
                do
                {
                    _printingActive = true;

                    Console.WriteLine("Streaming {0} messages of {1} size {2} bytes to {3} on stream Id {4}", NUMBER_OF_MESSAGES, RANDOM_MESSAGE_LENGTH ? " random" : "", MESSAGE_LENGTH, CHANNEL, STREAM_ID);

                    long backPressureCount = 0;

                    for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
                    {
                        var length = LENGTH_GENERATOR.AsInt;

                        ATOMIC_BUFFER.PutLong(0, i);
                        OFFER_IDLE_STRATEGY.Reset();
                        while (publication.Offer(ATOMIC_BUFFER, 0, length) < 0L)
                        {
                            // The offer failed, which is usually due to the publication
                            // being temporarily blocked.  Retry the offer after a short
                            // spin/yield/sleep, depending on the chosen IdleStrategy.
                            backPressureCount++;
                            OFFER_IDLE_STRATEGY.Idle();
                        }

                        reporter.OnMessage(1, length);
                    }

                    Console.WriteLine("Done streaming. Back pressure ratio " + (double) backPressureCount/NUMBER_OF_MESSAGES);

                    if (0 < LINGER_TIMEOUT_MS)
                    {
                        Console.WriteLine("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                        Thread.Sleep((int) LINGER_TIMEOUT_MS);
                    }

                    _printingActive = false;

                    Console.WriteLine("Execute again?");
                } while (Console.Read() == 'y');
            }

            reporter.Halt();
            _reporterThread.Abort();
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