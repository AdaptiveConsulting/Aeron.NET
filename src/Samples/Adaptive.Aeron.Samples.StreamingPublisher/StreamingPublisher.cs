/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public static class StreamingPublisher
    {
        private static readonly int StreamId = SampleConfiguration.STREAM_ID;
        private static readonly string Channel = SampleConfiguration.CHANNEL;
        private static readonly int MessageLength = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly long NumberOfMessages = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly long LingerTimeoutMs = SampleConfiguration.LINGER_TIMEOUT_MS;
        private static readonly bool RandomMessageLength = SampleConfiguration.RANDOM_MESSAGE_LENGTH;
        private static readonly IIdleStrategy OfferIdleStrategy = new BusySpinIdleStrategy();
        private static readonly IntSupplier LengthGenerator = new IntSupplier(RandomMessageLength, MessageLength);
        private static Thread s_reporterThread;

        private static volatile bool s_printingActive = true;

        public static void Main()
        {
            if (MessageLength < BitUtil.SIZE_OF_LONG)
            {
                throw new ArgumentException($"Message length must be at least {BitUtil.SIZE_OF_LONG:D} bytes");
            }

            ComputerSpecifications.Dump();

            var context = new Aeron.Context();
            var reporter = new RateReporter(1000, PrintRate);

            s_reporterThread = new Thread(_ => reporter.Run());
            s_reporterThread.Start();

            // Connect to media driver and add publication to send messages on the configured channel and stream ID.
            // The Aeron and Publication classes implement AutoCloseable, and will automatically
            // clean up resources when this try block is finished.
            using (var aeron = Aeron.Connect(context))
            using (var publication = aeron.AddPublication(Channel, StreamId))
            using (var byteBuffer = BufferUtil.AllocateDirectAligned(MessageLength, BitUtil.CACHE_LINE_LENGTH))
            using (var buffer = new UnsafeBuffer(byteBuffer))
            {
                do
                {
                    s_printingActive = true;

                    Console.WriteLine(
                        $"Streaming {NumberOfMessages} messages of {(RandomMessageLength ? " random" : "")} size {MessageLength} bytes to {Channel} on stream Id {StreamId}"
                    );

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

                    Console.WriteLine(
                        "Done streaming. Back pressure ratio " + (double)backPressureCount / NumberOfMessages
                    );

                    if (0 < LingerTimeoutMs)
                    {
                        Console.WriteLine("Lingering for " + LingerTimeoutMs + " milliseconds...");
                        Thread.Sleep((int)LingerTimeoutMs);
                    }

                    s_printingActive = false;

                    Console.WriteLine("Execute again?");
                } while (Console.ReadLine() == "y");
            }

            reporter.Halt();
            s_reporterThread.Join();
        }

        public static void PrintRate(double messagesPerSec, double bytesPerSec, long totalFragments, long totalBytes)
        {
            if (s_printingActive)
            {
                Console.WriteLine(
                    $"{messagesPerSec:g02} msgs/sec, {bytesPerSec:g02} bytes/sec, totals {totalFragments:D} messages {totalBytes / (1024 * 1024):D} MB, GC0 {GC.CollectionCount(0)}, GC1 {GC.CollectionCount(1)}, GC2 {GC.CollectionCount(2)}"
                );
            }
        }
    }
}
