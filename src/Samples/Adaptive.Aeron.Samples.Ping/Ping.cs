/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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
using System.Diagnostics;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Samples.Common;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Util;
using HdrHistogram;

namespace Adaptive.Aeron.Samples.Ping
{
    public class Ping
    {
        private static readonly string PingChannel = SampleConfiguration.PING_CHANNEL;
        private static readonly string PongChannel = SampleConfiguration.PONG_CHANNEL;
        private static readonly int PingStreamID = SampleConfiguration.PING_STREAM_ID;
        private static readonly int PongStreamID = SampleConfiguration.PONG_STREAM_ID;
        private static readonly int NumberOfMessages = SampleConfiguration.NUMBER_OF_MESSAGES;
        private static readonly int WarmupNumberOfMessages = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
        private static readonly int WarmupNumberOfIterations = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
        private static readonly int MessageLength = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly int FragmentCountLimit = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        private static readonly LongHistogram Histogram = new LongHistogram(NanoUtil.FromSeconds(10), 3);
        private static readonly CountdownEvent Latch = new CountdownEvent(1);
        private static readonly IIdleStrategy PollingIdleStrategy = new BusySpinIdleStrategy();

        public static void Main()
        {
            var ctx = new Aeron.Context()
                .AvailableImageHandler(AvailablePongImageHandler);

            var fragmentAssembler = new FragmentAssembler(HandlerHelper.ToFragmentHandler(PongHandler));

            Console.WriteLine("Publishing Ping at " + PingChannel + " on stream Id " + PingStreamID);
            Console.WriteLine("Subscribing Pong at " + PongChannel + " on stream Id " + PongStreamID);
            Console.WriteLine("Message length of " + MessageLength + " bytes");

            using (var aeron = Aeron.Connect(ctx))
            {
                Console.WriteLine("Warming up... " + WarmupNumberOfIterations + " iterations of " + WarmupNumberOfMessages + " messages");

                using (var publication = aeron.AddPublication(PingChannel, PingStreamID))
                using (var subscription = aeron.AddSubscription(PongChannel, PongStreamID))
                using (var byteBuffer = BufferUtil.AllocateDirectAligned(MessageLength, BitUtil.CACHE_LINE_LENGTH))
                using (var atomicBuffer = new UnsafeBuffer(byteBuffer))
                {
                    Latch.Wait();

                    for (var i = 0; i < WarmupNumberOfIterations; i++)
                    {
                        RoundTripMessages(atomicBuffer, fragmentAssembler, publication, subscription, WarmupNumberOfMessages);
                    }

                    Thread.Sleep(100);

                    do
                    {
                        Histogram.Reset();
                        Console.WriteLine("Pinging " + NumberOfMessages + " messages");

                        RoundTripMessages(atomicBuffer, fragmentAssembler, publication, subscription, NumberOfMessages);
                        Console.WriteLine("Histogram of RTT latencies in microseconds.");

                        Histogram.OutputPercentileDistribution(Console.Out, outputValueUnitScalingRatio: 1000);
                    } while (Console.Read() == 'y');
                }
            }
        }


        private static void RoundTripMessages(UnsafeBuffer buffer,
            IFragmentHandler fragmentHandler, Publication publication, Subscription subscription, int count)
        {
            for (var i = 0; i < count; i++)
            {
                do
                {
                    buffer.PutLong(0, Stopwatch.GetTimestamp());
                } while (publication.Offer(buffer, 0, MessageLength) < 0L);

                PollingIdleStrategy.Reset();
                while (subscription.Poll(fragmentHandler, FragmentCountLimit) <= 0)
                {
                    PollingIdleStrategy.Idle();
                }
            }
        }

        private static void PongHandler(IDirectBuffer buffer, int offset, int length, Header header)
        {
            var pingTimestamp = buffer.GetLong(offset);
            var rttNs = Stopwatch.GetTimestamp() - pingTimestamp;

            var b = rttNs*1000*1000*1000d/Stopwatch.Frequency;

            Histogram.RecordValue((long) b);
        }

        private static void AvailablePongImageHandler(Image image)
        {
            var subscription = image.Subscription;
            Console.WriteLine($"Available image: channel={subscription.Channel} streamId={subscription.StreamId} session={image.SessionId}");

            if (PongStreamID == subscription.StreamId && PongChannel.Equals(subscription.Channel))
            {
                Latch.Signal();
            }
        }
    }
}