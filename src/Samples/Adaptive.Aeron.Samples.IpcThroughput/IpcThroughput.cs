﻿/*
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

namespace Adaptive.Aeron.Samples.IpcThroughput
{
    public class IpcThroughput
    {
        private const int BurstLength = 1_000_000;
        private static readonly int MessageLength = SampleConfiguration.MESSAGE_LENGTH;
        private static readonly int MessageCountLimit = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
        private static readonly string Channel = Aeron.Context.IPC_CHANNEL;
        private static readonly int StreamID = SampleConfiguration.STREAM_ID;

        public static void Main()
        {
            ComputerSpecifications.Dump();

            var running = new AtomicBoolean(true);

            using (var aeron = Aeron.Connect())
            using (var publication = aeron.AddPublication(Channel, StreamID))
            using (var subscription = aeron.AddSubscription(Channel, StreamID))
            {
                var subscriber = new Subscriber(running, subscription);
                var subscriberThread = new Thread(subscriber.Run) {Name = "subscriber"};
                var publisherThread = new Thread(new Publisher(running, publication).Run) {Name = "publisher"};
                var rateReporterThread = new Thread(new RateReporter(running, subscriber).Run) {Name = "rate-reporter"};

                rateReporterThread.Start();
                subscriberThread.Start();
                publisherThread.Start();

                Console.WriteLine("Press any key to stop...");
                Console.Read();

                running.Set(false);

                subscriberThread.Join();
                publisherThread.Join();
                rateReporterThread.Join();
            }
        }

        public class RateReporter
        {
            internal readonly AtomicBoolean Running;
            internal readonly Subscriber Subscriber;
            private readonly Stopwatch _stopwatch;

            public RateReporter(AtomicBoolean running, Subscriber subscriber)
            {
                Running = running;
                Subscriber = subscriber;
                _stopwatch = Stopwatch.StartNew();
            }

            public void Run()
            {
                var lastTotalBytes = Subscriber.TotalBytes();

                while (Running)
                {
                    Thread.Sleep(1000);

                    var newTotalBytes = Subscriber.TotalBytes();
                    var duration = _stopwatch.ElapsedMilliseconds;
                    var bytesTransferred = newTotalBytes - lastTotalBytes;
                    Console.WriteLine($"Duration {duration:N0}ms - {bytesTransferred/MessageLength:N0} messages - {bytesTransferred:N0} bytes, GC0 {GC.CollectionCount(0)}, GC1 {GC.CollectionCount(1)}, GC2 {GC.CollectionCount(2)}");

                    _stopwatch.Restart();
                    lastTotalBytes = newTotalBytes;
                }
            }
        }

        public sealed class Publisher
        {
            internal readonly AtomicBoolean Running;
            internal readonly Publication Publication;

            public Publisher(AtomicBoolean running, Publication publication)
            {
                Running = running;
                Publication = publication;
            }

            public void Run()
            {
                var publication = Publication;
                using (var byteBuffer = BufferUtil.AllocateDirectAligned(publication.MaxMessageLength, BitUtil.CACHE_LINE_LENGTH))
                using (var buffer = new UnsafeBuffer(byteBuffer))
                {
                    long backPressureCount = 0;
                    long totalMessageCount = 0;

                    while (Running)
                    {
                        for (var i = 0; i < BurstLength; i++)
                        {
                            while (publication.Offer(buffer, 0, MessageLength) <= 0)
                            {
                                ++backPressureCount;
                                if (!Running)
                                {
                                    break;
                                }
                            }

                            ++totalMessageCount;
                        }
                    }

                    var backPressureRatio = backPressureCount/(double) totalMessageCount;
                    Console.WriteLine($"Publisher back pressure ratio: {backPressureRatio}");
                }
            }
        }

        public class Subscriber : IFragmentHandler
        {
            internal readonly AtomicBoolean Running;
            internal readonly Subscription Subscription;

            private readonly AtomicLong _totalBytes = new AtomicLong();

            public Subscriber(AtomicBoolean running, Subscription subscription)
            {
                Running = running;
                Subscription = subscription;
            }

            public long TotalBytes()
            {
                return _totalBytes.Get();
            }

            public void Run()
            {
                while (Subscription.ImageCount == 0)
                {
                    // wait for an image to be ready
                    Thread.Yield();
                }

                var image = Subscription.Images[0];

                long failedPolls = 0;
                long successfulPolls = 0;

                while (Running)
                {
                    var fragmentsRead = image.Poll(this, MessageCountLimit);
                    if (0 == fragmentsRead)
                    {
                        ++failedPolls;
                    }
                    else
                    {
                        ++successfulPolls;
                    }
                }

                var failureRatio = failedPolls / (double) (successfulPolls + failedPolls);
                Console.WriteLine($"Subscriber poll failure ratio: {failureRatio}");
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _totalBytes.Set(_totalBytes.Get() + length);
            }
        }
    }
}