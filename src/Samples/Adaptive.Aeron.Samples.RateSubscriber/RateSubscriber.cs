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
        private static readonly int StreamID = SampleConfiguration.STREAM_ID;
        private static readonly string Channel = Aeron.Context.IPC_CHANNEL;
        private static readonly int FragmentCountLimit = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

        public static void Main()
        {
            Console.WriteLine("Subscribing to " + Channel + " on stream Id " + StreamID);

            var ctx = new Aeron.Context()
                .AvailableImageHandler(SamplesUtil.PrintUnavailableImage)
                .UnavailableImageHandler(SamplesUtil.PrintUnavailableImage);

            var reporter = new RateReporter(1000, SamplesUtil.PrintRate);
            var fragmentAssembler = new FragmentAssembler(SamplesUtil.RateReporterHandler(reporter));
            var running = new AtomicBoolean(true);

            var t = new Thread(subscription => SamplesUtil.SubscriberLoop(fragmentAssembler.OnFragment, FragmentCountLimit, running)((Subscription) subscription));
            var report = new Thread(reporter.Run);

            using (var aeron = Aeron.Connect(ctx))
            using (var subscription = aeron.AddSubscription(Channel, StreamID))
            {
                t.Start(subscription);
                report.Start();

                Console.ReadLine();
                Console.WriteLine("Shutting down...");
                running.Set(false);
                reporter.Halt();

                t.Join();
                report.Join();
            }
        }
    }
}