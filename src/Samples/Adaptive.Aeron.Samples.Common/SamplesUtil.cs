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
using System.Text;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.Samples.Common
{
    /// <summary>
    /// Utility functions for samples
    /// </summary>
    public class SamplesUtil
    {
        /// <summary>
        /// Return a reusable, parameterised event loop that calls a default idler when no messages are received
        /// </summary>
        /// <param name="fragmentHandler"> to be called back for each message. </param>
        /// <param name="limit">           passed to <seealso cref="Subscription#poll(FragmentHandler, int)"/> </param>
        /// <param name="running">         indication for loop </param>
        /// <returns> loop function </returns>
        public static Action<Subscription> SubscriberLoop(FragmentHandler fragmentHandler, int limit, AtomicBoolean running)
        {
            IIdleStrategy idleStrategy = new BusySpinIdleStrategy();

            return SubscriberLoop(fragmentHandler, limit, running, idleStrategy);
        }

        /// <summary>
        /// Return a reusable, parameterized event loop that calls and idler when no messages are received
        /// </summary>
        /// <param name="fragmentHandler"> to be called back for each message. </param>
        /// <param name="limit">           passed to <seealso cref="Subscription#poll(FragmentHandler, int)"/> </param>
        /// <param name="running">         indication for loop </param>
        /// <param name="idleStrategy">    to use for loop </param>
        /// <returns> loop function </returns>
        public static Action<Subscription> SubscriberLoop(FragmentHandler fragmentHandler, int limit, AtomicBoolean running, IIdleStrategy idleStrategy)
        {
            return subscription =>
            {
                while (running.Get())
                {
                    idleStrategy.Idle(subscription.Poll(fragmentHandler, limit));
                }
            };
        }

        /// <summary>
        /// Return a reusable, parameterized <seealso cref="FragmentHandler"/> that prints to stdout
        /// </summary>
        /// <param name="streamId"> to show when printing </param>
        /// <returns> subscription data handler function that prints the message contents </returns>
        public static FragmentHandler PrintStringMessage(int streamId)
        {
            return (buffer, offset, length, header) =>
            {
                var data = new byte[length];
                buffer.GetBytes(offset, data);

                Console.WriteLine($"Message to stream {streamId:D} from session {header.SessionId:D} ({length:D}@{offset:D}) <<{Encoding.UTF8.GetString(data)}>>");
            };
        }

        /// <summary>
        /// Return a reusable, parameteried <seealso cref="FragmentHandler"/> that calls into a
        /// <seealso cref="RateReporter"/>.
        /// </summary>
        /// <param name="reporter"> for the rate </param>
        /// <returns> <seealso cref="FragmentHandler"/> that records the rate information </returns>
        public static FragmentHandler RateReporterHandler(RateReporter reporter)
        {
            return (buffer, offset, length, header) => reporter.OnMessage(1, length);
        }

        /// <summary>
        /// Generic error handler that just prints message to stdout.
        /// </summary>
        /// <param name="channel">   for the error </param>
        /// <param name="streamId">  for the error </param>
        /// <param name="sessionId"> for the error, if source </param>
        /// <param name="message">   indicating what the error was </param>
        /// <param name="cause">     of the error </param>
        public static void PrintError(string channel, int streamId, int sessionId, string message, HeaderFlyweight cause)
        {
            Console.WriteLine(message);
        }

        /// <summary>
        /// Print the rates to stdout
        /// </summary>
        /// <param name="messagesPerSec"> being reported </param>
        /// <param name="bytesPerSec">    being reported </param>
        /// <param name="totalMessages">  being reported </param>
        /// <param name="totalBytes">     being reported </param>
        public static void PrintRate(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes)
        {
            Console.WriteLine($"{messagesPerSec:g02} msgs/sec, {bytesPerSec:g02} bytes/sec, totals {totalMessages:D} messages {totalBytes/(1024*1024):D} MB, GC0 {GC.CollectionCount(0)}, GC1 {GC.CollectionCount(1)}, GC2 {GC.CollectionCount(2)}");
        }

        /// <summary>
        /// Print the information for an available image to stdout.
        /// </summary>
        /// <param name="image"> that has been created </param>
        public static void PrintAvailableImage(Image image)
        {
            var subscription = image.Subscription;
            Console.WriteLine($"Available image on {subscription.Channel} streamId={subscription.StreamId:D} sessionId={image.SessionId:D} from {image.SourceIdentity}");
        }

        /// <summary>
        /// Print the information for an unavailable image to stdout.
        /// </summary>
        /// <param name="image"> that has gone inactive </param>
        public static void PrintUnavailableImage(Image image)
        {
            var subscription = image.Subscription;
            Console.WriteLine($"Unavailable image on {subscription.Channel} streamId={subscription.StreamId:D} sessionId={image.SessionId:D}");
        }
    }
}