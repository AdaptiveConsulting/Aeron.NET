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
using System.Text;
using System.Threading;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class SystemTest
    {
        private EmbeddedMediaDriver _driver;

        [SetUp]
        public void StartDriver() => _driver = new EmbeddedMediaDriver();

        [TearDown]
        public void StopDriver() => _driver?.Dispose();

        [Test]
        public void BasicMessageTest()
        {
            using var aeron = Aeron.Connect();
            var publication = aeron.AddPublication("aeron:ipc", 1);
            var subscription = aeron.AddSubscription("aeron:ipc", 1);
            Await(() => publication.IsConnected);

            var testBytes = Encoding.ASCII.GetBytes("Hello World!");
            var buffer = new UnsafeBuffer(testBytes);
            Await(() => publication.Offer(buffer, 0, buffer.Capacity) > 0);

            bool messageReceived = false;

            void FragmentHandler(IDirectBuffer directBuffer, int offset, int length, Header header)
            {
                if ("Hello World!" == directBuffer.GetStringWithoutLengthAscii(offset, length))
                {
                    messageReceived = true;
                }
            }

            Await(() =>
            {
                subscription.Poll(FragmentHandler, 10);
                return messageReceived;
            });
        }

        private static void Await(Func<bool> predicate)
        {
            var clock = new SystemEpochClock();
            var deadline = clock.Time() + 15_000L;

            while (!predicate())
            {
                if (deadline < clock.Time())
                {
                    throw new TimeoutException();
                }

                Thread.Sleep(10);
            }
        }
    }
}
