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
using Adaptive.Agrona.Concurrent.Status;
using NUnit.Framework;

namespace Adaptive.Archiver.IntegrationTests.Infrastructure
{
    /// <summary>
    /// .NET equivalent of Java's <c>io.aeron.test.Tests</c> helpers. Provides
    /// the await/executeUntil idiom used throughout the system tests.
    /// </summary>
    internal static class Tests
    {
        private const int DefaultPollIntervalMs = 1;
        private const int DefaultTimeoutMs = 10_000;

        public static void ExecuteUntil(Func<bool> condition, Action action, int timeoutMs = DefaultTimeoutMs)
        {
            ExecuteUntil(condition, action, () => "condition was not met", timeoutMs);
        }

        public static void ExecuteUntil(
            Func<bool> condition,
            Action action,
            Func<string> message,
            int timeoutMs = DefaultTimeoutMs)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    Assert.Fail("timed out after " + timeoutMs + "ms: " + message());
                }

                action();
                Thread.Sleep(DefaultPollIntervalMs);
            }
        }

        public static void Await(Func<bool> condition, int timeoutMs = DefaultTimeoutMs)
        {
            ExecuteUntil(condition, () => { }, timeoutMs);
        }

        public static int AwaitRecordingCounterId(CountersReader countersReader, int sessionId, long archiveId)
        {
            int counterId;
            var deadline = DateTime.UtcNow.AddMilliseconds(DefaultTimeoutMs);
            while ((counterId = Adaptive.Archiver.RecordingPos.FindCounterIdBySession(
                    countersReader, sessionId, archiveId)) == CountersReader.NULL_COUNTER_ID)
            {
                if (DateTime.UtcNow > deadline)
                {
                    Assert.Fail("timed out waiting for recording counter for sessionId=" + sessionId);
                }
                Thread.Sleep(DefaultPollIntervalMs);
            }
            return counterId;
        }

        public static void AwaitPosition(CountersReader countersReader, int counterId, long position)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(DefaultTimeoutMs);
            while (countersReader.GetCounterValue(counterId) < position)
            {
                if (DateTime.UtcNow > deadline)
                {
                    Assert.Fail($"timed out waiting for counter {counterId} to reach position {position}");
                }
                Thread.Sleep(DefaultPollIntervalMs);
            }
        }

        public static void YieldingIdle(string reason)
        {
            Thread.Yield();
        }

        public static void AwaitConnected(Adaptive.Aeron.Subscription subscription, int timeoutMs = DefaultTimeoutMs)
        {
            Await(() => subscription.IsConnected, timeoutMs);
        }
    }
}
