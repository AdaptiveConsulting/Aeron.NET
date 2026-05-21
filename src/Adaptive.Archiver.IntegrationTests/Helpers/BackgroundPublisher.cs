/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
using System.Threading.Tasks;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Archiver.IntegrationTests.Helpers
{
    /// <summary>
    /// Background publisher used by the network-problems / live-and-replay-advancing tests.
    /// Mirrors the upstream Java test pattern: each message is length-randomised in
    /// [16, 2048] bytes, with a monotonically increasing message id written into both the
    /// first and last 8 bytes — <see cref="MessageVerifier"/> uses those to detect drops,
    /// reorders, or corruption.
    /// </summary>
    internal sealed class BackgroundPublisher : IDisposable
    {
        private readonly PersistentPublication _publication;
        private readonly int _ratePerSecond;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _task;

        public BackgroundPublisher(PersistentPublication publication, int ratePerSecond)
        {
            _publication = publication;
            _ratePerSecond = ratePerSecond;
            _task = Task.Run(Run);
        }

        private void Run()
        {
            var random = new Random();
            var buffer = new UnsafeBuffer(new byte[2048]);
            long messageId = 0;
            var nextMessageAtTicks = Stopwatch.GetTimestamp() + ToTicks(ExponentialArrivalDelayNanos(_ratePerSecond));

            while (!_cts.IsCancellationRequested)
            {
                var now = Stopwatch.GetTimestamp();
                if (now - nextMessageAtTicks >= 0)
                {
                    int length;
                    lock (random) { length = random.Next(2 * sizeof(long), buffer.Capacity + 1); }
                    buffer.PutLong(0, messageId);
                    buffer.PutLong(length - sizeof(long), messageId);
                    var result = _publication.Offer(buffer, 0, length);
                    if (result > 0)
                    {
                        messageId++;
                        nextMessageAtTicks = now + ToTicks(ExponentialArrivalDelayNanos(_ratePerSecond));
                    }
                }
            }
        }

        private static long ToTicks(long nanos)
        {
            return nanos * Stopwatch.Frequency / 1_000_000_000L;
        }

        private static long ExponentialArrivalDelayNanos(long ratePerSecond)
        {
            var uniform = ThreadLocalRandomDouble();
            var secondFraction = -Math.Log(1.0 - uniform) / ratePerSecond;
            return (long)(secondFraction * 1e9);
        }

        [ThreadStatic]
        private static Random s_threadRandom;
        private static double ThreadLocalRandomDouble()
        {
            s_threadRandom ??= new Random();
            return s_threadRandom.NextDouble();
        }

        private int _disposed;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }
            _cts.Cancel();
            try { _task.Wait(TimeSpan.FromSeconds(5)); } catch { }
            _cts.Dispose();
        }
    }
}
