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
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.RingBuffer;

namespace Adaptive.Agrona.PerformanceTest
{
    public class ManyToOneRingBufferConcurrentTest
    {
        private readonly bool _instanceFieldsInitialized;

        public ManyToOneRingBufferConcurrentTest()
        {
            if (!_instanceFieldsInitialized)
            {
                InitializeInstanceFields();
                _instanceFieldsInitialized = true;
            }
        }

        private void InitializeInstanceFields()
        {
            _unsafeBuffer = new UnsafeBuffer(BufferUtil.AllocateDirectAligned(16 * 1024 + RingBufferDescriptor.TrailerLength, BitUtil.CACHE_LINE_LENGTH));
            _ringBuffer = new ManyToOneRingBuffer(_unsafeBuffer);
        }

        private const int MsgTypeID = 7;

        private UnsafeBuffer _unsafeBuffer;
        private IRingBuffer _ringBuffer;

        public void ProvideCorrelationIds(int reps)
        {
            const int numThreads = 2;
            var barrier = new Barrier(numThreads);
            var threads = new Thread[numThreads];

            for (var i = 0; i < numThreads; i++)
            {
                threads[i] = new Thread(() =>
                {
                    try
                    {
                        barrier.SignalAndWait();
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                    for (var r = 0; r < reps; r++)
                    {
                        _ringBuffer.NextCorrelationId();
                    }
                });

                threads[i].Start();
            }

            foreach (var t in threads)
            {
                t.Join();
            }

            var nextCorrelationId = _ringBuffer.NextCorrelationId();
            if (nextCorrelationId != reps*numThreads)
            {
                Console.WriteLine("error - ProvideCorrelationIds - _ringBuffer.NextCorrelationId()={0}, reps*numThreads={1}", nextCorrelationId, reps * numThreads);
            }
        }

        public void ExchangeMessages(int reps)
        {
            const int numProducers = 2;
            var barrier = new Barrier(numProducers);

            for (var i = 0; i < numProducers; i++)
            {
                var i1 = i;
                new Thread(() =>
                {
                    var p = new Producer(this, i1, barrier, reps);
                    p.Run();
                }).Start();
            }

            var counts = new int[numProducers];

            MessageHandler handler = (msgTypeId, buffer, index, length) =>
            {
                var producerId = buffer.GetInt(index);
                var iteration = buffer.GetInt(index + BitUtil.SIZE_OF_INT);

                var count = counts[producerId];
                if (iteration != count)
                {
                    Console.WriteLine("error - iteration != count. count={0}, iteration={1}", count, iteration);
                    return;
                }

                counts[producerId]++;
            };

            var msgCount = 0;
            while (msgCount < reps*numProducers)
            {
                var readCount = _ringBuffer.Read(handler);
                if (0 == readCount)
                {
                    Thread.Yield();
                }

                msgCount += readCount;
            }

            if (msgCount != reps * numProducers)
            {
                Console.WriteLine("error - msgCount != reps * numProducers. msgCount={0}, reps * numProducers={1}", msgCount, reps * numProducers);
                return;
            }
        }

        internal class Producer
        {
            private readonly ManyToOneRingBufferConcurrentTest _outerInstance;

            internal readonly int ProducerId;
            internal readonly Barrier Barrier;
            internal readonly int Reps;

            internal Producer(ManyToOneRingBufferConcurrentTest outerInstance, int producerId, Barrier barrier, int reps)
            {
                _outerInstance = outerInstance;
                ProducerId = producerId;
                Barrier = barrier;
                Reps = reps;
            }

            public void Run()
            {
                try
                {
                    Barrier.SignalAndWait();
                }
                catch (Exception)
                {
                    // ignored
                }

                const int length = BitUtil.SIZE_OF_INT*2;
                const int repsValueOffset = BitUtil.SIZE_OF_INT;
                var srcBuffer = new UnsafeBuffer(new byte[1024]);

                srcBuffer.PutInt(0, ProducerId);

                for (var i = 0; i < Reps; i++)
                {
                    srcBuffer.PutInt(repsValueOffset, i);

                    while (!_outerInstance._ringBuffer.Write(MsgTypeID, srcBuffer, 0, length))
                    {
                        Thread.Yield();
                    }
                }
            }
        }
    }
}