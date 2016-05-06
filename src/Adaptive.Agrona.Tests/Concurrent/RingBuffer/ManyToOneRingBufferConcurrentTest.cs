using System;
using System.Threading;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.RingBuffer;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent.RingBuffer
{
    [TestFixture]
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
            _unsafeBuffer = new UnsafeBuffer(_byteBuffer);
            _ringBuffer = new ManyToOneRingBuffer(_unsafeBuffer);
        }

        private const int MsgTypeID = 7;

        private readonly byte[] _byteBuffer = new byte[16*1024 + RingBufferDescriptor.TrailerLength];
        private UnsafeBuffer _unsafeBuffer;
        private IRingBuffer _ringBuffer;

        [Test]
        public void ShouldProvideCorrelationIds()
        {
            const int reps = 10*1000*1000;
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

            Assert.AreEqual(_ringBuffer.NextCorrelationId(), (long) (reps*numThreads));
        }


        [Test]
        public void ShouldExchangeMessages()
        {
            const int reps = 10*1000*1000;
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
                var iteration = buffer.GetInt(index + BitUtil.SizeOfInt);

                var count = counts[producerId];
                Assert.AreEqual(iteration, count);

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

            Assert.AreEqual(msgCount, reps*numProducers);
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

                const int length = BitUtil.SizeOfInt*2;
                const int repsValueOffset = BitUtil.SizeOfInt;
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