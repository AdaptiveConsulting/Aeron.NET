using System;
using System.Diagnostics;

namespace Adaptive.Agrona.PerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // warm up
            new ManyToOneRingBufferConcurrentTest().ExchangeMessages(10*1000);
            new ManyToOneRingBufferConcurrentTest().ProvideCorrelationIds(10*1000);

            var iterations = 3;
            for (var i = 0; i < iterations; i++)
            {
                var sw = Stopwatch.StartNew();
                new ManyToOneRingBufferConcurrentTest().ExchangeMessages(10*1000*1000);
                Console.WriteLine("ExchangeMessages run {0} done in {1}ms", i, sw.ElapsedMilliseconds);
            }

            for (var i = 0; i < iterations; i++)
            {
                var sw = Stopwatch.StartNew();
                new ManyToOneRingBufferConcurrentTest().ProvideCorrelationIds(10 * 1000 * 1000);
                Console.WriteLine("ProvideCorrelationIds run {0} done in {1}ms", i, sw.ElapsedMilliseconds);
            }
        }
    }
}
