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
