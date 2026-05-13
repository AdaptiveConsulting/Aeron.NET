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

using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;

namespace Adaptive.Aeron.Samples.ClusterClient
{
    static class Program
    {
        static void Main()
        {
            var ctx = new AeronCluster.Context()
                .IngressChannel("aeron:udp?endpoint=localhost:9010")
                .EgressChannel("aeron:udp?endpoint=localhost:0")
                .EgressListener(new MessageListener());

            using (var c = AeronCluster.Connect(ctx))
            {
                var idleStrategy = ctx.IdleStrategy();
                var msgBuffer = new UnsafeBuffer(new byte[100]);
                var len = msgBuffer.PutStringWithoutLengthUtf8(0, "Hello World!");

                while (c.Offer(msgBuffer, 0, len) < 0)
                {
                    idleStrategy.Idle();
                }

                while (c.PollEgress() <= 0)
                {
                    idleStrategy.Idle();
                }
            }
        }
    }
}
