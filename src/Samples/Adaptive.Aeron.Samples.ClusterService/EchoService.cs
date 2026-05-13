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
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;
using Adaptive.Cluster.Service;

namespace Adaptive.Aeron.Samples.ClusterService
{
    public class EchoService : IClusteredService
    {
        private ICluster _cluster;

        public void OnStart(ICluster cluster, Image snapshotImage)
        {
            Console.WriteLine("OnStart");
            _cluster = cluster;
        }

        public void OnSessionOpen(IClientSession session, long timestampMs)
        {
            Console.WriteLine($"OnSessionOpen: sessionId={session.Id}, timestamp={timestampMs}");
        }

        public void OnSessionClose(IClientSession session, long timestampMs, CloseReason closeReason)
        {
            Console.WriteLine($"OnSessionClose: sessionId={session.Id}, timestamp={timestampMs}");
        }

        public void OnSessionMessage(
            IClientSession session,
            long timestampMs,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header
        )
        {
            Console.WriteLine($"OnSessionMessage: sessionId={session.Id}, timestamp={timestampMs}, length={length}");

            Console.WriteLine("Received Message: " + buffer.GetStringWithoutLengthUtf8(offset, length));

            while (session.Offer(buffer, offset, length) <= 0)
            {
                _cluster.IdleStrategy().Idle();
            }
        }

        public void OnTimerEvent(long correlationId, long timestampMs)
        {
            Console.WriteLine($"OnTimerEvent: correlationId={correlationId}, timestamp={timestampMs}");
        }

        public void OnTakeSnapshot(ExclusivePublication snapshotPublication)
        {
            Console.WriteLine("OnTakeSnapshot");
        }

        public void OnRoleChange(ClusterRole newRole)
        {
            Console.WriteLine($"OnRoleChange: newRole={newRole}");
        }

        public void OnTerminate(ICluster cluster)
        {
            Console.WriteLine("OnTerminate");
        }

        public void OnNewLeadershipTermEvent(
            long leadershipTermId,
            long logPosition,
            long timestamp,
            long termBaseLogPosition,
            int leaderMemberId,
            int logSessionId,
            ClusterTimeUnit timeUnit,
            int appVersion
        )
        {
            Console.WriteLine($"OnNewLeadershipTerm: leadershipTermId={leadershipTermId}");
        }

        public int DoBackgroundWork(long nowNs)
        {
            return 0;
        }
    }
}
