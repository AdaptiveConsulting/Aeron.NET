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
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Aeron.Samples.ClusterClient
{
    internal class MessageListener : IEgressListener
    {
        public void OnMessage(
            long clusterSessionId,
            long timestampMs,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header
        )
        {
            Console.WriteLine($"OnMessage: sessionId={clusterSessionId}, timestamp={timestampMs}, length={length}");

            Console.WriteLine("Received Message: " + buffer.GetStringWithoutLengthUtf8(offset, length));
        }

        public void OnSessionEvent(
            long correlationId,
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            EventCode code,
            string detail
        )
        {
            Console.WriteLine(
                $"Session Event:  leadershipTermId={leadershipTermId}, leaderMemberId={leaderMemberId}, "
                    + $"code={code}, detail={detail}"
            );
        }

        public void OnNewLeader(
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            string memberEndpoints
        )
        {
            Console.WriteLine(
                $"New Leader:  leadershipTermId={leadershipTermId}, leaderMemberId={leaderMemberId}, "
                    + $"memberEndpoints={memberEndpoints}"
            );
        }

        public void OnAdminResponse(
            long clusterSessionId,
            long correlationId,
            AdminRequestType requestType,
            AdminResponseCode responseCode,
            string message,
            IDirectBuffer payload,
            int payloadOffset,
            int payloadLength
        )
        {
            Console.WriteLine(
                $"OnAdminResponse:  clusterSessionId={clusterSessionId}, correlationId={correlationId}, "
                    + $"requestType={requestType}, responseCode={responseCode}"
            );
        }
    }
}
