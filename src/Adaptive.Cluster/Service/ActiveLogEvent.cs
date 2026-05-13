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

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Event to signal a change of active log to follow.
    /// </summary>
    internal class ActiveLogEvent
    {
        public readonly long logPosition;
        public readonly long maxLogPosition;
        public readonly int memberId;
        public readonly int sessionId;
        public readonly int streamId;
        public readonly bool isStartup;
        public readonly ClusterRole role;
        public readonly string channel;

        internal ActiveLogEvent(
            long logPosition,
            long maxLogPosition,
            int memberId,
            int sessionId,
            int streamId,
            bool isStartup,
            ClusterRole role,
            string channel
        )
        {
            this.logPosition = logPosition;
            this.maxLogPosition = maxLogPosition;
            this.memberId = memberId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.isStartup = isStartup;
            this.role = role;
            this.channel = channel;
        }

        public override string ToString()
        {
            return "NewActiveLogEvent{" +
                "logPosition=" + logPosition +
                ", maxLogPosition=" + maxLogPosition +
                ", memberId=" + memberId +
                ", sessionId=" + sessionId +
                ", streamId=" + streamId +
                ", isStartup=" + isStartup +
                ", role=" + role +
                ", channel='" + channel + "'" +
                '}';
        }
    }
}
