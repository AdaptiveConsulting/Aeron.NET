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

namespace Adaptive.Archiver.IntegrationTests.Infrastructure
{
    internal static class TestContexts
    {
        public const string LocalhostControlRequestChannel = "aeron:udp?endpoint=localhost:8010";
        public const string LocalhostControlResponseChannel = "aeron:udp?endpoint=localhost:0";
        public const string LocalhostReplicationChannel = "aeron:udp?endpoint=localhost:0";

        public const string IpcChannel = "aeron:ipc";
        public const string MulticastChannel = "aeron:udp?endpoint=224.20.30.39:14456|interface=localhost";
        public const string EphemeralReplayChannel = "aeron:udp?endpoint=localhost:0";

        // MDC and unicast channels with non-ephemeral ports must NOT be constants — two tests
        // sharing the same hard-coded port would collide on the UDP bind under fast teardown.
        // See PersistentSubscriptionTest.MdcSubscriptionChannel / MdcPublicationChannel for the
        // per-test allocation pattern.

        public const int StreamId = 1000;

        public static AeronArchive.Context LocalhostAeronArchive() =>
            new AeronArchive.Context()
                .ControlRequestChannel(LocalhostControlRequestChannel)
                .ControlResponseChannel(LocalhostControlResponseChannel);
    }
}
