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
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Internal listener used by <see cref="AsyncAeronArchive"/> to deliver control responses and recording
    /// descriptors to the owning <see cref="PersistentSubscription"/>.
    /// </summary>
    /// <remarks>Since 1.51.0</remarks>
    internal interface IAsyncAeronArchiveListener : IRecordingDescriptorConsumer
    {
        void OnConnected();

        void OnDisconnected();

        void OnControlResponse(long correlationId, long relevantId, ControlResponseCode code, string errorMessage);

        void OnError(Exception error);
    }
}
