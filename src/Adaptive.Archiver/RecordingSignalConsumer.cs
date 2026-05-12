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

using Adaptive.Aeron;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Consumer of signals representing operations applied to a recording.
    /// </summary>
    public interface IRecordingSignalConsumer
    {
        /// <summary>
        /// Signal of operation taken on a recording.
        /// </summary>
        /// <param name="controlSessionId"> that initiated the operation. </param>
        /// <param name="correlationId">    that initiated the operation, could be the replication id. </param>
        /// <param name="recordingId">      which has signalled. </param>
        /// <param name="subscriptionId">   of the <see cref="Subscription"/> associated with the recording. </param>
        /// <param name="position">         of the recorded stream at the point of signal. </param>
        /// <param name="signal">           type of the operation applied to the recording. </param>
        void OnSignal(
            long controlSessionId,
            long correlationId,
            long recordingId,
            long subscriptionId,
            long position,
            RecordingSignal signal
        );
    }
}
