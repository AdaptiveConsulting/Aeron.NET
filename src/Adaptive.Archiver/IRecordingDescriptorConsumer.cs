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

namespace Adaptive.Archiver
{
    /// <summary>
    /// Consumer of events describing Aeron stream recordings.
    /// </summary>
    public interface IRecordingDescriptorConsumer
    {
        /// <summary>
        /// A recording descriptor returned as a result of requesting a listing of recordings.
        /// </summary>
        /// <param name="controlSessionId">  of the originating session requesting to list recordings. </param>
        /// <param name="correlationId">     of the associated request to list recordings. </param>
        /// <param name="recordingId">       of this recording descriptor. </param>
        /// <param name="startTimestamp">    of the recording. </param>
        /// <param name="stopTimestamp">     of the recording. </param>
        /// <param name="startPosition"> of the recording against the recorded publication, the
        /// <seealso cref="Image.JoinPosition"/>. </param>
        /// <param name="stopPosition"> reached for the recording, final position for <seealso cref="Image.Position"/>.
        /// </param>
        /// <param name="initialTermId">     of the recorded stream, <seealso cref="Image.InitialTermId"/>. </param>
        /// <param name="segmentFileLength"> of the recording which is a multiple of termBufferLength. </param>
        /// <param name="termBufferLength">  of the recorded stream, <seealso cref="Image.TermBufferLength"/>. </param>
        /// <param name="mtuLength">         of the recorded stream, <seealso cref="Image.MtuLength"/>. </param>
        /// <param name="sessionId"> of the recorded stream, this will be the most recent session id for extended
        /// recordings. </param>
        /// <param name="streamId">          of the recorded stream, <seealso cref="Subscription.StreamId"/>. </param>
        /// <param name="strippedChannel"> of the recorded stream which is used for the recording subscription in the
        /// archive. </param>
        /// <param name="originalChannel"> of the recorded stream provided to the start recording request,
        /// <seealso cref="Subscription.Channel"/>. </param>
        /// <param name="sourceIdentity"> of the recorded stream, the <seealso cref="Image.SourceIdentity"/>. </param>
        void OnRecordingDescriptor(
            long controlSessionId,
            long correlationId,
            long recordingId,
            long startTimestamp,
            long stopTimestamp,
            long startPosition,
            long stopPosition,
            int initialTermId,
            int segmentFileLength,
            int termBufferLength,
            int mtuLength,
            int sessionId,
            int streamId,
            string strippedChannel,
            string originalChannel,
            string sourceIdentity
        );
    }
}
