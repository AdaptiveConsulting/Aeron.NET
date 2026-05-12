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

namespace Adaptive.Archiver
{
    /// <summary>
    /// Event listener for observing the status of recordings for an Archive.
    /// </summary>
    public interface IRecordingEventsListener
    {
        /// <summary>
        /// Fired when a recording is started.
        /// </summary>
        /// <param name="recordingId">    assigned to the new recording. </param>
        /// <param name="startPosition">  in the stream at which the recording started. </param>
        /// <param name="sessionId">      of the publication being recorded. </param>
        /// <param name="streamId">       of the publication being recorded. </param>
        /// <param name="channel">        of the publication being recorded. </param>
        /// <param name="sourceIdentity"> of the publication being recorded. </param>
        void OnStart(
            long recordingId,
            long startPosition,
            int sessionId,
            int streamId,
            string channel,
            string sourceIdentity
        );

        /// <summary>
        /// Progress indication of an active recording.
        /// </summary>
        /// <param name="recordingId">   for which progress is being reported. </param>
        /// <param name="startPosition"> in the stream at which the recording started. </param>
        /// <param name="position">      reached in recording the publication. </param>
        void OnProgress(long recordingId, long startPosition, long position);

        /// <summary>
        /// Fired when a recording is stopped.
        /// </summary>
        /// <param name="recordingId">   of the publication that has stopped recording. </param>
        /// <param name="startPosition"> in the stream at which the recording started. </param>
        /// <param name="stopPosition">  at which the recording stopped. </param>
        void OnStop(long recordingId, long startPosition, long stopPosition);
    }
}
