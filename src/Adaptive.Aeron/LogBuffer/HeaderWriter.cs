/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Runtime.CompilerServices;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Utility for applying a header to a message in a term buffer.
    /// 
    /// This class is designed to be thread safe to be used across multiple producers and makes the header
    /// visible in the correct order for consumers.
    /// </summary>
    public class HeaderWriter
    {
        private readonly long _versionFlagsType;
        private readonly long _sessionId;
        private readonly long _streamId;

        public HeaderWriter()
        {
        }

        public HeaderWriter(UnsafeBuffer defaultHeader)
        {
            _versionFlagsType = (long)defaultHeader.GetInt(HeaderFlyweight.VERSION_FIELD_OFFSET) << 32;
            _sessionId = (long)defaultHeader.GetInt(DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET) << 32;
            _streamId = defaultHeader.GetInt(DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET) & 0xFFFFFFFFL;
        }

        /// <summary>
        /// Write a header to the term buffer in <seealso cref="ByteOrder.LittleEndian"/> format using the minimum instructions.
        /// </summary>
        /// <param name="termBuffer"> to be written to. </param>
        /// <param name="offset">     at which the header should be written. </param>
        /// <param name="length">     of the fragment including the header. </param>
        /// <param name="termId">     of the current term buffer. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual void Write(UnsafeBuffer termBuffer, int offset, int length, int termId)
#else
        public void Write(UnsafeBuffer termBuffer, int offset, int length, int termId)
#endif
        {
            var lengthVersionFlagsType = _versionFlagsType | (-length & 0xFFFFFFFFL);
            var termOffsetSessionId = _sessionId | (uint)offset;
            var streamAndTermIds = _streamId | ((long)termId << 32);

            termBuffer.PutLongOrdered(offset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, lengthVersionFlagsType);

            termBuffer.PutLongOrdered(offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, termOffsetSessionId);
            termBuffer.PutLong(offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, streamAndTermIds);
        }
    }
}