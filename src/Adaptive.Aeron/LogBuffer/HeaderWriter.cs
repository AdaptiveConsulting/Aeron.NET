/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using System;
using System.Runtime.CompilerServices;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Utility for applying a header to a message in a term buffer.
    ///
    /// This class is designed to be thread safe to be used across multiple producers and makes the header visible in
    /// the correct order for consumers.
    /// </summary>
    public class HeaderWriter
    {
        protected readonly long _versionFlagsType;
        protected readonly long _sessionId;
        protected readonly long _streamId;

        public HeaderWriter()
        {
        }

        protected HeaderWriter(long versionFlagsType, long sessionId, long streamId)
        {
            _versionFlagsType = versionFlagsType;
            _sessionId = sessionId;
            _streamId = streamId;
        }

        public HeaderWriter(UnsafeBuffer defaultHeader)
        {
            _versionFlagsType = (long)defaultHeader.GetInt(HeaderFlyweight.VERSION_FIELD_OFFSET) << 32;
            _sessionId = (long)defaultHeader.GetInt(DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET) << 32;
            _streamId = defaultHeader.GetInt(DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET) & 0xFFFFFFFFL;
        }

        /// <summary>
        /// Create a new <see cref="HeaderWriter"/> that is byte-order specific to the platform.
        /// </summary>
        /// <param name="defaultHeader">for the stream.</param>
        /// <returns>a new <see cref="HeaderWriter"/> that is byte-order specific to the platform.</returns>
        public static HeaderWriter NewInstance(UnsafeBuffer defaultHeader)
        {
            return BitConverter.IsLittleEndian
                ? new HeaderWriter(defaultHeader)
                : new NativeBigEndianHeaderWriter(defaultHeader);
        }

        /// <summary>
        /// Write a header to the term buffer in <seealso cref="ByteOrder.LittleEndian"/> format using the minimum
        /// instructions.
        /// </summary>
        /// <param name="termBuffer"> to be written to. </param>
        /// <param name="offset">     at which the header should be written. </param>
        /// <param name="length">     of the fragment including the header. </param>
        /// <param name="termId">     of the current term buffer. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Write(UnsafeBuffer termBuffer, int offset, int length, int termId)
        {
            var lengthVersionFlagsType = _versionFlagsType | (-length & 0xFFFFFFFFL);
            var termOffsetSessionId = _sessionId | (uint)offset;
            var streamAndTermIds = _streamId | ((long)termId << 32);

            termBuffer.PutLongOrdered(offset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, lengthVersionFlagsType);

            termBuffer.PutLongOrdered(offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, termOffsetSessionId);
            termBuffer.PutLong(offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, streamAndTermIds);
        }
    }

    /// <summary>
    /// Header writer for big-endian native byte order. On a big-endian host, byte-reverses the length, term offset, and
    /// term id values so the resulting wire bytes remain little-endian (the Aeron protocol's wire format is always
    /// little-endian).
    ///
    /// Selected by <see cref="HeaderWriter.NewInstance"/> when <see cref="BitConverter.IsLittleEndian"/> is false. .NET
    /// currently runs only on little-endian platforms, but BE platforms have existed historically (e.g. PowerPC, MIPS)
    /// and may again.
    /// </summary>
    internal sealed class NativeBigEndianHeaderWriter : HeaderWriter
    {
        public NativeBigEndianHeaderWriter(UnsafeBuffer defaultHeader)
            : base(
                defaultHeader.GetInt(HeaderFlyweight.VERSION_FIELD_OFFSET) & 0xFFFFFFFFL,
                defaultHeader.GetInt(DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET) & 0xFFFFFFFFL,
                (long)defaultHeader.GetInt(DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET) << 32
            )
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Write(UnsafeBuffer termBuffer, int offset, int length, int termId)
        {
            termBuffer.PutLongOrdered(
                offset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET,
                ((long)ReverseBytes(-length) << 32) | _versionFlagsType
            );

            termBuffer.PutLongOrdered(
                offset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET,
                ((long)ReverseBytes(offset) << 32) | _sessionId
            );

            termBuffer.PutLong(
                offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET,
                _streamId | (ReverseBytes(termId) & 0xFFFFFFFFL)
            );
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ReverseBytes(int value)
        {
            return (int)(
                ((uint)value & 0x000000FFU) << 24
                | ((uint)value & 0x0000FF00U) << 8
                | ((uint)value & 0x00FF0000U) >> 8
                | ((uint)value & 0xFF000000U) >> 24
            );
        }
    }
}
