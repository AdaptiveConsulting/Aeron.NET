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

using System;
using System.Runtime.CompilerServices;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Description of the structure for message framing in a log buffer.
    /// 
    /// All messages are logged in frames that have a minimum header layout as follows plus a reserve then
    /// the encoded message follows:
    /// 
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |R|                       Frame Length                          |
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
    ///  |  Version      |B|E| Flags     |             Type              |
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
    ///  |R|                       Term Offset                           |
    ///  +-+-------------------------------------------------------------+
    ///  |                      Additional Fields                       ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    ///  |                        Encoded Message                       ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// 
    /// The (B)egin and (E)nd flags are used for message fragmentation. R is for reserved bit.
    /// Both (B)egin and (E)nd flags are set for a message that does not span frames.
    /// </summary>
    public class FrameDescriptor
    {
        /// <summary>
        /// Set a pragmatic maximum message length regardless of term length to encourage better design.
        /// Messages larger than half the cache size should be broken up into chunks and streamed.
        /// </summary>
        public const int MAX_MESSAGE_LENGTH = 16 * 1024 * 1024;

        /// <summary>
        /// Alignment as a multiple of bytes for each frame. The length field will store the unaligned length in bytes.
        /// </summary>
        public const int FRAME_ALIGNMENT = 32;

        /// <summary>
        /// Beginning fragment of a frame.
        /// </summary>
        public const byte BEGIN_FRAG_FLAG = 128;

        /// <summary>
        /// End fragment of a frame.
        /// </summary>
        public const byte END_FRAG_FLAG = 64;

        /// <summary>
        /// End fragment of a frame.
        /// </summary>
        public const byte UNFRAGMENTED = 192; // BEGIN_FRAG_FLAG | END_FRAG_FLAG;

        /// <summary>
        /// Offset within a frame at which the version field begins
        /// </summary>
        public const int VERSION_OFFSET = HeaderFlyweight.VERSION_FIELD_OFFSET;

        /// <summary>
        /// Offset within a frame at which the flags field begins
        /// </summary>
        public const int FLAGS_OFFSET = HeaderFlyweight.FLAGS_FIELD_OFFSET;

        /// <summary>
        /// Offset within a frame at which the type field begins
        /// </summary>
        public const int TYPE_OFFSET = HeaderFlyweight.TYPE_FIELD_OFFSET;

        /// <summary>
        /// Offset within a frame at which the term offset field begins
        /// </summary>
        public const int TERM_OFFSET = DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET;

        /// <summary>
        /// Offset within a frame at which the term id field begins
        /// </summary>
        public const int TERM_ID_OFFSET = DataHeaderFlyweight.TERM_ID_FIELD_OFFSET;

        /// <summary>
        /// Padding frame type to indicate the message should be ignored.
        /// </summary>
        public const int PADDING_FRAME_TYPE = HeaderFlyweight.HDR_TYPE_PAD;

        /// <summary>
        /// Compute the maximum supported message length for a buffer of given termLength.
        /// </summary>
        /// <param name="termLength"> of the log buffer. </param>
        /// <returns> the maximum supported length for a message. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ComputeMaxMessageLength(int termLength)
        {
            return Math.Min(termLength / 8, MAX_MESSAGE_LENGTH);
        }

        /// <summary>
        /// Compute the maximum supported message length for a buffer of given termLength when the publication is exclusive.
        /// </summary>
        /// <param name="termLength"> of the log buffer. </param>
        /// <returns> the maximum supported length for a message. </returns>
        public static int ComputeExclusiveMaxMessageLength(int termLength)
        {
            return Math.Min(termLength / 4, MAX_MESSAGE_LENGTH);
        }

        /// <summary>
        /// The buffer offset at which the length field begins.
        /// </summary>
        /// <param name="termOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the length field begins. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LengthOffset(int termOffset)
        {
            return termOffset;
        }

        /// <summary>
        /// The buffer offset at which the version field begins.
        /// </summary>
        /// <param name="termOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the version field begins. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int VersionOffset(int termOffset)
        {
            return termOffset + VERSION_OFFSET;
        }

        /// <summary>
        /// The buffer offset at which the flags field begins.
        /// </summary>
        /// <param name="termOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the flags field begins. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FlagsOffset(int termOffset)
        {
            return termOffset + FLAGS_OFFSET;
        }

        /// <summary>
        /// The buffer offset at which the type field begins.
        /// </summary>
        /// <param name="termOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the type field begins. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TypeOffset(int termOffset)
        {
            return termOffset + TYPE_OFFSET;
        }

        /// <summary>
        /// The buffer offset at which the term offset field begins.
        /// </summary>
        /// <param name="termOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the term offset field begins. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermOffsetOffset(int termOffset)
        {
            return termOffset + TERM_OFFSET;
        }

        /// <summary>
        /// The buffer offset at which the term id field begins.
        /// </summary>
        /// <param name="termOffset"> at which the frame begins. </param>
        /// <returns> the offset at which the term id field begins. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TermIdOffset(int termOffset)
        {
            return termOffset + TERM_ID_OFFSET;
        }

        /// <summary>
        /// Read the type of of the frame from header.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <returns> the value of the frame type header. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FrameVersion(IAtomicBuffer buffer, int termOffset)
        {
            return buffer.GetByte(VersionOffset(termOffset));
        }

        /// <summary>
        /// Get the flags field for a frame.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <returns> the value of the frame type header. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FrameFlags(IAtomicBuffer buffer, int termOffset)
        {
            return buffer.GetByte(FlagsOffset(termOffset));
        }

        /// <summary>
        /// Read the type of of the frame from header.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <returns> the value of the frame type header. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FrameType(IAtomicBuffer buffer, int termOffset)
        {
            //return buffer.GetShort(TypeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
            return buffer.GetShort(TypeOffset(termOffset)) & 0xFFFF;
        }

        /// <summary>
        /// Is the frame starting at the termOffset a padding frame at the end of a buffer?
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <returns> true if the frame is a padding frame otherwise false. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsPaddingFrame(IAtomicBuffer buffer, int termOffset)
        {
            return buffer.GetShort(TypeOffset(termOffset)) == PADDING_FRAME_TYPE;
        }

        /// <summary>
        /// Get the length of a frame from the header.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <returns> the value for the frame length. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FrameLength(IAtomicBuffer buffer, int termOffset)
        {
            return buffer.GetInt(termOffset);
        }

        /// <summary>
        /// Get the length of a frame from the header as a volatile read.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <returns> the value for the frame length. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FrameLengthVolatile(IAtomicBuffer buffer, int termOffset)
        {
            int frameLength = buffer.GetIntVolatile(termOffset);

            return frameLength;
        }

        /// <summary>
        /// Write the length header for a frame in a memory ordered fashion.
        /// </summary>
        /// <param name="buffer">      containing the frame. </param>
        /// <param name="termOffset">  at which a frame begins. </param>
        /// <param name="frameLength"> field to be set for the frame. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FrameLengthOrdered(IAtomicBuffer buffer, int termOffset, int frameLength)
        {
            //if (ByteOrder.NativeOrder() != LITTLE_ENDIAN)
            //{
            //    frameLength = Integer.ReverseBytes(frameLength);
            //}

            buffer.PutIntOrdered(termOffset, frameLength);
        }

        /// <summary>
        /// Write the type field for a frame.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <param name="type">       type value for the frame. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FrameType(IAtomicBuffer buffer, int termOffset, int type)
        {
            buffer.PutShort(TypeOffset(termOffset), (short) type);
        }

        /// <summary>
        /// Write the flags field for a frame.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <param name="flags">      value for the frame. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FrameFlags(IAtomicBuffer buffer, int termOffset, byte flags)
        {
            buffer.PutByte(FlagsOffset(termOffset), flags);
        }

        /// <summary>
        /// Write the term offset field for a frame.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FrameTermOffset(UnsafeBuffer buffer, int termOffset)
        {
            buffer.PutInt(TermOffsetOffset(termOffset), termOffset);
        }

        /// <summary>
        /// Write the term id field for a frame.
        /// </summary>
        /// <param name="buffer">     containing the frame. </param>
        /// <param name="termOffset"> at which a frame begins. </param>
        /// <param name="termId">     value for the frame. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FrameTermId(UnsafeBuffer buffer, int termOffset, int termId)
        {
            buffer.PutInt(TermIdOffset(termOffset), termId);
        }
    }
}