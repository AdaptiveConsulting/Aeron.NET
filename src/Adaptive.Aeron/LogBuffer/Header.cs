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
using Adaptive.Agrona;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Represents the header of the data frame for accessing meta data fields.
    /// </summary>
    public class Header
    {
        private readonly int _positionBitsToShift;
        private readonly int _initialTermId;
        private int _offset;
        private IDirectBuffer _buffer;
        private readonly Object _context;
        
        /// <summary>
        /// Construct a header that references a buffer for the log.
        /// </summary>
        /// <param name="initialTermId">       this stream started at. </param>
        /// <param name="positionBitsToShift"> for calculating positions. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Header(int initialTermId, int positionBitsToShift) : this(initialTermId, positionBitsToShift, null)
        {
        }

        /// <summary>
        /// Construct a header that references a buffer for the log.
        /// </summary>
        /// <param name="initialTermId">       this stream started at. </param>
        /// <param name="positionBitsToShift"> for calculating positions. </param>
        /// <param name="context"> for storing state when which can be accessed with <see cref="Context"/>.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Header(int initialTermId, int positionBitsToShift, Object context)
        {
            _initialTermId = initialTermId;
            _positionBitsToShift = positionBitsToShift;
            _context = context;
        }
        
        /// <summary>
        /// Context for storing state related to the context of the callback where the header is used.
        /// </summary>
        /// <returns>  context for storing state related to the context of the callback where the header is used.</returns>
        public Object Context
        {
            get { return _context; }
        }

        /// <summary>
        /// Get the current position to which the image has advanced on reading this message.
        /// </summary>
        /// <returns> the current position to which the image has advanced on reading this message. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Position()
        {
            var resultingOffset = BitUtil.Align(TermOffset + FrameLength, FrameDescriptor.FRAME_ALIGNMENT);
            return LogBufferDescriptor.ComputePosition(TermId, resultingOffset, _positionBitsToShift, _initialTermId);
        }

        /// <summary>
        /// Get the initial term id this stream started at.
        /// </summary>
        /// <returns> the initial term id this stream started at. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialTermId()
        {
            return _initialTermId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetBuffer(IDirectBuffer buffer, int offset)
        {
            Buffer = buffer;
            Offset = offset;
        }

        /// <summary>
        /// The offset at which the frame begins.
        /// </summary>
        public int Offset
        {
            get { return _offset; }
            set { _offset = value; }
        }

        /// <summary>
        /// The <seealso cref="IDirectBuffer"/> containing the header.
        /// </summary>
        public IDirectBuffer Buffer
        {
            get { return _buffer; }
            set {
                if (value != _buffer)
                {
                    _buffer = value;
                }
            }
        }
        
        /// <summary>
        /// The total length of the frame including the header.
        /// </summary>
        /// <returns> the total length of the frame including the header. </returns>
        public int FrameLength => Buffer.GetInt(Offset);

        /// <summary>
        /// The session ID to which the frame belongs.
        /// </summary>
        /// <returns> the session ID to which the frame belongs. </returns>
        public int SessionId => Buffer.GetInt(Offset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET);

        /// <summary>
        /// The stream ID to which the frame belongs.
        /// </summary>
        /// <returns> the stream ID to which the frame belongs. </returns>
        public int StreamId => Buffer.GetInt(Offset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET);

        /// <summary>
        /// The term ID to which the frame belongs.
        /// </summary>
        /// <returns> the term ID to which the frame belongs. </returns>
        public int TermId => Buffer.GetInt(Offset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET);

        /// <summary>
        /// The offset in the term at which the frame begins. This will be the same as <seealso cref="Offset()"/>
        /// </summary>
        /// <returns> the offset in the term at which the frame begins. </returns>
        public int TermOffset => Offset;

        /// <summary>
        /// The type of the the frame which should always be <seealso cref="DataHeaderFlyweight.HDR_TYPE_DATA"/>
        /// </summary>
        /// <returns> type of the the frame which should always be <seealso cref="DataHeaderFlyweight.HDR_TYPE_DATA"/> </returns>
        public int Type => Buffer.GetShort(Offset + HeaderFlyweight.TYPE_FIELD_OFFSET) & 0xFFFF;

        /// <summary>
        /// The flags for this frame. Valid flags are <seealso cref="DataHeaderFlyweight.BEGIN_FLAG"/>
        /// and <seealso cref="DataHeaderFlyweight.END_FLAG"/>. A convenience flag <seealso cref="DataHeaderFlyweight.BEGIN_AND_END_FLAGS"/>
        /// can be used for both flags.
        /// </summary>
        /// <returns> the flags for this frame. </returns>
        public byte Flags => Buffer.GetByte(Offset + HeaderFlyweight.FLAGS_FIELD_OFFSET);

        /// <summary>
        /// Get the value stored in the reserve space at the end of a data frame header.
        /// <para>
        /// Note: The value is in <seealso cref="ByteOrder.LittleEndian"/> format.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the value stored in the reserve space at the end of a data frame header. </returns>
        /// <seealso cref="DataHeaderFlyweight" />
        public long ReservedValue => Buffer.GetLong(Offset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET);
    }
}