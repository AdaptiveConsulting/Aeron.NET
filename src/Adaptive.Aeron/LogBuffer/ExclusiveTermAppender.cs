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
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    /// <summary>
    /// Term buffer appender which supports a single exclusive producer writing an append-only log.
    /// 
    /// <b>Note:</b> This class is NOT threadsafe.
    /// 
    /// Messages are appended to a term using a framing protocol as described in <seealso cref="FrameDescriptor"/>.
    /// 
    /// A default message header is applied to each message with the fields filled in for fragment flags, type, term number,
    /// as appropriate.
    /// 
    /// A message of type <seealso cref="FrameDescriptor.PADDING_FRAME_TYPE"/> is appended at the end of the buffer if claimed
    /// space is not sufficiently large to accommodate the message about to be written.
    /// </summary>
    public class ExclusiveTermAppender
    {
        /// <summary>
        /// The append operation tripped the end of the buffer and needs to rotate.
        /// </summary>
        public const int TRIPPED = -1;
        
        private readonly long _tailAddressOffset;
        private readonly UnsafeBuffer _termBuffer;
        private readonly UnsafeBuffer _metaDataBuffer;

        /// <summary>
        /// Construct a view over a term buffer and state buffer for appending frames.
        /// </summary>
        /// <param name="termBuffer">     for where messages are stored. </param>
        /// <param name="metaDataBuffer"> for where the state of writers is stored manage concurrency. </param>
        /// <param name="partitionIndex"> for this will be the active appender.</param>
        public ExclusiveTermAppender(UnsafeBuffer termBuffer, UnsafeBuffer metaDataBuffer, int partitionIndex)
        {
            var tailCounterOffset = LogBufferDescriptor.TERM_TAIL_COUNTERS_OFFSET + partitionIndex * BitUtil.SIZE_OF_LONG;
            metaDataBuffer.BoundsCheck(tailCounterOffset, BitUtil.SIZE_OF_LONG);
            _termBuffer = termBuffer;
            _metaDataBuffer = metaDataBuffer;
            _tailAddressOffset = tailCounterOffset; // TODO divergence
        }

        /// <summary>
        /// Claim length of a the term buffer for writing in the message with zero copy semantics.
        /// </summary>
        /// <param name="termId">      for the current term.</param>
        /// <param name="termOffset">  in the term at which to append.</param>
        /// <param name="header">      for writing the default header. </param>
        /// <param name="length">      of the message to be written. </param>
        /// <param name="bufferClaim"> to be updated with the claimed region. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <see cref="TRIPPED"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Claim(
            int termId,
            int termOffset, 
            HeaderWriter header, 
            int length, 
            ExclusiveBufferClaim bufferClaim)
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;
            
            int resultingOffset = termOffset + alignedLength;
            PutRawTailOrdered(termId, resultingOffset);

            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                header.Write(termBuffer, termOffset, frameLength, termId);
                bufferClaim.Wrap(termBuffer, termOffset, frameLength);
            }

            return resultingOffset;
        }

        /// <summary>
        /// Pad a length of the term buffer with a padding record.
        /// </summary>
        /// <param name="termId"> for the current term.</param>
        /// <param name="termOffset"> in the term at which to append.</param>
        /// <param name="header"> for writing the default header.</param>
        /// <param name="length"> of the padding to be written.</param>
        /// <returns> the resulting offset of the term after success otherwise <see cref="TRIPPED"/>.</returns>
        public int AppendPadding(
            int termId,
            int termOffset,
            HeaderWriter header,
            int length)
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            int resultingOffset = termOffset + alignedLength;
            PutRawTailOrdered(termId, resultingOffset);

            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                header.Write(termBuffer, termOffset, frameLength, termId);
                FrameDescriptor.FrameType(termBuffer, termOffset, FrameDescriptor.PADDING_FRAME_TYPE);
                FrameDescriptor.FrameLengthOrdered(termBuffer, termOffset, frameLength);
            }

            return resultingOffset;
        }

        /// <summary>
        /// Append an unfragmented message to the the term buffer.
        /// </summary>
        /// <param name="termId">      for the current term.</param>
        /// <param name="termOffset">  in the term at which to append.</param>
        /// <param name="header">    for writing the default header. </param>
        /// <param name="srcBuffer"> containing the message. </param>
        /// <param name="srcOffset"> at which the message begins. </param>
        /// <param name="length">    of the message in the source buffer. </param>
        /// <param name="reservedValueSupplier"><see cref="ReservedValueSupplier"/> for the frame</param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="TRIPPED"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int AppendUnfragmentedMessage(
            int termId,
            int termOffset,
            HeaderWriter header, 
            UnsafeBuffer srcBuffer, 
            int srcOffset, 
            int length, 
            ReservedValueSupplier reservedValueSupplier)
#else
        public int AppendUnfragmentedMessage(
            int termId,
            int termOffset, 
            HeaderWriter header, 
            UnsafeBuffer srcBuffer, 
            int srcOffset, 
            int length, 
            ReservedValueSupplier reservedValueSupplier)
#endif
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;
            
            int resultingOffset = termOffset + alignedLength;
            PutRawTailOrdered(termId, resultingOffset);

            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                header.Write(termBuffer, termOffset, frameLength, termId);
                termBuffer.PutBytes(termOffset + DataHeaderFlyweight.HEADER_LENGTH, srcBuffer, srcOffset, length);

                if (null != reservedValueSupplier)
                {
                    long reservedValue = reservedValueSupplier(termBuffer, termOffset, frameLength);
                    termBuffer.PutLong(termOffset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET, reservedValue);
                }

                FrameDescriptor.FrameLengthOrdered(termBuffer, termOffset, frameLength);
            }

            return resultingOffset;
        }


        /// <summary>
        /// Append a fragmented message to the the term buffer.
        /// The message will be split up into fragments of MTU length minus header.
        /// </summary>
        /// <param name="termId">      for the current term.</param>
        /// <param name="termOffset">  in the term at which to append.</param>
        /// <param name="header">           for writing the default header. </param>
        /// <param name="srcBuffer">        containing the message. </param>
        /// <param name="srcOffset">        at which the message begins. </param>
        /// <param name="length">           of the message in the source buffer. </param>
        /// <param name="maxPayloadLength"> that the message will be fragmented into. </param>
        /// /// <param name="reservedValueSupplier"><see cref="ReservedValueSupplier"/> for the frame</param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="TRIPPED"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AppendFragmentedMessage(
            int termId,
            int termOffset, 
            HeaderWriter header, 
            UnsafeBuffer srcBuffer, 
            int srcOffset, 
            int length,
            int maxPayloadLength, 
            ReservedValueSupplier reservedValueSupplier)
        {
            int numMaxPayloads = length / maxPayloadLength;
            int remainingPayload = length % maxPayloadLength;
            int lastFrameLength = remainingPayload > 0
                ? BitUtil.Align(remainingPayload + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT)
                : 0;
            int requiredLength = (numMaxPayloads * (maxPayloadLength + DataHeaderFlyweight.HEADER_LENGTH)) +
                                 lastFrameLength;
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            int resultingOffset = termOffset + requiredLength;
            PutRawTailOrdered(termId, resultingOffset);
            
            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int offset = termOffset;
                byte flags = FrameDescriptor.BEGIN_FRAG_FLAG;
                int remaining = length;
                do
                {
                    int bytesToWrite = Math.Min(remaining, maxPayloadLength);
                    int frameLength = bytesToWrite + DataHeaderFlyweight.HEADER_LENGTH;
                    int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                    header.Write(termBuffer, offset, frameLength, termId);
                    termBuffer.PutBytes(offset + DataHeaderFlyweight.HEADER_LENGTH, srcBuffer,
                        srcOffset + (length - remaining), bytesToWrite);

                    if (remaining <= maxPayloadLength)
                    {
                        flags |= FrameDescriptor.END_FRAG_FLAG;
                    }

                    FrameDescriptor.FrameFlags(termBuffer, offset, flags);

                    if (null != reservedValueSupplier)
                    {
                        long reservedValue = reservedValueSupplier(termBuffer, offset, frameLength);
                        termBuffer.PutLong(offset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET, reservedValue);
                    }

                    FrameDescriptor.FrameLengthOrdered(termBuffer, offset, frameLength);

                    flags = 0;
                    offset += alignedLength;
                    remaining -= bytesToWrite;
                } while (remaining > 0);
            }

            return resultingOffset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int HandleEndOfLogCondition(
            UnsafeBuffer termBuffer,
            long termOffset,
            HeaderWriter header,
            int termLength,
            int termId)
        {
            if (termOffset < termLength)
            {
                int offset = (int) termOffset;
                int paddingLength = termLength - offset;
                header.Write(termBuffer, offset, paddingLength, termId);
                FrameDescriptor.FrameType(termBuffer, offset, FrameDescriptor.PADDING_FRAME_TYPE);
                FrameDescriptor.FrameLengthOrdered(termBuffer, offset, paddingLength);
            }

            return TRIPPED;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PutRawTailOrdered(int termId, int termOffset)
        {
            _metaDataBuffer.PutLongOrdered((int)_tailAddressOffset, LogBufferDescriptor.PackTail(termId, termOffset));
        }
    }
}