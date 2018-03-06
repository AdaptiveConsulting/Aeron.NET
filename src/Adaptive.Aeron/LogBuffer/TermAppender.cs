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
    /// Term buffer appender which supports many producers concurrently writing an append-only log.
    /// 
    /// <b>Note:</b> This class is threadsafe.
    /// 
    /// Messages are appended to a term using a framing protocol as described in <seealso cref="FrameDescriptor"/>.
    /// 
    /// A default message header is applied to each message with the fields filled in for fragment flags, type, term number,
    /// as appropriate.
    /// 
    /// A message of type <seealso cref="FrameDescriptor.PADDING_FRAME_TYPE"/> is appended at the end of the buffer if claimed
    /// space is not sufficiently large to accommodate the message about to be written.
    /// </summary>
    public class TermAppender
    {
        /// <summary>
        /// The append operation failed because it was past the end of the buffer.
        /// </summary>
        public const int FAILED = -2;

        private readonly long _tailAddressOffset;
        private readonly UnsafeBuffer _termBuffer;
        private readonly UnsafeBuffer _metaDataBuffer;

        /// <summary>
        /// Construct a view over a term buffer and state buffer for appending frames.
        /// </summary>
        /// <param name="termBuffer">     for where messages are stored. </param>
        /// <param name="metaDataBuffer"> for where the state of writers is stored manage concurrency. </param>
        /// <param name="partitionIndex"> for this will be the active appender.</param>
        public TermAppender(UnsafeBuffer termBuffer, UnsafeBuffer metaDataBuffer, int partitionIndex)
        {
            var tailCounterOffset = LogBufferDescriptor.TERM_TAIL_COUNTERS_OFFSET + partitionIndex * BitUtil.SIZE_OF_LONG;
            metaDataBuffer.BoundsCheck(tailCounterOffset, BitUtil.SIZE_OF_LONG);
            _termBuffer = termBuffer;
            _metaDataBuffer = metaDataBuffer;
            _tailAddressOffset = tailCounterOffset; // TODO divergence
        }

        /// <summary>
        /// Get the raw current tail value in a volatile memory ordering fashion.
        /// </summary>
        /// <returns> the current tail value. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long RawTailVolatile()
        {
            return _metaDataBuffer.GetLongVolatile((int)_tailAddressOffset);
        }
        
        /// <summary>
        /// Claim length of a the term buffer for writing in the message with zero copy semantics.
        /// </summary>
        /// <param name="header">      for writing the default header. </param>
        /// <param name="length">      of the message to be written. </param>
        /// <param name="bufferClaim"> to be updated with the claimed region. </param>
        /// <param name="activeTermId"> used for flow control. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="FAILED"/></returns> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Claim(
            HeaderWriter header, 
            int length, 
            BufferClaim bufferClaim, 
            int activeTermId)
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            long rawTail = GetAndAddRawTail(alignedLength);
            int termId = LogBufferDescriptor.TermId(rawTail);
            long termOffset = rawTail & 0xFFFFFFFFL;
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            CheckTerm(activeTermId, termId);
            
            long resultingOffset = termOffset + alignedLength;
            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int frameOffset = (int) termOffset;
                header.Write(termBuffer, frameOffset, frameLength, termId);
                bufferClaim.Wrap(termBuffer, frameOffset, frameLength);
            }

            return (int)resultingOffset;
        }

        /// <summary>
        /// Append an unfragmented message to the the term buffer.
        /// </summary>
        /// <param name="header">    for writing the default header. </param>
        /// <param name="srcBuffer"> containing the message. </param>
        /// <param name="srcOffset"> at which the message begins. </param>
        /// <param name="length">    of the message in the source buffer. </param>
        /// <param name="reservedValueSupplier"><see cref="ReservedValueSupplier"/> for the frame</param>
        /// <param name="activeTermId"> used for flow control. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="FAILED"/>. </returns> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int AppendUnfragmentedMessage(HeaderWriter header, IDirectBuffer srcBuffer, int srcOffset, int length, ReservedValueSupplier reservedValueSupplier, int activeTermId)
#else
        public int AppendUnfragmentedMessage(HeaderWriter header, IDirectBuffer srcBuffer, int srcOffset, int length, ReservedValueSupplier reservedValueSupplier, int activeTermId)
#endif
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            long rawTail = GetAndAddRawTail(alignedLength);
            int termId = LogBufferDescriptor.TermId(rawTail);
            long termOffset = rawTail & 0xFFFFFFFFL;
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            CheckTerm(activeTermId, termId);
            
            long resultingOffset = termOffset + alignedLength;
            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int frameOffset = (int) termOffset;
                header.Write(termBuffer, frameOffset, frameLength, LogBufferDescriptor.TermId(rawTail));
                termBuffer.PutBytes(frameOffset + DataHeaderFlyweight.HEADER_LENGTH, srcBuffer, srcOffset, length);

                if (null != reservedValueSupplier)
                {
                    long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
                    termBuffer.PutLong(frameOffset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
                }

                FrameDescriptor.FrameLengthOrdered(termBuffer, frameOffset, frameLength);
            }

            return (int)resultingOffset;
        }
        
        /// <summary>
        /// Append an unfragmented message to the the term buffer.
        /// </summary>
        /// <param name="header">    for writing the default header. </param>
        /// <param name="vectors">   to the buffers. </param>
        /// <param name="length">    of the message in the source buffer. </param>
        /// <param name="reservedValueSupplier"><see cref="ReservedValueSupplier"/> for the frame</param>
        /// <param name="activeTermId"> used for flow control. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="FAILED"/>. </returns> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if DEBUG
        public virtual int AppendUnfragmentedMessage(HeaderWriter header, DirectBufferVector[] vectors, int length, ReservedValueSupplier reservedValueSupplier, int activeTermId)
#else
        public int AppendUnfragmentedMessage(HeaderWriter header, DirectBufferVector[] vectors, int length, ReservedValueSupplier reservedValueSupplier, int activeTermId)
#endif
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            long rawTail = GetAndAddRawTail(alignedLength);
            int termId = LogBufferDescriptor.TermId(rawTail);
            long termOffset = rawTail & 0xFFFFFFFFL;
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            CheckTerm(activeTermId, termId);
            
            long resultingOffset = termOffset + alignedLength;
            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int frameOffset = (int) termOffset;
                header.Write(termBuffer, frameOffset, frameLength, LogBufferDescriptor.TermId(rawTail));

                int offset = frameOffset + DataHeaderFlyweight.HEADER_LENGTH;

                foreach (var vector in vectors)
                {
                    termBuffer.PutBytes(offset, vector.buffer, vector.offset, vector.length);
                    offset += vector.length;
                }
                
                if (null != reservedValueSupplier)
                {
                    long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
                    termBuffer.PutLong(frameOffset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
                }

                FrameDescriptor.FrameLengthOrdered(termBuffer, frameOffset, frameLength);
            }

            return (int)resultingOffset;
        }
        
        

        /// <summary>
        /// Append a fragmented message to the the term buffer.
        /// The message will be split up into fragments of MTU length minus header.
        /// </summary>
        /// <param name="header">           for writing the default header. </param>
        /// <param name="srcBuffer">        containing the message. </param>
        /// <param name="srcOffset">        at which the message begins. </param>
        /// <param name="length">           of the message in the source buffer. </param>
        /// <param name="maxPayloadLength"> that the message will be fragmented into. </param>
        /// <param name="reservedValueSupplier"><see cref="ReservedValueSupplier"/> for the frame</param>
        /// <param name="activeTermId"> used for flow control. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="FAILED"/>. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AppendFragmentedMessage(HeaderWriter header, IDirectBuffer srcBuffer, int srcOffset, int length,
            int maxPayloadLength, ReservedValueSupplier reservedValueSupplier, int activeTermId)
        {
            int numMaxPayloads = length/maxPayloadLength;
            int remainingPayload = length%maxPayloadLength;
            int lastFrameLength = remainingPayload > 0 ? BitUtil.Align(remainingPayload + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT) : 0;
            int requiredLength = (numMaxPayloads*(maxPayloadLength + DataHeaderFlyweight.HEADER_LENGTH)) + lastFrameLength;
            long rawTail = GetAndAddRawTail(requiredLength);
            int termId = LogBufferDescriptor.TermId(rawTail);
            long termOffset = rawTail & 0xFFFFFFFFL;
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            CheckTerm(activeTermId, termId);
            
            long resultingOffset = termOffset + requiredLength;
            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int frameOffset = (int) termOffset;
                byte flags = FrameDescriptor.BEGIN_FRAG_FLAG;
                int remaining = length;
                
                do
                {
                    int bytesToWrite = Math.Min(remaining, maxPayloadLength);
                    int frameLength = bytesToWrite + DataHeaderFlyweight.HEADER_LENGTH;
                    int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                    header.Write(termBuffer, frameOffset, frameLength, termId);
                    termBuffer.PutBytes(frameOffset + DataHeaderFlyweight.HEADER_LENGTH, srcBuffer,
                        srcOffset + (length - remaining), bytesToWrite);

                    if (remaining <= maxPayloadLength)
                    {
                        flags |= FrameDescriptor.END_FRAG_FLAG;
                    }

                    FrameDescriptor.FrameFlags(termBuffer, frameOffset, flags);

                    if (null != reservedValueSupplier)
                    {
                        long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
                        termBuffer.PutLong(frameOffset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
                    }

                    FrameDescriptor.FrameLengthOrdered(termBuffer, frameOffset, frameLength);

                    flags = 0;
                    frameOffset += alignedLength;
                    remaining -= bytesToWrite;
                } while (remaining > 0);
            }

            return (int)resultingOffset;
        }
        
        /// <summary>
        /// Append a fragmented message to the the term buffer.
        /// The message will be split up into fragments of MTU length minus header.
        /// </summary>
        /// <param name="header">           for writing the default header. </param>
        /// <param name="vectors">          to the buffers. </param>
        /// <param name="length">           of the message as a sum of the vectors. </param>
        /// <param name="maxPayloadLength"> that the message will be fragmented into. </param>
        /// <param name="reservedValueSupplier"><see cref="ReservedValueSupplier"/> for the frame</param>
        /// <param name="activeTermId"> used for flow control. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="FAILED"/>. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AppendFragmentedMessage(HeaderWriter header, DirectBufferVector[] vectors, int length,
            int maxPayloadLength, ReservedValueSupplier reservedValueSupplier, int activeTermId)
        {
            int numMaxPayloads = length/maxPayloadLength;
            int remainingPayload = length%maxPayloadLength;
            int lastFrameLength = remainingPayload > 0 ? BitUtil.Align(remainingPayload + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT) : 0;
            int requiredLength = (numMaxPayloads*(maxPayloadLength + DataHeaderFlyweight.HEADER_LENGTH)) + lastFrameLength;
            long rawTail = GetAndAddRawTail(requiredLength);
            int termId = LogBufferDescriptor.TermId(rawTail);
            long termOffset = rawTail & 0xFFFFFFFFL;
            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            CheckTerm(activeTermId, termId);
            
            long resultingOffset = termOffset + requiredLength;
            if (resultingOffset > termLength)
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int frameOffset = (int) termOffset;
                byte flags = FrameDescriptor.BEGIN_FRAG_FLAG;
                int remaining = length;
                int vectorIndex = 0;
                int vectorOffset = 0;
                
                do
                {
                    int bytesToWrite = Math.Min(remaining, maxPayloadLength);
                    int frameLength = bytesToWrite + DataHeaderFlyweight.HEADER_LENGTH;
                    int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                    header.Write(termBuffer, frameOffset, frameLength, termId);

                    int bytesWritten = 0;
                    int payloadOffset = frameOffset + DataHeaderFlyweight.HEADER_LENGTH;

                    do
                    {
                        var vector = vectors[vectorIndex];
                        int vectorRemaining = vector.length - vectorOffset;
                        int numBytes = Math.Min(bytesToWrite - bytesWritten, vectorRemaining);
                        
                        termBuffer.PutBytes(payloadOffset, vector.buffer, vectorOffset, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        vectorOffset += numBytes;

                        if (vectorRemaining <= numBytes)
                        {
                            vectorIndex++;
                            vectorOffset = 0;
                        }
                        
                    } while (bytesWritten < bytesToWrite);
                    
                    if (remaining <= maxPayloadLength)
                    {
                        flags |= FrameDescriptor.END_FRAG_FLAG;
                    }

                    FrameDescriptor.FrameFlags(termBuffer, frameOffset, flags);

                    if (null != reservedValueSupplier)
                    {
                        long reservedValue = reservedValueSupplier(termBuffer, frameOffset, frameLength);
                        termBuffer.PutLong(frameOffset + DataHeaderFlyweight.RESERVED_VALUE_OFFSET, reservedValue, ByteOrder.LittleEndian);
                    }

                    FrameDescriptor.FrameLengthOrdered(termBuffer, frameOffset, frameLength);

                    flags = 0;
                    frameOffset += alignedLength;
                    remaining -= bytesToWrite;
                } while (remaining > 0);
            }

            return (int)resultingOffset;
        }

        private static void CheckTerm(int expectedTermId, int termId)
        {
            if (termId != expectedTermId)
            {
                throw new InvalidOperationException($"Action possibly delayed: expectedTermId={expectedTermId} termId={termId}");
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long HandleEndOfLogCondition(UnsafeBuffer termBuffer, long termOffset, HeaderWriter header,
            int termLength, int termId)
        {
            if (termOffset < termLength)
            {
                int offset = (int) termOffset;
                int paddingLength = termLength - offset;
                header.Write(termBuffer, offset, paddingLength, termId);
                FrameDescriptor.FrameType(termBuffer, offset, FrameDescriptor.PADDING_FRAME_TYPE);
                FrameDescriptor.FrameLengthOrdered(termBuffer, offset, paddingLength);
            }

            return FAILED;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetAndAddRawTail(int alignedLength)
        {
            return _metaDataBuffer.GetAndAddLong((int)_tailAddressOffset, alignedLength);
        }
    }
}