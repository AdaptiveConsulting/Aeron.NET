using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer {
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
    public unsafe sealed class TermAppender {
        /// <summary>
        /// The append operation tripped the end of the buffer and needs to rotate.
        /// </summary>
        public const int TRIPPED = -1;

        /// <summary>
        /// The append operation went past the end of the buffer and failed.
        /// </summary>
        public const int FAILED = -2;

        private readonly UnsafeBuffer _termBuffer;
        private readonly IntPtr _termBufferPointer;
        private readonly UnsafeBuffer _metaDataBuffer;
        private readonly IntPtr _metaDataBufferPointer;
        private readonly int _capacity;

        /// <summary>
        /// Construct a view over a term buffer and state buffer for appending frames.
        /// </summary>
        /// <param name="termBuffer">     for where messages are stored. </param>
        /// <param name="metaDataBuffer"> for where the state of writers is stored manage concurrency. </param>
        public TermAppender(UnsafeBuffer termBuffer, UnsafeBuffer metaDataBuffer) 
        {
            this._termBuffer = termBuffer;
            this._termBufferPointer = termBuffer.BufferPointer;
            this._capacity = termBuffer.Capacity;
            this._metaDataBuffer = metaDataBuffer;
            this._metaDataBufferPointer = metaDataBuffer.BufferPointer;
        }

        /// <summary>
        /// The log of messages for a term.
        /// </summary>
        /// <returns> the log of messages for a term. </returns>
        public UnsafeBuffer TermBuffer() 
        {
            return _termBuffer;
        }

        /// <summary>
        /// The meta data describing the term.
        /// </summary>
        /// <returns> the meta data describing the term. </returns>
        public UnsafeBuffer MetaDataBuffer() 
        {
            return _metaDataBuffer;
        }

        /// <summary>
        /// Get the raw value current tail value in a volatile memory ordering fashion.
        /// </summary>
        /// <returns> the current tail value. </returns>
        public long RawTailVolatile() 
        {
            return _metaDataBuffer.GetLongVolatile(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET);
        }

        /// <summary>
        /// Set the value for the tail counter.
        /// </summary>
        /// <param name="termId"> for the tail counter </param>
        public void TailTermId(int termId) 
        {
            _metaDataBuffer.PutLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, ((long)termId) << 32);
        }

        /// <summary>
        /// Set the status of the log buffer with StoreStore memory ordering semantics.
        /// </summary>
        /// <param name="status"> to be set for the log buffer. </param>
        public void StatusOrdered(int status) 
        {
            _metaDataBuffer.PutIntOrdered(LogBufferDescriptor.TERM_STATUS_OFFSET, status);
        }

        /// <summary>
        /// Claim length of a the term buffer for writing in the message with zero copy semantics.
        /// </summary>
        /// <param name="header">      for writing the default header. </param>
        /// <param name="length">      of the message to be written. </param>
        /// <param name="bufferClaim"> to be updated with the claimed region. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="#TRIPPED"/> or <seealso cref="#FAILED"/>
        /// packed with the termId if a padding record was inserted at the end. </returns>
        public unsafe long Claim(HeaderWriter header, int length, BufferClaim bufferClaim) 
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            long rawTail = Interlocked.Add(ref *(long*)(_metaDataBufferPointer + LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET), alignedLength) - alignedLength;
            long termOffset = rawTail & 0xFFFFFFFFL;

            var termBuffer = _termBuffer;
            int termLength = _capacity;
            var termId = TermId(rawTail);
            long resultingOffset = termOffset + alignedLength;
            if (resultingOffset > termLength) {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            } 
            else 
            {
                int offset = (int)termOffset;
                Volatile.Write(ref *(int*)(_termBufferPointer + offset), -length);
                header.Write(termBuffer, offset, frameLength, termId);
                header.Write(termBuffer, offset, frameLength, TermId(rawTail));
                bufferClaim.Wrap(termBuffer, offset, frameLength);
            }

            return resultingOffset;
        }

        /// <summary>
        /// Append an unfragmented message to the the term buffer.
        /// </summary>
        /// <param name="header">    for writing the default header. </param>
        /// <param name="srcBuffer"> containing the message. </param>
        /// <param name="srcOffset"> at which the message begins. </param>
        /// <param name="length">    of the message in the source buffer. </param>
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="#TRIPPED"/> or <seealso cref="#FAILED"/>
        /// packed with the termId if a padding record was inserted at the end. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long AppendUnfragmentedMessage(HeaderWriter header, UnsafeBuffer srcBuffer, int srcOffset, int length) 
        {
            int frameLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            long rawTail = Interlocked.Add(ref *(long*)(_metaDataBufferPointer + LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET), alignedLength) - alignedLength;
            long termOffset = rawTail & 0xFFFFFFFFL;

            var termBuffer = _termBuffer;
            int termLength = _capacity;
            var termId = TermId(rawTail);
            long resultingOffset = termOffset + alignedLength;
            if (resultingOffset > termLength) 
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            }
            else
            {
                int offset = (int)termOffset;
                Volatile.Write(ref *(int*)(_termBufferPointer + offset), -length);
                header.Write(termBuffer, offset, frameLength, termId);
                termBuffer.PutBytes(offset + DataHeaderFlyweight.HEADER_LENGTH, srcBuffer, srcOffset, length);
                termBuffer.PutIntOrdered(offset, frameLength);
            }

            return resultingOffset;
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
        /// <returns> the resulting offset of the term after the append on success otherwise <seealso cref="#TRIPPED"/> or <seealso cref="#FAILED"/>
        /// packed with the termId if a padding record was inserted at the end. </returns>
        public long AppendFragmentedMessage(HeaderWriter header, UnsafeBuffer srcBuffer, int srcOffset, int length,
            int maxPayloadLength) 
        {
            int numMaxPayloads = length / maxPayloadLength;
            int remainingPayload = length % maxPayloadLength;
            int lastFrameLength = remainingPayload > 0
                ? BitUtil.Align(remainingPayload + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT)
                : 0;
            int requiredLength = (numMaxPayloads * (maxPayloadLength + DataHeaderFlyweight.HEADER_LENGTH)) +
                                 lastFrameLength;
            long rawTail = GetAndAddRawTail(requiredLength);
            int termId = TermId(rawTail);
            long termOffset = rawTail & 0xFFFFFFFFL;

            UnsafeBuffer termBuffer = _termBuffer;
            int termLength = termBuffer.Capacity;

            long resultingOffset = termOffset + requiredLength;
            if (resultingOffset > termLength) 
            {
                resultingOffset = HandleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
            } 
            else 
            {
                int offset = (int)termOffset;
                byte flags = FrameDescriptor.BEGIN_FRAG_FLAG;
                int remaining = length;
                do 
                {
                    int bytesToWrite = Math.Min(remaining, maxPayloadLength);
                    int frameLength = bytesToWrite + DataHeaderFlyweight.HEADER_LENGTH;
                    int alignedLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);

                    Volatile.Write(ref *(int*)(_termBufferPointer + offset), -frameLength);

                    header.Write(termBuffer, offset, frameLength, termId);
                    termBuffer.PutBytes(offset + DataHeaderFlyweight.HEADER_LENGTH, srcBuffer,
                        srcOffset + (length - remaining), bytesToWrite);

                    if (remaining <= maxPayloadLength) {
                        flags |= FrameDescriptor.END_FRAG_FLAG;
                    }

                    FrameDescriptor.FrameFlags(termBuffer, offset, flags);
                    FrameDescriptor.FrameLengthOrdered(termBuffer, offset, frameLength);

                    flags = 0;
                    offset += alignedLength;
                    remaining -= bytesToWrite;
                } while (remaining > 0);
            }

            return resultingOffset;
        }


        /// <summary>
        /// Pack the values for termOffset and termId into a long for returning on the stack.
        /// </summary>
        /// <param name="termId">     value to be packed. </param>
        /// <param name="termOffset"> value to be packed. </param>
        /// <returns> a long with both ints packed into it. </returns>
        public static long Pack(int termId, int termOffset) 
        {
            return ((long)termId << 32) | (termOffset & 0xFFFFFFFFL);
        }

        /// <summary>
        /// The termOffset as a result of the append
        /// </summary>
        /// <param name="result"> into which the termOffset value has been packed. </param>
        /// <returns> the termOffset after the append </returns>
        public static int TermOffset(long result) 
        {
            return (int)result;
        }

        /// <summary>
        /// The termId in which the append operation took place.
        /// </summary>
        /// <param name="result"> into which the termId value has been packed. </param>
        /// <returns> the termId in which the append operation took place. </returns>
        public static int TermId(long result) 
        {
            return (int)((long)((ulong)result >> 32));
        }

        private long HandleEndOfLogCondition(UnsafeBuffer termBuffer, long termOffset, HeaderWriter header,
            int termLength, int termId) 
        {
            int resultingOffset = FAILED;

            if (termOffset <= termLength) 
            {
                resultingOffset = TRIPPED;

                if (termOffset < termLength) 
                {
                    int offset = (int)termOffset;
                    int paddingLength = termLength - offset;
                    header.Write(termBuffer, offset, paddingLength, termId);
                    FrameDescriptor.FrameType(termBuffer, offset, FrameDescriptor.PADDING_FRAME_TYPE);
                    FrameDescriptor.FrameLengthOrdered(termBuffer, offset, paddingLength);
                }
            }

            return Pack(termId, resultingOffset);
        }

        private long GetAndAddRawTail(int alignedLength) 
        {
            return _metaDataBuffer.GetAndAddLong(LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET, alignedLength);
        }
    }
}