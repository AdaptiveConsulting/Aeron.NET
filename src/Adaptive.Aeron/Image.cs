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
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Represents a replicated publication <seealso cref="Image"/> from a publisher to a <seealso cref="Subscription"/>.
    /// Each <seealso cref="Image"/> identifies a source publisher by session id.
    /// 
    /// By default fragmented messages are not reassembled before delivery. If an application must
    /// receive whole messages, whether or not they were fragmented, then the Subscriber
    /// should be created with a <seealso cref="FragmentAssembler"/> or a custom implementation.
    /// 
    /// It is an application's responsibility to <seealso cref="Poll"/> the <seealso cref="Image"/> for new messages.
    /// 
    /// <b>Note:</b>Images are not threadsafe and should not be shared between subscribers.
    /// </summary>
    public class Image
    {
        private readonly long _joinPosition;
        private long _finalPosition;
        private readonly int _initialTermId;

        private readonly int _termLengthMask;
        private readonly int _positionBitsToShift;
        private bool _isEos;
        private volatile bool _isClosed;

        private readonly IPosition _subscriberPosition;
        private readonly UnsafeBuffer[] _termBuffers;
        private readonly Header _header;
        private readonly ErrorHandler _errorHandler;
        private readonly LogBuffers _logBuffers;

        internal Image()
        {
        }

        /// <summary>
        /// Construct a new image over a log to represent a stream of messages from a <seealso cref="Publication"/>.
        /// </summary>
        /// <param name="subscription">       to which this <seealso cref="Image"/> belongs. </param>
        /// <param name="sessionId">          of the stream of messages. </param>
        /// <param name="subscriberPosition"> for indicating the position of the subscriber in the stream. </param>
        /// <param name="logBuffers">         containing the stream of messages. </param>
        /// <param name="errorHandler">       to be called if an error occurs when polling for messages. </param>
        /// <param name="sourceIdentity">     of the source sending the stream of messages. </param>
        /// <param name="correlationId">      of the request to the media driver. </param>
        public Image(Subscription subscription, int sessionId, IPosition subscriberPosition, LogBuffers logBuffers, ErrorHandler errorHandler, string sourceIdentity, long correlationId)
        {
            Subscription = subscription;
            SessionId = sessionId;
            _subscriberPosition = subscriberPosition;
            _logBuffers = logBuffers;
            _errorHandler = errorHandler;
            SourceIdentity = sourceIdentity;
            CorrelationId = correlationId;
            _joinPosition = subscriberPosition.Get();

            _termBuffers = logBuffers.DuplicateTermBuffers();

            var termLength = logBuffers.TermLength();
            _termLengthMask = termLength - 1;
            _positionBitsToShift = IntUtil.NumberOfTrailingZeros(termLength);
            _initialTermId = LogBufferDescriptor.InitialTermId(logBuffers.MetaDataBuffer());
            _header = new Header(LogBufferDescriptor.InitialTermId(logBuffers.MetaDataBuffer()), _positionBitsToShift, this);
        }

        /// <summary>
        /// Get the length in bytes for each term partition in the log buffer.
        /// </summary>
        /// <returns> the length in bytes for each term partition in the log buffer. </returns>
        public int TermBufferLength => _termLengthMask + 1;

        /// <summary>
        /// The sessionId for the steam of messages. Sessions are unique within a <see cref="Subscription"/> and unique across
        /// all <see cref="Publication"/>s from a <see cref="SourceIdentity"/>
        /// </summary>
        /// <returns> the sessionId for the steam of messages. </returns>
        public int SessionId { get; }

        /// <summary>
        /// The source identity of the sending publisher as an abstract concept appropriate for the media.
        /// </summary>
        /// <returns> source identity of the sending publisher as an abstract concept appropriate for the media. </returns>
        public string SourceIdentity { get; }

        /// <summary>
        /// The length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram.
        /// </summary>
        /// <returns> length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram. </returns>
        public int MtuLength()
        {
            return LogBufferDescriptor.MtuLength(_logBuffers.MetaDataBuffer());
        }

        /// <summary>
        /// The initial term at which the stream started for this session.
        /// </summary>
        /// <returns> the initial term id. </returns>
        public int InitialTermId => _initialTermId;

        /// <summary>
        /// The correlationId for identification of the image with the media driver.
        /// </summary>
        /// <returns> the correlationId for identification of the image with the media driver. </returns>
        public virtual long CorrelationId { get; }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> to which this <seealso cref="Image"/> belongs.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> to which this <seealso cref="Image"/> belongs. </returns>
        public Subscription Subscription { get; }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool Closed => _isClosed;

        /// <summary>
        /// Get the position the subscriber joined this stream at.
        /// </summary>
        /// <returns> the position the subscriber joined this stream at.</returns>
        public long JoinPosition()
        {
            return _joinPosition;
        }

        /// <summary>
        /// The position this <seealso cref="Image"/> has been consumed to by the subscriber.
        /// </summary>
        /// <returns> the position this <seealso cref="Image"/> has been consumed to by the subscriber. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Position()
        {
            if (_isClosed)
            {
                return _finalPosition;
            }

            return _subscriberPosition.Get();
        }

        /// <summary>
        /// Set the subscriber position for this <seealso cref="Image"/> to indicate where it has been consumed to.
        /// </summary>
        /// <param name="newPosition"> for the consumption point. </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Position(long newPosition)
        {
            if (_isClosed)
            {
                ThrowHelper.ThrowInvalidOperationException("Image is closed");
            }

            ValidatePosition(newPosition);

            _subscriberPosition.SetOrdered(newPosition);
        }

        /// <summary>
        /// Is the current consumed position at the end of the stream?
        /// </summary>
        /// <returns> true if at the end of the stream or false if not. </returns>
        public virtual bool IsEndOfStream()
        {
            if (_isClosed)
            {
                return _isEos;
            }

            return _subscriberPosition.Get() >= LogBufferDescriptor.EndOfStreamPosition(_logBuffers.MetaDataBuffer());
        }

        ///// <summary>
        ///// The <seealso cref="FileChannel"/> to the raw log of the Image.
        ///// </summary>
        ///// <returns> the <seealso cref="FileChannel"/> to the raw log of the Image. </returns>
        //public FileChannel FileChannel()
        //{
        //    return logBuffers.FileChannel();
        //}

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="FragmentHandler"/> up to a limited number of fragments as specified.
        /// 
        /// Use a <see cref="FragmentAssembler"/> to assemble messages which span multiple fragments.
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="FragmentAssembler" />
        /// <seealso cref="ImageFragmentAssembler" />
#if DEBUG
        public virtual int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
#else
        public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
#endif
        {
            var handler = HandlerHelper.ToFragmentHandler(fragmentHandler);
            return Poll(handler, fragmentLimit);
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="FragmentHandler"/> up to a limited number of fragments as specified.
        /// 
        /// Use a <see cref="FragmentAssembler"/> to assemble messages which span multiple fragments.
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="FragmentAssembler" />
        /// <seealso cref="ImageFragmentAssembler" />
#if DEBUG
        public virtual int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
#else
        public int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
#endif
        {
            if (_isClosed)
            {
                return 0;
            }

            var position = _subscriberPosition.Get();

            return TermReader.Read(
                ActiveTermBuffer(position),
                (int) position & _termLengthMask,
                fragmentHandler,
                fragmentLimit,
                _header,
                _errorHandler,
                position,
                _subscriberPosition);
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IControlledFragmentHandler"/> up to a limited number of fragments as specified.
        ///     
        /// Use a <see cref="ControlledFragmentAssembler"/>. to assemble messages which span multiple fragments.
        /// 
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler" />
        /// <seealso cref="ImageControlledFragmentAssembler" />
        public int ControlledPoll(IControlledFragmentHandler fragmentHandler, int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var fragmentsRead = 0;
            var initialPosition = _subscriberPosition.Get();
            var initialOffset = (int) initialPosition & _termLengthMask;
            var resultingOffset = initialOffset;
            var termBuffer = ActiveTermBuffer(initialPosition);
            int capacity = termBuffer.Capacity;
            _header.Buffer = termBuffer;

            try
            {
                do
                {
                    var length = FrameDescriptor.FrameLengthVolatile(termBuffer, resultingOffset);
                    if (length <= 0)
                    {
                        break;
                    }

                    var frameOffset = resultingOffset;
                    var alignedLength = BitUtil.Align(length, FrameDescriptor.FRAME_ALIGNMENT);
                    resultingOffset += alignedLength;

                    if (FrameDescriptor.IsPaddingFrame(termBuffer, frameOffset))
                    {
                        continue;
                    }

                    _header.Offset = frameOffset;

                    var action = fragmentHandler.OnFragment(
                        termBuffer,
                        frameOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        length - DataHeaderFlyweight.HEADER_LENGTH,
                        _header);

                    if (action == ControlledFragmentHandlerAction.ABORT)
                    {
                        resultingOffset -= alignedLength;
                        break;
                    }

                    ++fragmentsRead;

                    if (action == ControlledFragmentHandlerAction.BREAK)
                    {
                        break;
                    }
                    else if (action == ControlledFragmentHandlerAction.COMMIT)
                    {
                        initialPosition += (resultingOffset - initialOffset);
                        initialOffset = resultingOffset;
                        _subscriberPosition.SetOrdered(initialPosition);
                    }
                } while (fragmentsRead < fragmentLimit && resultingOffset < capacity);
            }
            catch (Exception t)
            {
                _errorHandler(t);
            }
            finally
            {
                long resultingPosition = initialPosition + (resultingOffset - initialOffset);
                if (resultingPosition > initialPosition)
                {
                    _subscriberPosition.SetOrdered(resultingPosition);
                }
            }

            return fragmentsRead;
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="ControlledFragmentHandler"/> up to a limited number of fragments as specified.
        ///     
        /// Use a <see cref="ControlledFragmentAssembler"/>. to assemble messages which span multiple fragments.
        /// 
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler" />
        /// <seealso cref="ImageControlledFragmentAssembler" />
        public int ControlledPoll(ControlledFragmentHandler fragmentHandler, int fragmentLimit)
        {
            var handler = HandlerHelper.ToControlledFragmentHandler(fragmentHandler);
            return ControlledPoll(handler, fragmentLimit);
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IControlledFragmentHandler"/> up to a limited number of fragments as specified or
        /// the maximum position specified.
        /// <para>
        /// Use a <seealso cref="IControlledFragmentHandler"/> to assemble messages which span multiple fragments.
        ///     
        /// </para>
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="maxPosition">     to consume messages up to. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler"/>
        /// <seealso cref="ImageControlledFragmentAssembler"/>
        public virtual int BoundedControlledPoll(IControlledFragmentHandler fragmentHandler, long maxPosition,
            int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var fragmentsRead = 0;
            var initialPosition = _subscriberPosition.Get();
            var initialOffset = (int) initialPosition & _termLengthMask;
            var resultingOffset = initialOffset;
            var termBuffer = ActiveTermBuffer(initialPosition);
            var endOffset = Math.Min(termBuffer.Capacity, (int) (maxPosition - initialPosition + initialOffset));
            _header.Buffer = termBuffer;

            try
            {
                while (fragmentsRead < fragmentLimit && resultingOffset < endOffset)
                {
                    int length = FrameDescriptor.FrameLengthVolatile(termBuffer, resultingOffset);
                    if (length <= 0)
                    {
                        break;
                    }

                    int frameOffset = resultingOffset;
                    int alignedLength = BitUtil.Align(length, FrameDescriptor.FRAME_ALIGNMENT);
                    resultingOffset += alignedLength;

                    if (FrameDescriptor.IsPaddingFrame(termBuffer, frameOffset))
                    {
                        continue;
                    }

                    _header.Offset = frameOffset;

                    var action = fragmentHandler.OnFragment(termBuffer,
                        frameOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        length - DataHeaderFlyweight.HEADER_LENGTH, _header);

                    if (action == ControlledFragmentHandlerAction.ABORT)
                    {
                        resultingOffset -= alignedLength;
                        break;
                    }

                    ++fragmentsRead;

                    if (action == ControlledFragmentHandlerAction.BREAK)
                    {
                        break;
                    }
                    else if (action == ControlledFragmentHandlerAction.COMMIT)
                    {
                        initialPosition += (resultingOffset - initialOffset);
                        initialOffset = resultingOffset;
                        _subscriberPosition.SetOrdered(initialPosition);
                    }
                }
            }
            catch (Exception t)
            {
                _errorHandler(t);
            }
            finally
            {
                long resultingPosition = initialPosition + (resultingOffset - initialOffset);
                if (resultingPosition > initialPosition)
                {
                    _subscriberPosition.SetOrdered(resultingPosition);
                }
            }

            return fragmentsRead;
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IControlledFragmentHandler"/> up to a limited number of fragments as specified or
        /// the maximum position specified.
        /// <para>
        /// Use a <seealso cref="IControlledFragmentHandler"/> to assemble messages which span multiple fragments.
        ///     
        /// </para>
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="maxPosition">     to consume messages up to. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler"/>
        /// <seealso cref="ImageControlledFragmentAssembler"/>
        public virtual int BoundedControlledPoll(ControlledFragmentHandler fragmentHandler, long maxPosition,
            int fragmentLimit)
        {
            var handler = HandlerHelper.ToControlledFragmentHandler(fragmentHandler);
            return BoundedControlledPoll(handler, maxPosition, fragmentLimit);
        }

        /// <summary>
        /// Peek for new messages in a stream by scanning forward from an initial position. If new messages are found then
        /// they will be delivered to the <seealso cref="IControlledFragmentHandler"/> up to a limited position.
        ///    
        /// Use a <seealso cref="ControlledFragmentAssembler"/> to assemble messages which span multiple fragments. Scans must also
        /// start at the beginning of a message so that the assembler is reset.
        /// 
        /// </summary>
        /// <param name="initialPosition"> from which to peek forward. </param>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="limitPosition">   up to which can be scanned. </param>
        /// <returns> the resulting position after the scan terminates which is a complete message. </returns>
        /// <seealso cref="ControlledFragmentAssembler"/>
        /// <seealso cref="ImageControlledFragmentAssembler"/>
        public virtual long ControlledPeek(long initialPosition, IControlledFragmentHandler fragmentHandler, long limitPosition)
        {
            if (_isClosed)
            {
                return 0;
            }

            ValidatePosition(initialPosition);

            int initialOffset = (int) initialPosition & _termLengthMask;
            int offset = initialOffset;
            long position = initialPosition;
            UnsafeBuffer termBuffer = ActiveTermBuffer(initialPosition);
            int capacity = termBuffer.Capacity;
            _header.Buffer = termBuffer;
            long resultingPosition = initialPosition;

            try
            {
                do
                {
                    int length = FrameDescriptor.FrameLengthVolatile(termBuffer, offset);
                    if (length <= 0)
                    {
                        break;
                    }

                    int frameOffset = offset;
                    var alignedLength = BitUtil.Align(length, FrameDescriptor.FRAME_ALIGNMENT);
                    offset += alignedLength;

                    if (FrameDescriptor.IsPaddingFrame(termBuffer, frameOffset))
                    {
                        continue;
                    }

                    _header.Offset = frameOffset;


                    var action = fragmentHandler.OnFragment(
                        termBuffer,
                        frameOffset + DataHeaderFlyweight.HEADER_LENGTH,
                        length - DataHeaderFlyweight.HEADER_LENGTH,
                        _header);

                    if (action == ControlledFragmentHandlerAction.ABORT)
                    {
                        break;
                    }

                    position += (offset - initialOffset);
                    initialOffset = offset;

                    if ((_header.Flags & FrameDescriptor.END_FRAG_FLAG) == FrameDescriptor.END_FRAG_FLAG)
                    {
                        resultingPosition = position;
                    }

                    if (action == ControlledFragmentHandlerAction.BREAK)
                    {
                        break;
                    }
                } while (position < limitPosition && offset < capacity);
            }
            catch (Exception t)
            {
                _errorHandler(t);
            }

            return resultingPosition;
        }

        public virtual long ControlledPeek(long initialPosition, ControlledFragmentHandler fragmentHandler, long limitPosition)
        {
            var handler = HandlerHelper.ToControlledFragmentHandler(fragmentHandler);
            return ControlledPeek(initialPosition, handler, limitPosition);
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IBlockHandler"/> up to a limited number of bytes.
        /// </summary>
        /// <param name="blockHandler">     to which block is delivered. </param>
        /// <param name="blockLengthLimit"> up to which a block may be in length. </param>
        /// <returns> the number of bytes that have been consumed. </returns>
        public int BlockPoll(BlockHandler blockHandler, int blockLengthLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var position = _subscriberPosition.Get();
            var termOffset = (int) position & _termLengthMask;
            var termBuffer = ActiveTermBuffer(position);
            var limit = Math.Min(termOffset + blockLengthLimit, termBuffer.Capacity);

            var resultingOffset = TermBlockScanner.Scan(termBuffer, termOffset, limit);

            var bytesConsumed = resultingOffset - termOffset;
            if (resultingOffset > termOffset)
            {
                try
                {
                    var termId = termBuffer.GetInt(termOffset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET);

                    blockHandler(termBuffer, termOffset, bytesConsumed, SessionId, termId);
                }
                catch (Exception t)
                {
                    _errorHandler(t);
                }
                finally
                {
                    _subscriberPosition.SetOrdered(position + bytesConsumed);
                }
            }

            return bytesConsumed;
        }

        // TODO
        ///// <summary>
        ///// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        ///// will be delivered to the <seealso cref="IRawBlockHandler"/> up to a limited number of bytes.
        ///// </summary>
        ///// <param name="fileBlockHandler"> to which block is delivered. </param>
        ///// <param name="blockLengthLimit"> up to which a block may be in length. </param>
        ///// <returns> the number of bytes that have been consumed. </returns>
        //public int RawPoll(IRawBlockHandler rawBlockHandler, int blockLengthLimit)
        //{
        //    if (_isClosed)
        //    {
        //        return 0;
        //    }

        //    long position = _subscriberPosition.Get();
        //    int termOffset = (int)position & _termLengthMask;
        //    int activeIndex = LogBufferDescriptor.IndexByPosition(position, _positionBitsToShift);
        //    UnsafeBuffer termBuffer = _termBuffers[activeIndex];
        //    int capacity = termBuffer.Capacity;
        //    int limit = Math.Min(termOffset + blockLengthLimit, capacity);

        //    int resultingOffset = TermBlockScanner.Scan(termBuffer, termOffset, limit);

        //    int length = resultingOffset - termOffset;
        //    if (resultingOffset > termOffset)
        //    {
        //        try
        //        {
        //            long fileOffset = ((long)capacity * activeIndex) + termOffset;
        //            int termId = termBuffer.GetInt(termOffset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET);

        //            rawBlockHandler.OnBlock(_logBuffers.FileChannel(), fileOffset, termBuffer, termOffset, length, _sessionId, termId);
        //        }
        //        catch (Exception t)
        //        {
        //            _errorHandler(t);
        //        }

        //        _subscriberPosition.SetOrdered(position + length);
        //    }

        //    return length;
        //}

        private UnsafeBuffer ActiveTermBuffer(long position)
        {
            return _termBuffers[LogBufferDescriptor.IndexByPosition(position, _positionBitsToShift)];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidatePosition(long newPosition)
        {
            long currentPosition = _subscriberPosition.Get();
            long limitPosition = currentPosition + TermBufferLength;
            if (newPosition < currentPosition || newPosition > limitPosition)
            {
                ThrowHelper.ThrowArgumentException("newPosition of " + newPosition + " out of range " + currentPosition + "-" + limitPosition);
            }

            if (0 != (newPosition & (FrameDescriptor.FRAME_ALIGNMENT - 1)))
            {
                ThrowHelper.ThrowArgumentException("newPosition of " + newPosition + " not aligned to FRAME_ALIGNMENT");
            }
        }

        internal LogBuffers LogBuffers()
        {
            return _logBuffers;
        }

        internal void Close()
        {
            _finalPosition = _subscriberPosition.GetVolatile();
            _isEos = _finalPosition >= LogBufferDescriptor.EndOfStreamPosition(_logBuffers.MetaDataBuffer());
            _isClosed = true;
        }
    }
}