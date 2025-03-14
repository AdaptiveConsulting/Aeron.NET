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
using static Adaptive.Aeron.LogBuffer.FrameDescriptor;
using static Adaptive.Aeron.Protocol.DataHeaderFlyweight;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Represents a replicated <see cref="Publication"/> from a publisher which matches a <seealso cref="Subscription"/>.
    /// Each <seealso cref="Image"/> identifies a source <see cref="Publication"/> by <see cref="SessionId"/>.
    /// 
    /// By default, fragmented messages are not reassembled before delivery. If an application must
    /// receive whole messages, whether they were fragmented or not, then the Subscriber
    /// should be created with a <seealso cref="FragmentAssembler"/> or a custom implementation.
    /// 
    /// It is an application's responsibility to <seealso cref="Poll(Adaptive.Aeron.LogBuffer.FragmentHandler,int)"/> the <seealso cref="Image"/> for new messages.
    /// 
    /// <b>Note:</b>Images are not threadsafe and should not be shared between subscribers.
    /// </summary>
    public class Image
    {
        private readonly long _joinPosition;
        private long _finalPosition;
        private readonly int _initialTermId;

        private readonly int _termLengthMask;

        private long _eosPosition = long.MaxValue;
        private bool _isEos;
        private volatile bool _isClosed;

        private readonly IPosition _subscriberPosition;
        private readonly UnsafeBuffer[] _termBuffers;
        private readonly Header _header;
        private readonly IErrorHandler _errorHandler;
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
        public Image(Subscription subscription, int sessionId, IPosition subscriberPosition, LogBuffers logBuffers,
            IErrorHandler errorHandler, string sourceIdentity, long correlationId)
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
            PositionBitsToShift = LogBufferDescriptor.PositionBitsToShift(termLength);
            _initialTermId = LogBufferDescriptor.InitialTermId(logBuffers.MetaDataBuffer());
            _header = new Header(LogBufferDescriptor.InitialTermId(logBuffers.MetaDataBuffer()), PositionBitsToShift,
                this);
        }

        /// <summary>
        /// Number of bits to right shift a position to get a term count for how far the stream has progressed.
        /// </summary>
        /// <returns> of bits to right shift a position to get a term count for how far the stream has progressed. </returns>
        public int PositionBitsToShift { get; }

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
        public int MtuLength => LogBufferDescriptor.MtuLength(_logBuffers.MetaDataBuffer());

        /// <summary>
        /// The initial term at which the stream started for this session.
        /// </summary>
        /// <returns> the initial term id. </returns>
        public int InitialTermId => _initialTermId;

        /// <summary>
        /// The correlationId for identification of the image with the media driver.
        /// </summary>
        /// <returns> the correlationId for identification of the image with the media driver. </returns>
        public long CorrelationId { get; }

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
        public long JoinPosition => _joinPosition;

        /// <summary>
        /// The position this <seealso cref="Image"/> has been consumed to by the subscriber.
        /// </summary>
        /// <returns> the position this <seealso cref="Image"/> has been consumed to by the subscriber. </returns>
        public long Position
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_isClosed)
                {
                    return _finalPosition;
                }

                return _subscriberPosition.Get();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (!_isClosed)
                {
                    ValidatePosition(value);
                    _subscriberPosition.SetOrdered(value);
                }
            }
        }

        /// <summary>
        /// The counter id for the subscriber position counter.
        /// </summary>
        /// <returns> the id for the subscriber position counter. </returns>
        public int SubscriberPositionId => _subscriberPosition.Id;

        /// <summary>
        /// Is the current consumed position at the end of the stream?
        /// </summary>
        /// <returns> true if at the end of the stream or false if not. </returns>
        public bool IsEndOfStream
        {
            get
            {
                if (_isClosed)
                {
                    return _isEos;
                }

                return _subscriberPosition.Get() >=
                       LogBufferDescriptor.EndOfStreamPosition(_logBuffers.MetaDataBuffer());
            }
        }
        
        /// <summary>
        /// The position the stream reached when EOS was received from the publisher. The position will be
        /// <seealso cref="long.MaxValue"/> until the stream ends and EOS is set.
        /// </summary>
        /// <returns> position the stream reached when EOS was received from the publisher. </returns>
        public long EndOfStreamPosition
        {
            get
            {
                if (_isClosed)
                {
                    return _eosPosition;
                }

                return LogBufferDescriptor.EndOfStreamPosition(_logBuffers.MetaDataBuffer());
            }
        }

        /// <summary>
        /// Count of observed active transports within the image liveness timeout.
        ///   
        /// If the image is closed, then this is 0. This may also be 0 if no actual datagrams have arrived. IPC
        /// Images also will be 0.
        /// </summary>
        /// <returns> count of active transports - 0 if Image is closed, no datagrams yet, or IPC. </returns>
        public int ActiveTransportCount()
        {
            if (_isClosed)
            {
                return 0;
            }

            return LogBufferDescriptor.ActiveTransportCount(_logBuffers.MetaDataBuffer());
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
        public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
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
        public int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            int fragmentsRead = 0;
            long initialPosition = _subscriberPosition.Get();
            int initialOffset = (int)initialPosition & _termLengthMask;
            int offset = initialOffset;
            UnsafeBuffer termBuffer = ActiveTermBuffer(initialPosition);
            int capacity = termBuffer.Capacity;
            Header header = _header;
            header.Buffer = termBuffer;

            try
            {
                while (fragmentsRead < fragmentLimit && offset < capacity && !_isClosed)
                {
                    int frameLength = FrameLengthVolatile(termBuffer, offset);
                    if (frameLength <= 0)
                    {
                        break;
                    }

                    int frameOffset = offset;
                    offset += BitUtil.Align(frameLength, FRAME_ALIGNMENT);

                    if (!IsPaddingFrame(termBuffer, frameOffset))
                    {
                        ++fragmentsRead;
                        header.Offset = frameOffset;
                        fragmentHandler.OnFragment(termBuffer, frameOffset + HEADER_LENGTH, frameLength - HEADER_LENGTH,
                            header);
                    }
                }
            }
            catch (Exception ex)
            {
                _errorHandler.OnError(ex);
            }
            finally
            {
                long newPosition = initialPosition + (offset - initialOffset);
                if (newPosition > initialPosition)
                {
                    _subscriberPosition.SetOrdered(newPosition);
                }
            }

            return fragmentsRead;
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IControlledFragmentHandler"/> up to a limited number of fragments as specified.
        ///     
        /// Use a <see cref="ControlledFragmentAssembler"/>. to assemble messages which span multiple fragments.
        /// 
        /// </summary>
        /// <param name="handler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler" />
        /// <seealso cref="ImageControlledFragmentAssembler" />
        public int ControlledPoll(IControlledFragmentHandler handler, int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var fragmentsRead = 0;
            var initialPosition = _subscriberPosition.Get();
            var initialOffset = (int) initialPosition & _termLengthMask;
            var offset = initialOffset;
            var termBuffer = ActiveTermBuffer(initialPosition);
            int capacity = termBuffer.Capacity;
            var header = _header;
            header.Buffer = termBuffer;

            try
            {
                while (fragmentsRead < fragmentLimit && offset < capacity && !_isClosed)
                {
                    var length = FrameLengthVolatile(termBuffer, offset);
                    if (length <= 0)
                    {
                        break;
                    }

                    var frameOffset = offset;
                    var alignedLength = BitUtil.Align(length, FRAME_ALIGNMENT);
                    offset += alignedLength;

                    if (IsPaddingFrame(termBuffer, frameOffset))
                    {
                        continue;
                    }

                    ++fragmentsRead;
                    header.Offset = frameOffset;

                    var action = handler.OnFragment(
                        termBuffer,
                        frameOffset + HEADER_LENGTH,
                        length - HEADER_LENGTH,
                        header);

                    if (ControlledFragmentHandlerAction.ABORT == action)
                    {
                        --fragmentsRead;
                        offset -= alignedLength;
                        break;
                    }

                    if (ControlledFragmentHandlerAction.BREAK == action)
                    {
                        break;
                    }

                    if (ControlledFragmentHandlerAction.COMMIT == action)
                    {
                        initialPosition += (offset - initialOffset);
                        initialOffset = offset;
                        _subscriberPosition.SetOrdered(initialPosition);
                    }
                }
            }
            catch (Exception ex)
            {
                _errorHandler.OnError(ex);
            }
            finally
            {
                long resultingPosition = initialPosition + (offset - initialOffset);
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
        /// <param name="handler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler" />
        /// <seealso cref="ImageControlledFragmentAssembler" />
        public int ControlledPoll(ControlledFragmentHandler handler, int fragmentLimit)
        {
            var fragmentHandler = HandlerHelper.ToControlledFragmentHandler(handler);
            return ControlledPoll(fragmentHandler, fragmentLimit);
        }
        
        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="FragmentHandler"/> up to a limited number of fragments as specified or
        /// the maximum position specified.
        /// <para>
        /// Use a <seealso cref="FragmentAssembler"/> to assemble messages which span multiple fragments.
        ///    
        /// </para>
        /// </summary>
        /// <param name="handler">       to which message fragments are delivered. </param>
        /// <param name="limitPosition"> to consume messages up to. </param>
        /// <param name="fragmentLimit"> for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="FragmentAssembler" />
        /// <seealso cref="ImageFragmentAssembler" />
        public int BoundedPoll(IFragmentHandler handler, long limitPosition, int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            long initialPosition = _subscriberPosition.Get();
            if (initialPosition >= limitPosition)
            {
                return 0;
            }

            int fragmentsRead = 0;
            int initialOffset = (int) initialPosition & _termLengthMask;
            int offset = initialOffset;
            UnsafeBuffer termBuffer = ActiveTermBuffer(initialPosition);
            int limitOffset = (int) Math.Min(termBuffer.Capacity, (limitPosition - initialPosition) + offset);
            Header header = _header;
            header.Buffer = termBuffer;

            try
            {
                while (fragmentsRead < fragmentLimit && offset < limitOffset && !_isClosed)
                {
                    int length = FrameLengthVolatile(termBuffer, offset);
                    if (length <= 0)
                    {
                        break;
                    }

                    int frameOffset = offset;
                    int alignedLength = BitUtil.Align(length, FRAME_ALIGNMENT);
                    offset += alignedLength;

                    if (IsPaddingFrame(termBuffer, frameOffset))
                    {
                        continue;
                    }

                    ++fragmentsRead;
                    header.Offset = frameOffset;
                    handler.OnFragment(termBuffer, frameOffset + HEADER_LENGTH, length - HEADER_LENGTH, header);
                }
            }
            catch (Exception ex)
            {
                _errorHandler.OnError(ex);
            }
            finally
            {
                long resultingPosition = initialPosition + (offset - initialOffset);
                if (resultingPosition > initialPosition)
                {
                    _subscriberPosition.SetOrdered(resultingPosition);
                }
            }

            return fragmentsRead;
        }
        

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="FragmentHandler"/> up to a limited number of fragments as specified or
        /// the maximum position specified.
        /// <para>
        /// Use a <seealso cref="FragmentAssembler"/> to assemble messages which span multiple fragments.
        ///    
        /// </para>
        /// </summary>
        /// <param name="handler">       to which message fragments are delivered. </param>
        /// <param name="limitPosition"> to consume messages up to. </param>
        /// <param name="fragmentLimit"> for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="FragmentAssembler" />
        /// <seealso cref="ImageFragmentAssembler" />
        public int BoundedPoll(FragmentHandler handler, long limitPosition, int fragmentLimit)
        {
            var fragmentHandler = HandlerHelper.ToFragmentHandler(handler);
            return BoundedPoll(fragmentHandler, limitPosition, fragmentLimit);
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
        /// <param name="handler"> to which message fragments are delivered. </param>
        /// <param name="limitPosition">     to consume messages up to. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler"/>
        /// <seealso cref="ImageControlledFragmentAssembler"/>
        public int BoundedControlledPoll(IControlledFragmentHandler handler, long limitPosition,
            int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            IPosition subscriberPosition = this._subscriberPosition;
            var initialPosition = subscriberPosition.Get();
            if (initialPosition >= limitPosition)
            {
                return 0;
            }
            
            var fragmentsRead = 0;
            var initialOffset = (int) initialPosition & _termLengthMask;
            var offset = initialOffset;
            var termBuffer = ActiveTermBuffer(initialPosition);
            var limitOffset = (int) Math.Min(termBuffer.Capacity, (limitPosition - initialPosition + initialOffset));
            var header = _header;
            header.Buffer = termBuffer;

            try
            {
                while (fragmentsRead < fragmentLimit && offset < limitOffset && !_isClosed)
                {
                    int length = FrameLengthVolatile(termBuffer, offset);
                    if (length <= 0)
                    {
                        break;
                    }

                    int frameOffset = offset;
                    int alignedLength = BitUtil.Align(length, FRAME_ALIGNMENT);
                    offset += alignedLength;

                    if (IsPaddingFrame(termBuffer, frameOffset))
                    {
                        continue;
                    }

                    ++fragmentsRead;
                    header.Offset = frameOffset;

                    var action = handler.OnFragment(termBuffer,
                        frameOffset + HEADER_LENGTH,
                        length - HEADER_LENGTH, header);

                    if (ControlledFragmentHandlerAction.ABORT == action)
                    {
                        --fragmentsRead;
                        offset -= alignedLength;
                        break;
                    }
                   
                    if (ControlledFragmentHandlerAction.BREAK == action)
                    {
                        break;
                    }
                    
                    if (ControlledFragmentHandlerAction.COMMIT == action)
                    {
                        initialPosition += (offset - initialOffset);
                        initialOffset = offset;
                        _subscriberPosition.SetOrdered(initialPosition);
                    }
                }
            }
            catch (Exception ex)
            {
                _errorHandler.OnError(ex);
            }
            finally
            {
                long resultingPosition = initialPosition + (offset - initialOffset);
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
        /// <param name="handler"> to which message fragments are delivered. </param>
        /// <param name="limitPosition">     to consume messages up to. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler"/>
        /// <seealso cref="ImageControlledFragmentAssembler"/>
        public int BoundedControlledPoll(ControlledFragmentHandler handler, long limitPosition,
            int fragmentLimit)
        {
            var fragmentHandler = HandlerHelper.ToControlledFragmentHandler(handler);
            return BoundedControlledPoll(fragmentHandler, limitPosition, fragmentLimit);
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
        /// <param name="handler"> to which message fragments are delivered. </param>
        /// <param name="limitPosition">   up to which can be scanned. </param>
        /// <returns> the resulting position after the scan terminates which is a complete message. </returns>
        /// <seealso cref="ControlledFragmentAssembler"/>
        /// <seealso cref="ImageControlledFragmentAssembler"/>
        public long ControlledPeek(long initialPosition, IControlledFragmentHandler handler, long limitPosition)
        {
            if (_isClosed)
            {
                return initialPosition;
            }

            ValidatePosition(initialPosition);
            if (initialPosition >= limitPosition)
            {
                return initialPosition;
            }

            int initialOffset = (int) initialPosition & _termLengthMask;
            int offset = initialOffset;
            long position = initialPosition;
            UnsafeBuffer termBuffer = ActiveTermBuffer(initialPosition);
            var header = _header;
            int limitOffset = (int) Math.Min(termBuffer.Capacity, (limitPosition - initialPosition) + offset);
            _header.Buffer = termBuffer;
            long resultingPosition = initialPosition;

            try
            {
                while (offset < limitOffset && !_isClosed)
                {
                    int length = FrameLengthVolatile(termBuffer, offset);
                    if (length <= 0)
                    {
                        break;
                    }

                    int frameOffset = offset;
                    offset += BitUtil.Align(length, FRAME_ALIGNMENT);

                    if (IsPaddingFrame(termBuffer, frameOffset))
                    {
                        position += (offset - initialOffset);
                        initialOffset = offset;
                        resultingPosition = position;

                        continue;
                    }

                    _header.Offset = frameOffset;


                    var action = handler.OnFragment(
                        termBuffer,
                        frameOffset + HEADER_LENGTH,
                        length - HEADER_LENGTH,
                        _header);

                    if (ControlledFragmentHandlerAction.ABORT == action)
                    {
                        break;
                    }

                    position += (offset - initialOffset);
                    initialOffset = offset;

                    if ((_header.Flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                    {
                        resultingPosition = position;
                    }

                    if (ControlledFragmentHandlerAction.BREAK == action)
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                _errorHandler.OnError(ex);
            }

            return resultingPosition;
        }

        public long ControlledPeek(long initialPosition, ControlledFragmentHandler handler, long limitPosition)
        {
            var fragmentHandler = HandlerHelper.ToControlledFragmentHandler(handler);
            return ControlledPeek(initialPosition, fragmentHandler, limitPosition);
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IBlockHandler"/> up to a limited number of bytes.
        ///
        /// A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
        /// for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
        /// at the offset the padding frame begins. Padding frames are delivered singularly in a block.
        ///
        /// Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
        /// relevant length of the frame is <see cref="DataHeaderFlyweight.HEADER_LENGTH"/>
        /// </summary>
        /// <param name="handler">     to which block is delivered. </param>
        /// <param name="blockLengthLimit"> up to which a block may be in length. </param>
        /// <returns> the number of bytes that have been consumed. </returns>
        public int BlockPoll(BlockHandler handler, int blockLengthLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var position = _subscriberPosition.Get();
            var offset = (int) position & _termLengthMask;
            var limitOffset = Math.Min(offset + blockLengthLimit, _termLengthMask + 1);
            var termBuffer = ActiveTermBuffer(position);
            var resultingOffset = TermBlockScanner.Scan(termBuffer, offset, limitOffset);
            var length = resultingOffset - offset;

            if (resultingOffset > offset)
            {
                try
                {
                    var termId = termBuffer.GetInt(offset + TERM_ID_FIELD_OFFSET);

                    handler(termBuffer, offset, length, SessionId, termId);
                }
                catch (Exception ex)
                {
                    _errorHandler.OnError(ex);
                }
                finally
                {
                    _subscriberPosition.SetOrdered(position + length);
                }
            }

            return length;
        }

        void Reject(string reason)
        {
            Subscription.RejectImage(CorrelationId, Position, reason);
        }
        
        private UnsafeBuffer ActiveTermBuffer(long position)
        {
            return _termBuffers[LogBufferDescriptor.IndexByPosition(position, PositionBitsToShift)];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidatePosition(long position)
        {
            long currentPosition = _subscriberPosition.Get();
            long limitPosition = (currentPosition - (currentPosition & _termLengthMask)) + _termLengthMask + 1;
            if (position < currentPosition || position > limitPosition)
            {
                ThrowHelper.ThrowArgumentException(
                    $"{position} position out of range {currentPosition}-{limitPosition}");
            }

            if (0 != (position & (FRAME_ALIGNMENT - 1)))
            {
                ThrowHelper.ThrowArgumentException($"{position} position not aligned to FRAME_ALIGNMENT");
            }
        }

        internal LogBuffers LogBuffers => _logBuffers;

        internal void Close()
        {
            _finalPosition = _subscriberPosition.GetVolatile();
            _eosPosition = LogBufferDescriptor.EndOfStreamPosition(_logBuffers.MetaDataBuffer());
            _isEos = _finalPosition >= _eosPosition;
            _isClosed = true;
        }

        public override string ToString()
        {
            return "Image{" +
                   $"correlationId={CorrelationId}, " +
                   $"sessionId={SessionId}, " +
                   $"isClosed={_isClosed}, " +
                   $"isEos={IsEndOfStream}, " +
                   $"initialTermId={InitialTermId}, " +
                   $"termLength={TermBufferLength}, " +
                   $"joinPosition={JoinPosition}, " +
                   $"position={Position}, " +
                   $"endOfStreamPosition={EndOfStreamPosition}, " +
                   $"activeTransportCount={ActiveTransportCount()}, " +
                   $"sourceIdentity='{SourceIdentity}', " +
                   $"subscription={Subscription}" +
                   '}';
        }
    }
}