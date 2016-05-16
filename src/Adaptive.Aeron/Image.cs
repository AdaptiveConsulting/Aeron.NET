using System;
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
    /// </summary>
    public class Image
    {
        private readonly int _termLengthMask;
        private readonly int _positionBitsToShift;
        private volatile bool _isClosed;

        private readonly IPosition _subscriberPosition;
        private readonly UnsafeBuffer[] _termBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];
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

            var buffers = logBuffers.AtomicBuffers();
            Array.Copy(buffers, 0, _termBuffers, 0, LogBufferDescriptor.PARTITION_COUNT);

            var termLength = logBuffers.TermLength();
            _termLengthMask = termLength - 1;
            _positionBitsToShift = IntUtil.NumberOfTrailingZeros(termLength);
            _header = new Header(LogBufferDescriptor.InitialTermId(buffers[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX]), _positionBitsToShift);
        }

        /// <summary>
        /// Get the length in bytes for each term partition in the log buffer.
        /// </summary>
        /// <returns> the length in bytes for each term partition in the log buffer. </returns>
        public int TermBufferLength=> _logBuffers.TermLength();

        /// <summary>
        /// The sessionId for the steam of messages.
        /// </summary>
        /// <returns> the sessionId for the steam of messages. </returns>
        public int SessionId { get; }

        /// <summary>
        /// The source identity of the sending publisher as an abstract concept appropriate for the media.
        /// </summary>
        /// <returns> source identity of the sending publisher as an abstract concept appropriate for the media. </returns>
        public string SourceIdentity { get; }


        /// <summary>
        /// The initial term at which the stream started for this session.
        /// </summary>
        /// <returns> the initial term id. </returns>
        public int InitialTermId => _header.InitialTermId();

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
        /// The position this <seealso cref="Image"/> has been consumed to by the subscriber.
        /// </summary>
        /// <returns> the position this <seealso cref="Image"/> has been consumed to by the subscriber. </returns>
        public long Position
        {
            get
            {
                if (_isClosed)
                {
                    return 0;
                }

                return _subscriberPosition.Get();
            }
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
        /// will be delivered to the <seealso cref="IFragmentHandler"/> up to a limited number of fragments as specified.
        /// 
        /// To assemble messages that span multiple fragments then use <seealso cref="FragmentAssembler"/>.
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="FragmentAssembler" />
        public virtual int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var position = _subscriberPosition.Get();
            var termOffset = (int) position & _termLengthMask;
            var termBuffer = ActiveTermBuffer(position);

            var outcome = TermReader.Read(termBuffer, termOffset, fragmentHandler, fragmentLimit, _header, _errorHandler);

            UpdatePosition(position, termOffset, TermReader.Offset(outcome));

            return TermReader.FragmentsRead(outcome);
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IControlledFragmentHandler"/> up to a limited number of fragments as specified.
        ///     
        /// To assemble messages that span multiple fragments then use <seealso cref="ControlledFragmentAssembler"/>.
        /// </summary>
        /// <param name="fragmentHandler"> to which message fragments are delivered. </param>
        /// <param name="fragmentLimit">   for the number of fragments to be consumed during one polling operation. </param>
        /// <returns> the number of fragments that have been consumed. </returns>
        /// <seealso cref="ControlledFragmentAssembler" />
        public int ControlledPoll(IControlledFragmentHandler fragmentHandler, int fragmentLimit)
        {
            if (_isClosed)
            {
                return 0;
            }

            var position = _subscriberPosition.Get();
            var termOffset = (int) position & _termLengthMask;
            var offset = termOffset;
            var fragmentsRead = 0;
            var termBuffer = ActiveTermBuffer(position);

            try
            {
                var capacity = termBuffer.Capacity;
                do
                {
                    var length = FrameDescriptor.FrameLengthVolatile(termBuffer, offset);
                    if (length <= 0)
                    {
                        break;
                    }

                    var frameOffset = offset;
                    var alignedLength = BitUtil.Align(length, FrameDescriptor.FRAME_ALIGNMENT);
                    offset += alignedLength;

                    if (!FrameDescriptor.IsPaddingFrame(termBuffer, frameOffset))
                    {
                        _header.Buffer(termBuffer);
                        _header.Offset(frameOffset);

                        var action = fragmentHandler.OnFragment(termBuffer, frameOffset + DataHeaderFlyweight.HEADER_LENGTH, length - DataHeaderFlyweight.HEADER_LENGTH, _header);

                        ++fragmentsRead;

                        if (action == ControlledFragmentHandlerAction.BREAK)
                        {
                            break;
                        }
                        if (action == ControlledFragmentHandlerAction.ABORT)
                        {
                            --fragmentsRead;
                            offset = frameOffset;
                            break;
                        }
                        if (action == ControlledFragmentHandlerAction.COMMIT)
                        {
                            position += alignedLength;
                            termOffset = offset;
                            _subscriberPosition.SetOrdered(position);
                        }
                    }
                } while (fragmentsRead < fragmentLimit && offset < capacity);
            }
            catch (Exception t)
            {
                _errorHandler(t);
            }

            UpdatePosition(position, termOffset, offset);

            return fragmentsRead;
        }

        /// <summary>
        /// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        /// will be delivered to the <seealso cref="IBlockHandler"/> up to a limited number of bytes.
        /// </summary>
        /// <param name="blockHandler">     to which block is delivered. </param>
        /// <param name="blockLengthLimit"> up to which a block may be in length. </param>
        /// <returns> the number of bytes that have been consumed. </returns>
        public int BlockPoll(IBlockHandler blockHandler, int blockLengthLimit)
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

                    blockHandler.OnBlock(termBuffer, termOffset, bytesConsumed, SessionId, termId);
                }
                catch (Exception t)
                {
                    _errorHandler(t);
                }

                _subscriberPosition.SetOrdered(position + bytesConsumed);
            }

            return bytesConsumed;
        }

        // TODO
        ///// <summary>
        ///// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
        ///// will be delivered to the <seealso cref="IFileBlockHandler"/> up to a limited number of bytes.
        ///// </summary>
        ///// <param name="fileBlockHandler"> to which block is delivered. </param>
        ///// <param name="blockLengthLimit"> up to which a block may be in length. </param>
        ///// <returns> the number of bytes that have been consumed. </returns>
        //public int FilePoll(IFileBlockHandler fileBlockHandler, int blockLengthLimit)
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

        //    int bytesConsumed = resultingOffset - termOffset;
        //    if (resultingOffset > termOffset)
        //    {
        //        try
        //        {
        //            long offset = ((long)capacity * activeIndex) + termOffset;
        //            int termId = termBuffer.GetInt(termOffset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET);

        //            fileBlockHandler.OnBlock(_logBuffers.FileChannel(), offset, bytesConsumed, _sessionId, termId);
        //        }
        //        catch (Exception t)
        //        {
        //            _errorHandler(t);
        //        }

        //        _subscriberPosition.SetOrdered(position + bytesConsumed);
        //    }

        //    return bytesConsumed;
        //}

        private void UpdatePosition(long positionBefore, int offsetBefore, int offsetAfter)
        {
            var position = positionBefore + (offsetAfter - offsetBefore);
            if (position > positionBefore)
            {
                _subscriberPosition.SetOrdered(position);
            }
        }

        private UnsafeBuffer ActiveTermBuffer(long position)
        {
            return _termBuffers[LogBufferDescriptor.IndexByPosition(position, _positionBitsToShift)];
        }

        internal IManagedResource ManagedResource()
        {
            _isClosed = true;
            return new ImageManagedResource(this);
        }

        private class ImageManagedResource : IManagedResource
        {
            private long _timeOfLastStateChange;
            private readonly Image _image;

            public ImageManagedResource(Image image)
            {
                _image = image;
            }

            public void TimeOfLastStateChange(long time)
            {
                _timeOfLastStateChange = time;
            }

            public long TimeOfLastStateChange()
            {
                return _timeOfLastStateChange;
            }

            public void Delete()
            {
                _image._logBuffers.Dispose();
            }
        }
    }
}