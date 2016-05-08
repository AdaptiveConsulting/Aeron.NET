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
    /// Aeron Publisher API for sending messages to subscribers of a given channel and streamId pair. Publishers
    /// are created via an <seealso cref="Aeron"/> object, and messages are sent via an offer method or a claim and commit
    /// method combination.
    /// <para>
    /// The APIs used to send are all non-blocking.
    /// </para>
    /// <para>
    /// Note: Publication instances are threadsafe and can be shared between publishing threads.
    /// </para>
    /// </summary>
    /// <seealso cref="Aeron.AddPublication(string, int)" />
    public class Publication : IDisposable
    {
        /// <summary>
        /// The publication is not yet connected to a subscriber.
        /// </summary>
        public const long NOT_CONNECTED = -1;

        /// <summary>
        /// The offer failed due to back pressure from the subscribers preventing further transmission.
        /// </summary>
        public const long BACK_PRESSURED = -2;

        /// <summary>
        /// The offer failed due to an administration action and should be retried.
        /// </summary>
        public const long ADMIN_ACTION = -3;

        /// <summary>
        /// The <seealso cref="Publication"/> has been closed and should no longer be used.
        /// </summary>
        public const long CLOSED = -4;

        private readonly long registrationId;
        private int refCount = 0;
        private readonly int streamId;
        private readonly int sessionId;
        private readonly int initialTermId;
        private readonly int maxMessageLength;
        private readonly int maxPayloadLength;
        private readonly int positionBitsToShift;
        private volatile bool isClosed = false;

        private readonly TermAppender[] termAppenders = new TermAppender[LogBufferDescriptor.PARTITION_COUNT];
        private readonly IReadablePosition positionLimit;
        private readonly UnsafeBuffer logMetaDataBuffer;
        private readonly HeaderWriter headerWriter;
        private readonly LogBuffers logBuffers;
        private readonly ClientConductor clientConductor;
        private readonly string channel;

        internal Publication(ClientConductor clientConductor, string channel, int streamId, int sessionId, IReadablePosition positionLimit, LogBuffers logBuffers, long registrationId)
        {
            UnsafeBuffer[] buffers = logBuffers.AtomicBuffers();
            UnsafeBuffer logMetaDataBuffer = buffers[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX];

            for (int i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                termAppenders[i] = new TermAppender(buffers[i], buffers[i + LogBufferDescriptor.PARTITION_COUNT]);
            }

            int termLength = logBuffers.TermLength();
            this.maxPayloadLength = LogBufferDescriptor.MtuLength(logMetaDataBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
            this.maxMessageLength = FrameDescriptor.ComputeMaxMessageLength(termLength);
            this.clientConductor = clientConductor;
            this.channel = channel;
            this.streamId = streamId;
            this.sessionId = sessionId;
            this.initialTermId = LogBufferDescriptor.InitialTermId(logMetaDataBuffer);
            this.logMetaDataBuffer = logMetaDataBuffer;
            this.registrationId = registrationId;
            this.positionLimit = positionLimit;
            this.logBuffers = logBuffers;
            this.positionBitsToShift = IntUtil.NumberOfTrailingZeros(termLength);
            this.headerWriter = new HeaderWriter(LogBufferDescriptor.DefaultFrameHeader(logMetaDataBuffer));
        }

        /// <summary>
        /// Get the length in bytes for each term partition in the log buffer.
        /// </summary>
        /// <returns> the length in bytes for each term partition in the log buffer. </returns>
        public int TermBufferLength()
        {
            return logBuffers.TermLength();
        }

        /// <summary>
        /// Media address for delivery to the channel.
        /// </summary>
        /// <returns> Media address for delivery to the channel. </returns>
        public string Channel()
        {
            return channel;
        }

        /// <summary>
        /// Stream identity for scoping within the channel media address.
        /// </summary>
        /// <returns> Stream identity for scoping within the channel media address. </returns>
        public int StreamId()
        {
            return streamId;
        }

        /// <summary>
        /// Session under which messages are published. Identifies this Publication instance.
        /// </summary>
        /// <returns> the session id for this publication. </returns>
        public int SessionId()
        {
            return sessionId;
        }

        /// <summary>
        /// The initial term id assigned when this <seealso cref="Publication"/> was created. This can be used to determine how many
        /// terms have passed since creation.
        /// </summary>
        /// <returns> the initial term id. </returns>
        public int InitialTermId()
        {
            return initialTermId;
        }

        /// <summary>
        /// Maximum message length supported in bytes.
        /// </summary>
        /// <returns> maximum message length supported in bytes. </returns>
        public int MaxMessageLength()
        {
            return maxMessageLength;
        }

        /// <summary>
        /// Has the <seealso cref="Publication"/> seen an active Subscriber recently?
        /// </summary>
        /// <returns> true if this <seealso cref="Publication"/> has seen an active subscriber otherwise false. </returns>
        public bool Connected
        {
            get
            {
                return !isClosed && clientConductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(logMetaDataBuffer));
            }
        }

        /// <summary>
        /// Release resources used by this Publication when there are no more references.
        /// 
        /// Publications are reference counted and are only truly closed when the ref count reaches zero.
        /// </summary>
        public void Close()
        {
            lock (clientConductor)
            {
                if (--refCount == 0)
                {
                    Release();
                }
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool Closed
        {
            get
            {
                return isClosed;
            }
        }

        /// <summary>
        /// Release resources and forcibly close the Publication regardless of reference count.
        /// </summary>
        internal void Release()
        {
            if (!isClosed)
            {
                isClosed = true;
                clientConductor.ReleasePublication(this);
            }
        }

        /// <summary>
        /// Get the current position to which the publication has advanced for this stream.
        /// </summary>
        /// <returns> the current position to which the publication has advanced for this stream. </returns>
        /// <exception cref="IllegalStateException"> if the publication is closed. </exception>
        public long Position()
        {
            if (isClosed)
            {
                return CLOSED;
            }

            long rawTail = termAppenders[LogBufferDescriptor.ActivePartitionIndex(logMetaDataBuffer)].RawTailVolatile();
            int termOffset = LogBufferDescriptor.TermOffset(rawTail, logBuffers.TermLength());

            return LogBufferDescriptor.ComputePosition(LogBufferDescriptor.TermId(rawTail), termOffset, positionBitsToShift, initialTermId);
        }

        /// <summary>
        /// Get the position limit beyond which this <seealso cref="Publication"/> will be back pressured.
        /// 
        /// This should only be used as a guide to determine when back pressure is likely to be applied.
        /// </summary>
        /// <returns> the position limit beyond which this <seealso cref="Publication"/> will be back pressured. </returns>
        public long PositionLimit()
        {
            if (isClosed)
            {
                return CLOSED;
            }

            return positionLimit.Volatile;
        }

        /// <summary>
        /// Non-blocking publish of a buffer containing a message.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <returns> The new stream position, otherwise <seealso cref="#NOT_CONNECTED"/>, <seealso cref="#BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/> or <seealso cref="CLOSED"/>. </returns>
        public long Offer(IDirectBuffer buffer)
        {
            return Offer(buffer, 0, buffer.Capacity);
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <param name="offset"> offset in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in bytes of the encoded message. </param>
        /// <returns> The new stream position, otherwise a negative error value <seealso cref="#NOT_CONNECTED"/>, <seealso cref="#BACK_PRESSURED"/>,
        /// <seealso cref="#ADMIN_ACTION"/> or <seealso cref="#CLOSED"/>. </returns>
        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            long newPosition = CLOSED;
            if (!isClosed)
            {
                long limit = positionLimit.Volatile;
                int partitionIndex = LogBufferDescriptor.ActivePartitionIndex(logMetaDataBuffer);
                TermAppender termAppender = termAppenders[partitionIndex];
                long rawTail = termAppender.RawTailVolatile();
                long termOffset = rawTail & 0xFFFFFFFFL;
                long position = LogBufferDescriptor.ComputeTermBeginPosition(LogBufferDescriptor.TermId(rawTail), positionBitsToShift, initialTermId) + termOffset;

                if (position < limit)
                {
                    long result;
                    if (length <= maxPayloadLength)
                    {
                        result = termAppender.AppendUnfragmentedMessage(headerWriter, buffer, offset, length);
                    }
                    else
                    {
                        CheckForMaxMessageLength(length);
                        result = termAppender.AppendFragmentedMessage(headerWriter, buffer, offset, length, maxPayloadLength);
                    }

                    newPosition = NewPosition(partitionIndex, (int)termOffset, position, result);
                }
                else if (clientConductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(logMetaDataBuffer)))
                {
                    newPosition = BACK_PRESSURED;
                }
                else
                {
                    newPosition = NOT_CONNECTED;
                }
            }

            return newPosition;
        }

        /// <summary>
        /// Try to claim a range in the publication log into which a message can be written with zero copy semantics.
        /// Once the message has been written then <seealso cref="BufferClaim#commit()"/> should be called thus making it available.
        /// <para>
        /// <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
        ///     
        /// <pre>{@code
        ///     final BufferClaim bufferClaim = new BufferClaim(); // Can be stored and reused to avoid allocation
        ///     
        ///     if (publication.tryClaim(messageLength, bufferClaim) > 0L)
        ///     {
        ///         try
        ///         {
        ///              final MutableDirectBuffer buffer = bufferClaim.buffer();
        ///              final int offset = bufferClaim.offset();
        ///     
        ///              // Work with buffer directly or wrap with a flyweight
        ///         }
        ///         finally
        ///         {
        ///             bufferClaim.commit();
        ///         }
        ///     }
        /// }</pre>
        ///     
        /// </para>
        /// </summary>
        /// <param name="length">      of the range to claim, in bytes.. </param>
        /// <param name="bufferClaim"> to be populated if the claim succeeds. </param>
        /// <returns> The new stream position, otherwise <seealso cref="#NOT_CONNECTED"/>, <seealso cref="#BACK_PRESSURED"/>,
        /// <seealso cref="#ADMIN_ACTION"/> or <seealso cref="#CLOSED"/>. </returns>
        /// <exception cref="IllegalArgumentException"> if the length is greater than max payload length within an MTU. </exception>
        /// <seealso cref= BufferClaim#commit() </seealso>
        /// <seealso cref= BufferClaim#abort() </seealso>
        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            long newPosition = CLOSED;
            if (!isClosed)
            {
                CheckForMaxPayloadLength(length);

                long limit = positionLimit.Volatile;
                int partitionIndex = LogBufferDescriptor.ActivePartitionIndex(logMetaDataBuffer);
                TermAppender termAppender = termAppenders[partitionIndex];
                long rawTail = termAppender.RawTailVolatile();
                long termOffset = rawTail & 0xFFFFFFFFL;
                long position = LogBufferDescriptor.ComputeTermBeginPosition(LogBufferDescriptor.TermId(rawTail), positionBitsToShift, initialTermId) + termOffset;

                if (position < limit)
                {
                    long result = termAppender.Claim(headerWriter, length, bufferClaim);
                    newPosition = NewPosition(partitionIndex, (int)termOffset, position, result);
                }
                else if (clientConductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(logMetaDataBuffer)))
                {
                    newPosition = BACK_PRESSURED;
                }
                else
                {
                    newPosition = NOT_CONNECTED;
                }
            }

            return newPosition;
        }

        /// <summary>
        /// Return the registration id used to register this Publication with the media driver.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId()
        {
            return registrationId;
        }

        /// <seealso cref= Publication#close() </seealso>
        internal void IncRef()
        {
            lock (clientConductor)
            {
                ++refCount;
            }
        }

        private long NewPosition(int index, int currentTail, long position, long result)
        {
            long newPosition = ADMIN_ACTION;
            int termOffset = TermAppender.TermOffset(result);
            if (termOffset > 0)
            {
                newPosition = (position - currentTail) + termOffset;
            }
            else if (termOffset == TermAppender.TRIPPED)
            {
                int nextIndex = LogBufferDescriptor.NextPartitionIndex(index);
                int nextNextIndex = LogBufferDescriptor.NextPartitionIndex(nextIndex);

                termAppenders[nextIndex].TailTermId(TermAppender.TermId(result) + 1);
                termAppenders[nextNextIndex].StatusOrdered(LogBufferDescriptor.NEEDS_CLEANING);
                LogBufferDescriptor.ActivePartitionIndex(logMetaDataBuffer, nextIndex);
            }

            return newPosition;
        }

        private void CheckForMaxPayloadLength(int length)
        {
            if (length > maxPayloadLength)
            {
                throw new ArgumentException(
                    $"Claim exceeds maxPayloadLength of {maxPayloadLength:D}, length={length:D}");
            }
        }

        private void CheckForMaxMessageLength(int length)
        {
            if (length > maxMessageLength)
            {
                throw new ArgumentException(
                    $"Encoded message exceeds maxMessageLength of {maxMessageLength:D}, length={length:D}");
            }
        }

        internal IManagedResource ManagedResource()
        {
            return new PublicationManagedResource(this);
        }

        private class PublicationManagedResource : IManagedResource
        {
            private readonly Publication _publication;
            private long timeOfLastStateChange = 0;

            public PublicationManagedResource(Publication publication)
            {
                _publication = publication;
            }

            public void TimeOfLastStateChange(long time)
            {
                timeOfLastStateChange = time;
            }

            public long TimeOfLastStateChange()
            {
                return timeOfLastStateChange;
            }

            public void Delete()
            {
                _publication.logBuffers.Dispose();
            }
        }

    }
}