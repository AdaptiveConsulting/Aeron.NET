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
    /// Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
    /// each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
    /// 
    /// <see cref="ExclusivePublication"/>s are create via the <see cref="Aeron.AddExclusivePublication(string, int)"/> method,
    /// and messages are sent via one of the <see cref="Offer(UnsafeBuffer)"/> methods, or a
    /// <see cref="TryClaim(int, ExlusiveBufferClaim)"/> and <see cref="ExclusiveBufferClaim.Commit"/> method combination.
    /// 
    /// <see cref="ExclusivePublication"/>s have the potential to provide greater throughput than <see cref="Publication"/>s.
    /// 
    /// The APIs used try claim and offer are non-blocking.
    /// 
    /// <b>Note:</b> Instances are NOT threadsafe for offer and try claim methods but are for the others.
    /// </summary>
    /// <seealso cref="Aeron.AddExclusivePublication(string, int)" />
    /// <seealso cref="ExclusiveBufferClaim" />
    public class ExclusivePublication : IDisposable
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
        /// The action is an operation such as log rotation which is likely to have succeeded by the next retry attempt.
        /// </summary>
        public const long ADMIN_ACTION = -3;

        /// <summary>
        /// The <seealso cref="Publication"/> has been closed and should no longer be used.
        /// </summary>
        public const long CLOSED = -4;

        /// <summary>
        /// The offer failed due to reaching the maximum position of the stream given term buffer length times the total
        /// possible number of terms.
        /// <para>
        /// If this happen then the publication should be closed and a new one added. To make it less likely to happen then
        /// increase the term buffer length.
        /// </para>
        /// </summary>
        public const long MAX_POSITION_EXCEEDED = -5;

        private readonly long _originalRegistrationId;
        private readonly long _maxPossiblePosition;
        private readonly int _termBufferLength;
        private readonly int _positionBitsToShift;
        private volatile bool _isClosed;
        private long _termBeginPosition;
        private int _activePartitionIndex;
        private int _termId;
        private int _termOffset;

        private readonly ExclusiveTermAppender[] _termAppenders =
            new ExclusiveTermAppender[LogBufferDescriptor.PARTITION_COUNT];

        private readonly IReadablePosition _positionLimit;
        private readonly UnsafeBuffer _logMetaDataBuffer;
        private readonly HeaderWriter _headerWriter;
        private readonly LogBuffers _logBuffers;
        private readonly ClientConductor _conductor;

        internal ExclusivePublication(
            ClientConductor clientConductor,
            string channel,
            int streamId,
            int sessionId,
            IReadablePosition positionLimit,
            LogBuffers logBuffers,
            long originalRegistrationId,
            long registrationId)
        {
            var buffers = logBuffers.TermBuffers();
            var logMetaDataBuffer = logBuffers.MetaDataBuffer();

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                _termAppenders[i] = new ExclusiveTermAppender(buffers[i], logMetaDataBuffer, i);
            }

            var termLength = logBuffers.TermLength();
            _termBufferLength = termLength;
            MaxPayloadLength = LogBufferDescriptor.MtuLength(logMetaDataBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
            MaxMessageLength = FrameDescriptor.ComputeExclusiveMaxMessageLength(termLength);
            _maxPossiblePosition = termLength * (1L << 31);
            _conductor = clientConductor;
            Channel = channel;
            StreamId = streamId;
            SessionId = sessionId;
            
            _logMetaDataBuffer = logMetaDataBuffer;
            _originalRegistrationId = originalRegistrationId;
            RegistrationId = registrationId;
            _positionLimit = positionLimit;
            _logBuffers = logBuffers;
            _positionBitsToShift = IntUtil.NumberOfTrailingZeros(termLength);
            _headerWriter = new HeaderWriter(LogBufferDescriptor.DefaultFrameHeader(logMetaDataBuffer));
            InitialTermId = LogBufferDescriptor.InitialTermId(logMetaDataBuffer);

            var activeIndex = LogBufferDescriptor.ActivePartitionIndex(logMetaDataBuffer);
            _activePartitionIndex = activeIndex;

            long rawTail = LogBufferDescriptor.RawTail(_logMetaDataBuffer, activeIndex);
            _termId = LogBufferDescriptor.TermId(rawTail);
            _termOffset = LogBufferDescriptor.TermOffset(rawTail, termLength);
            _termBeginPosition =
                LogBufferDescriptor.ComputeTermBeginPosition(_termId, _positionBitsToShift, InitialTermId);
        }

        /// <summary>
        /// Get the length in bytes for each term partition in the log buffer.
        /// </summary>
        /// <returns> the length in bytes for each term partition in the log buffer. </returns>
        public int TermBufferLength => _logBuffers.TermLength();

        /// <summary>
        /// The maximum possible position this stream can reach due to its term buffer length.
        /// 
        /// Maximum possible position is term-length times 2^31 in bytes.
        /// 
        /// </summary>
        /// <returns> the maximum possible position this stream can reach due to it term buffer length. </returns>
        public long MaxPossiblePosition()
        {
            return _maxPossiblePosition;
        }
        
        /// <summary>
        /// Media address for delivery to the channel.
        /// </summary>
        /// <returns> Media address for delivery to the channel. </returns>
        public string Channel { get; }


        /// <summary>
        /// Stream identity for scoping within the channel media address.
        /// </summary>
        /// <returns> Stream identity for scoping within the channel media address. </returns>
        public int StreamId { get; }


        /// <summary>
        /// Session under which messages are published. Identifies this <see cref="ExclusivePublication"/> instance.
        /// </summary>
        /// <returns> the session id for this publication. </returns>
        public int SessionId { get; }

        /// <summary>
        /// The initial term id assigned when this <seealso cref="ExclusivePublication"/> was created. This can be used to determine how many
        /// terms have passed since creation.
        /// </summary>
        /// <returns> the initial term id. </returns>
        public int InitialTermId { get; }

        /// <summary>
        /// Maximum message length supported in bytes. Messages may be made of of multiple fragments if greater than
        /// MTU length.
        /// </summary>
        /// <returns> maximum message length supported in bytes. </returns>
        public int MaxMessageLength { get; }

        /// <summary>
        /// Maximum length of a message payload that fits within a message fragment.
        /// 
        /// This is he MTU length minus the message fragment header length.
        /// 
        /// <returns>maximum message fragment payload length.</returns>
        /// </summary>
        public int MaxPayloadLength { get; }

        /// <summary>
        /// Get the original registration used to register this Publication with the media driver by the first publisher.
        /// </summary>
        /// <returns> original registration id </returns>
        public long OriginalRegistrationId()
        {
            return _originalRegistrationId;
        }

        /// <summary>
        /// Is this Publication the original instance added to the driver? If not then it was added after another client
        /// has already added the publication.
        /// </summary>
        /// <returns> true if this instance is the first added otherwise false. </returns>
        public bool IsOriginal()
        {
            return _originalRegistrationId == RegistrationId;
        }
        
        /// <summary>
        /// Get the registration id used to register this Publication with the media driver.
        /// 
        /// If this value is different from the <see cref="OriginalRegistrationId"/> then another client has previously added
        /// this Publication. In the case of an exclusive publication this should never happen.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId { get; }

        /// <summary>
        /// Has the <see cref="ExclusivePublication"/> seen an active Subscriber recently?
        /// </summary>
        /// <returns> true if this <see cref="ExclusivePublication"/> has seen an active subscriber otherwise false. </returns>
        public bool IsConnected => !_isClosed &&
                                   _conductor.IsPublicationConnected(
                                       LogBufferDescriptor.TimeOfLastStatusMessage(_logMetaDataBuffer));

        /// <summary>
        /// Release resources used by this Publication.
        /// </summary>
        public void Dispose()
        {
            _conductor.ClientLock().Lock();
            try
            {
                if (!_isClosed)
                {
                    _isClosed = true;
                    _conductor.ReleasePublication(this);
                }
            }
            finally
            {
                _conductor.ClientLock().Unlock();
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool IsClosed => _isClosed;

        /// <summary>
        /// Forcibly close the Publication and release resources
        /// </summary>
        internal void ForceClose()
        {
            if (!_isClosed)
            {
                _isClosed = true;
                _conductor.AsyncReleasePublication(RegistrationId);
                _conductor.LingerResource(ManagedResource());
            }
        }

        /// <summary>
        /// Get the current position to which the publication has advanced for this stream.
        /// </summary>
        /// <returns> the current position to which the publication has advanced for this stream. </returns>
        /// <exception cref="InvalidOperationException"> if the publication is closed. </exception>
        public long Position
        {
            get
            {
                if (_isClosed)
                {
                    return CLOSED;
                }

                var rawTail = LogBufferDescriptor.RawTailVolatile(_logMetaDataBuffer);
                var termOffset = LogBufferDescriptor.TermOffset(rawTail, _logBuffers.TermLength());

                return LogBufferDescriptor.ComputePosition(LogBufferDescriptor.TermId(rawTail), termOffset,
                    _positionBitsToShift, InitialTermId);
            }
        }

        /// <summary>
        /// Get the position limit beyond which this <see cref="ExclusivePublication"/> will be back pressured.
        /// 
        /// This should only be used as a guide to determine when back pressure is likely to be applied.
        /// </summary>
        /// <returns> the position limit beyond which this <see cref="ExclusivePublication"/> will be back pressured. </returns>
        public long PositionLimit
        {
            get
            {
                if (_isClosed)
                {
                    return CLOSED;
                }

                return _positionLimit.Volatile;
            }
        }

        /// <summary>
        /// Non-blocking publish of a buffer containing a message.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <returns> The new stream position, otherwise <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/>, <seealso cref="CLOSED"/> or <see cref="MAX_POSITION_EXCEEDED"/>. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Offer(UnsafeBuffer buffer)
        {
            return Offer(buffer, 0, buffer.Capacity);
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <param name="offset"> offset in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in bytes of the encoded message. </param>
        /// <param name="reservedValueSupplier"> <see cref="ReservedValueSupplier"/> for the frame.</param>
        /// <returns> The new stream position, otherwise a negative error value <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/>, <seealso cref="CLOSED"/> or <see cref="MAX_POSITION_EXCEEDED"/>. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Offer(
            UnsafeBuffer buffer,
            int offset,
            int length,
            ReservedValueSupplier reservedValueSupplier = null)
        {
            var newPosition = CLOSED;
            if (!_isClosed)
            {
                var limit = _positionLimit.Volatile;
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;

                if (position < limit)
                {
                    int result;
                    if (length <= MaxPayloadLength)
                    {
                        result = termAppender.AppendUnfragmentedMessage(_termId, _termOffset, _headerWriter, buffer,
                            offset, length, reservedValueSupplier);
                    }
                    else
                    {
                        CheckForMaxMessageLength(length);
                        result = termAppender.AppendFragmentedMessage(_termId, _termOffset, _headerWriter, buffer,
                            offset, length, MaxPayloadLength, reservedValueSupplier);
                    }

                    newPosition = NewPosition(result);
                }
                else
                {
                    newPosition = BackPressureStatus(position, length);
                }
            }

            return newPosition;
        }

        /// <summary>
        /// Try to claim a range in the publication log into which a message can be written with zero copy semantics.
        /// Once the message has been written then <seealso cref="ExclusiveBufferClaim.Commit()"/> should be called thus making it available.
        /// <para>
        /// <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
        ///     
        /// <pre>{@code
        ///     final ExclusiveBufferClaim bufferClaim = new ExclusiveBufferClaim(); // Can be stored and reused to avoid allocation
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
        /// <returns> The new stream position, otherwise <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/>, <seealso cref="CLOSED"/> or <see cref="MAX_POSITION_EXCEEDED"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than max payload length within an MTU. </exception>
        /// <seealso cref="ExclusiveBufferClaim.Commit()" />
        /// <seealso cref="ExclusiveBufferClaim.Abort()" />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryClaim(int length, ExclusiveBufferClaim bufferClaim)
        {
            CheckForMaxPayloadLength(length);
            var newPosition = CLOSED;

            if (!_isClosed)
            {
                var limit = _positionLimit.Volatile;
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;

                if (position < limit)
                {
                    int result = termAppender.Claim(_termId, _termOffset, _headerWriter, length, bufferClaim);
                    newPosition = NewPosition(result);
                }
                else
                {
                    newPosition = BackPressureStatus(position, length);
                }
            }

            return newPosition;
        }

        /// <summary>
        /// Append a padding record log of a given length to make up the log to a position.
        /// </summary>
        /// <param name="length"> of the range to claim, in bytes.. </param>
        /// <returns> The new stream position, otherwise a negative error value of <seealso cref="NOT_CONNECTED"/>,
        /// <seealso cref="BACK_PRESSURED"/>, <seealso cref="ADMIN_ACTION"/>, <seealso cref="CLOSED"/>, or <seealso cref="MAX_POSITION_EXCEEDED"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="MaxMessageLength()"/>. </exception>
        public virtual long AppendPadding(int length)
        {
            CheckForMaxMessageLength(length);
            long newPosition = CLOSED;

            if (!_isClosed)
            {
                long limit = _positionLimit.Volatile;
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;

                if (position < limit)
                {
                    int result = termAppender.AppendPadding(_termId, _termOffset, _headerWriter, length);
                    newPosition = NewPosition(result);
                }
                else
                {
                    newPosition = BackPressureStatus(position, length);
                }
            }

            return newPosition;
        }


        /// <summary>
        /// Add a destination manually to a multi-destination-cast Publication.
        /// </summary>
        /// <param name="endpointChannel"> for the destination to add </param>
        public void AddDestination(string endpointChannel)
        {
            _conductor.ClientLock().Lock();
            try
            {
                _conductor.AddDestination(RegistrationId, endpointChannel);
            }
            finally
            {
                _conductor.ClientLock().Unlock();
            }
        }

        /// <summary>
        /// Remove a previously added destination manually from a multi-destination-cast Publication.
        /// </summary>
        /// <param name="endpointChannel"> for the destination to remove </param>
        public void RemoveDestination(string endpointChannel)
        {
            _conductor.ClientLock().Lock();
            try
            {
                _conductor.RemoveDestination(RegistrationId, endpointChannel);
            }
            finally
            {
                _conductor.ClientLock().Unlock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NewPosition(int resultingOffset)
        {
            if (resultingOffset > 0)
            {
                _termOffset = resultingOffset;

                return _termBeginPosition + resultingOffset;
            }
            else
            {
                if ((_termBeginPosition + _termBufferLength) >= MaxPossiblePosition())
                {
                    return MAX_POSITION_EXCEEDED;
                }

                int nextIndex = LogBufferDescriptor.NextPartitionIndex(_activePartitionIndex);
                int nextTermId = _termId + 1;

                _activePartitionIndex = nextIndex;
                _termOffset = 0;
                _termId = nextTermId;
                _termBeginPosition =
                    LogBufferDescriptor.ComputeTermBeginPosition(nextTermId, _positionBitsToShift, InitialTermId);

                LogBufferDescriptor.InitialiseTailWithTermId(_logMetaDataBuffer, nextIndex, nextTermId);
                LogBufferDescriptor.ActivePartitionIndexOrdered(_logMetaDataBuffer, nextIndex);

                return ADMIN_ACTION;
            }
        }

        private long BackPressureStatus(long currentPosition, int messageLength)
        {
            long status = NOT_CONNECTED;

            if ((currentPosition + messageLength) >= _maxPossiblePosition)
            {
                status = MAX_POSITION_EXCEEDED;
            }
            else if (_conductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(_logMetaDataBuffer)))
            {
                status = BACK_PRESSURED;
            }

            return status;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckForMaxPayloadLength(int length)
        {
            if (length > MaxPayloadLength)
            {
                ThrowHelper.ThrowArgumentException(
                    $"Claim exceeds maxPayloadLength of {MaxPayloadLength:D}, length={length:D}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckForMaxMessageLength(int length)
        {
            if (length > MaxMessageLength)
            {
                ThrowHelper.ThrowArgumentException(
                    $"Mssage exceeds maxMessageLength of {MaxMessageLength:D}, length={length:D}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IManagedResource ManagedResource()
        {
            return new PublicationManagedResource(this);
        }

        private class PublicationManagedResource : IManagedResource
        {
            private readonly ExclusivePublication _publication;
            private long _timeOfLastStateChange;

            public PublicationManagedResource(ExclusivePublication publication)
            {
                _publication = publication;
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
                _publication._logBuffers.Dispose();
            }
        }
    }
}