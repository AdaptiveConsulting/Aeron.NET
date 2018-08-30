﻿/*
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
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. <seealso cref="Publication"/>s
    /// are created via the <seealso cref="Aeron#addPublication(String, int)"/> <seealso cref="Aeron#addExclusivePublication(String, int)"/>
    /// methods, and messages are sent via one of the <seealso cref="#offer(DirectBuffer)"/> methods.
    /// <para>
    /// The APIs used for tryClaim and offer are non-blocking.
    /// </para>
    /// <para>
    /// <b>Note:</b> All methods are threadsafe with the exception of offer and tryClaim for the subclass
    /// <seealso cref="ExclusivePublication"/>. In the case of <seealso cref="ConcurrentPublication"/> all methods are threadsafe.
    /// 
    /// </para>
    /// </summary>
    /// <seealso cref="ConcurrentPublication"/>
    /// <seealso cref="ExclusivePublication"/>
    /// <seealso cref="Aeron.AddPublication(String, int)"/>
    /// <seealso cref="Aeron.AddExclusivePublication(String, int)"/>
    public abstract class Publication : IDisposable
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
        /// If this happens then the publication should be closed and a new one added. To make it less likely to happen then
        /// increase the term buffer length.
        /// </para>
        /// </summary>
        public const long MAX_POSITION_EXCEEDED = -5;

        protected readonly long _originalRegistrationId;
        protected readonly long _maxPossiblePosition;
        protected readonly int _channelStatusId;
        protected readonly int _positionBitsToShift;
        protected volatile bool _isClosed;

        protected readonly IReadablePosition _positionLimit;
        protected readonly UnsafeBuffer _logMetaDataBuffer;
        protected readonly HeaderWriter _headerWriter;
        protected readonly LogBuffers _logBuffers;
        internal readonly ClientConductor _conductor;

        internal Publication(
            ClientConductor clientConductor, 
            string channel, 
            int streamId, 
            int sessionId, 
            IReadablePosition positionLimit,
            int channelStatusId,
            LogBuffers logBuffers, 
            long originalRegistrationId,
            long registrationId,
            int maxMessageLength)
        {
            _logMetaDataBuffer = logBuffers.MetaDataBuffer();
            TermBufferLength = logBuffers.TermLength();
            MaxMessageLength = maxMessageLength;
            MaxPayloadLength = LogBufferDescriptor.MtuLength(_logMetaDataBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
            _maxPossiblePosition = TermBufferLength * (1L << 31);
            _conductor = clientConductor;
            Channel = channel;
            StreamId = streamId;
            SessionId = sessionId;
            InitialTermId = LogBufferDescriptor.InitialTermId(_logMetaDataBuffer);
            _originalRegistrationId = originalRegistrationId;
            RegistrationId = registrationId;
            _positionLimit = positionLimit;
            _channelStatusId = channelStatusId;
            _logBuffers = logBuffers;
            _positionBitsToShift = LogBufferDescriptor.PositionBitsToShift(TermBufferLength);
            _headerWriter = new HeaderWriter(LogBufferDescriptor.DefaultFrameHeader(_logMetaDataBuffer));
        }

        /// <summary>
        /// Get the length in bytes for each term partition in the log buffer.
        /// </summary>
        /// <returns> the length in bytes for each term partition in the log buffer. </returns>
        public int TermBufferLength { get; }
        
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
        /// Session under which messages are published. Identifies this Publication instance. Sessions are unique across
        /// all active publications on a driver instance.
        /// </summary>
        /// <returns> the session id for this publication. </returns>
        public int SessionId { get; }

        /// <summary>
        /// The initial term id assigned when this <seealso cref="Publication"/> was created. This can be used to determine how many
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
        /// Get the registration used to register this Publication with the media driver by the first publisher.
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
        /// If this value is different from the <see cref="OriginalRegistrationId"/> then a previous active registration exists.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId { get; }

        /// <summary>
        /// Has the <seealso cref="Publication"/> seen an active Subscriber recently?
        /// </summary>
        /// <returns> true if this <seealso cref="Publication"/> has seen an active subscriber otherwise false. </returns>
        public bool IsConnected => !_isClosed && LogBufferDescriptor.IsConnected(_logMetaDataBuffer);

        /// <summary>
        /// Release resources used by this Publication when there are no more references.
        /// 
        /// Publications are reference counted and are only truly closed when the ref count reaches zero.
        /// </summary>
        public void Dispose()
        {
            if (!_isClosed)
            {
                _conductor.ReleasePublication(this);
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool IsClosed => _isClosed;

        /// <summary>
        /// Get the status of the media channel for this Publication.
        /// <para>
        /// The status will be <seealso cref="ChannelEndpointStatus#ERRORED"/> if a socket exception occurs on setup
        /// and <seealso cref="ChannelEndpointStatus#ACTIVE"/> if all is well.
        ///     
        /// </para>
        /// </summary>
        /// <returns> status for the channel as one of the constants from <seealso cref="ChannelEndpointStatus"/> with it being
        /// <seealso cref="ChannelEndpointStatus.NO_ID_ALLOCATED"/> if the publication is closed. </returns>
        /// <seealso cref="ChannelEndpointStatus"/>
        public long ChannelStatus()
        {
            if (_isClosed)
            {
                return ChannelEndpointStatus.NO_ID_ALLOCATED;
            }

            return _conductor.ChannelStatus(_channelStatusId);
        }
        
        /// <summary>
        /// Get the counter used to represent the channel status for this publication.
        /// </summary>
        /// <returns> the counter used to represent the channel status for this publication. </returns>
        public int ChannelStatusId()
        {
            return _channelStatusId;
        }

        /// <summary>
        /// Get the current position to which the publication has advanced for this stream.
        /// </summary>
        /// <returns> the current position to which the publication has advanced for this stream or <see cref="CLOSED"/>. </returns>
        public long Position
        {
            get
            {
                if (_isClosed)
                {
                    return CLOSED;
                }

                var rawTail = LogBufferDescriptor.RawTailVolatile(_logMetaDataBuffer);
                var termOffset = LogBufferDescriptor.TermOffset(rawTail, TermBufferLength);

                return LogBufferDescriptor.ComputePosition(LogBufferDescriptor.TermId(rawTail), termOffset, _positionBitsToShift, InitialTermId);
            }
        }

        /// <summary>
        /// Get the position limit beyond which this <seealso cref="Publication"/> will be back pressured.
        /// 
        /// This should only be used as a guide to determine when back pressure is likely to be applied.
        /// </summary>
        /// <returns> the position limit beyond which this <seealso cref="Publication"/> will be back pressured. </returns>
        public long PositionLimit
        {
            get
            {
                if (_isClosed)
                {
                    return CLOSED;
                }

                return _positionLimit.GetVolatile();
            }
        }
        
        /// <summary>
        /// Get the counter id for the position limit after which the publication will be back pressured.
        /// </summary>
        /// <returns> the counter id for the position limit after which the publication will be back pressured. </returns>
        public int PositionLimitId()
        {
            return _positionLimit.Id();
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
        public abstract long Offer(
            IDirectBuffer buffer,
            int offset,
            int length,
            ReservedValueSupplier reservedValueSupplier = null);

        /// <summary>
        /// Non-blocking publish by gathering buffer vectors into a message.
        /// </summary>
        /// <param name="vectors"> which make up the message. </param>
        /// <param name="reservedValueSupplier"> <see cref="ReservedValueSupplier"/> for the frame.</param>
        /// <returns> The new stream position, otherwise a negative error value <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/>, <seealso cref="CLOSED"/> or <see cref="MAX_POSITION_EXCEEDED"/>. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public abstract long Offer(DirectBufferVector[] vectors, ReservedValueSupplier reservedValueSupplier = null);

        /// <summary>
        /// Try to claim a range in the publication log into which a message can be written with zero copy semantics.
        /// Once the message has been written then <seealso cref="BufferClaim.Commit()"/> should be called thus making it available.
        ///
        /// <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
        /// If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
        /// assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress
        /// for <see cref="ConcurrentPublication"/> and other claims to be sent to reach end-of-stream (EOS).
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
        /// <returns> The new stream position, otherwise <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/>, <seealso cref="CLOSED"/> or <see cref="MAX_POSITION_EXCEEDED"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than max payload length within an MTU. </exception>
        /// <seealso cref="BufferClaim.Commit()" />
        /// <seealso cref="BufferClaim.Abort()" />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public abstract long TryClaim(int length, BufferClaim bufferClaim);
        
       /// <summary>
        /// Add a destination manually to a multi-destination-cast Publication.
        /// </summary>
        /// <param name="endpointChannel"> for the destination to add </param>
        public void AddDestination(string endpointChannel)
        {
            if (_isClosed)
            {
                throw new AeronException("Publication is closed.");
            }
            
            _conductor.AddDestination(_originalRegistrationId, endpointChannel);
        }

        /// <summary>
        /// Remove a previously added destination manually from a multi-destination-cast Publication.
        /// </summary>
        /// <param name="endpointChannel"> for the destination to remove </param>
        public void RemoveDestination(string endpointChannel)
        {
            if (_isClosed)
            {
                throw new AeronException("Publication is closed.");
            }
            
            _conductor.RemoveDestination(_originalRegistrationId, endpointChannel);
        }

        internal void InternalClose()
        {
            _isClosed = true;
        }

        internal LogBuffers LogBuffers()
        {
            return _logBuffers;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long BackPressureStatus(long currentPosition, int messageLength)
        {
            if ((currentPosition + messageLength) >= _maxPossiblePosition)
            {
                return MAX_POSITION_EXCEEDED;
            }

            if (LogBufferDescriptor.IsConnected(_logMetaDataBuffer))
            {
                return BACK_PRESSURED;
            }

            return NOT_CONNECTED;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void CheckForMaxPayloadLength(int length)
        {
            if (length > MaxPayloadLength)
            {
                ThrowHelper.ThrowArgumentException(
                    $"claim exceeds maxPayloadLength of {MaxPayloadLength:D}, length={length:D}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void CheckForMaxMessageLength(int length)
        {
            if (length > MaxMessageLength)
            {
                ThrowHelper.ThrowArgumentException(
                    $"message exceeds maxMessageLength of {MaxMessageLength:D}, length={length:D}");
            }
        }
    }
}