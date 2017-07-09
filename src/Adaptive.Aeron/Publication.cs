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
    /// are created via the <seealso cref="Aeron.AddPublication(String, int)"/> method, and messages are sent via one of the
    /// <seealso cref="Offer(IDirectBuffer)"/> methods, or a <seealso cref="TryClaim(int, BufferClaim)"/> and <seealso cref="BufferClaim.Commit"/>
    /// method combination.
    /// 
    /// The APIs used to send are all non-blocking.
    /// 
    /// <b>Note:</b> Publication instances are threadsafe and can be shared between publishing threads.
    /// </summary>
    /// <seealso cref="Aeron.AddPublication(string, int)" />
    /// <seealso cref="BufferClaim" />
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

        private int _refCount;
        private readonly int _positionBitsToShift;
        private volatile bool _isClosed;

        private readonly TermAppender[] _termAppenders = new TermAppender[LogBufferDescriptor.PARTITION_COUNT];
        private readonly IReadablePosition _positionLimit;
        private readonly UnsafeBuffer _logMetaDataBuffer;
        private readonly HeaderWriter _headerWriter;
        private readonly LogBuffers _logBuffers;
        private readonly ClientConductor _conductor;

        internal Publication(ClientConductor clientConductor, string channel, int streamId, int sessionId, IReadablePosition positionLimit, LogBuffers logBuffers, long registrationId)
        {
            var buffers = logBuffers.TermBuffers();
            var logMetaDataBuffer = logBuffers.MetaDataBuffer();

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                _termAppenders[i] = new TermAppender(buffers[i], logMetaDataBuffer, i);
            }

            var termLength = logBuffers.TermLength();
            MaxPayloadLength = LogBufferDescriptor.MtuLength(logMetaDataBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
            MaxMessageLength = FrameDescriptor.ComputeMaxMessageLength(termLength);
            _conductor = clientConductor;
            Channel = channel;
            StreamId = streamId;
            SessionId = sessionId;
            InitialTermId = LogBufferDescriptor.InitialTermId(logMetaDataBuffer);
            _logMetaDataBuffer = logMetaDataBuffer;
            RegistrationId = registrationId;
            _positionLimit = positionLimit;
            _logBuffers = logBuffers;
            _positionBitsToShift = IntUtil.NumberOfTrailingZeros(termLength);
            _headerWriter = new HeaderWriter(LogBufferDescriptor.DefaultFrameHeader(logMetaDataBuffer));
        }

        /// <summary>
        /// Get the length in bytes for each term partition in the log buffer.
        /// </summary>
        /// <returns> the length in bytes for each term partition in the log buffer. </returns>
        public int TermBufferLength => _logBuffers.TermLength();

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
        /// Session under which messages are published. Identifies this Publication instance.
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
        /// Return the registration id used to register this Publication with the media driver.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId { get; }

        /// <summary>
        /// Has the <seealso cref="Publication"/> seen an active Subscriber recently?
        /// </summary>
        /// <returns> true if this <seealso cref="Publication"/> has seen an active subscriber otherwise false. </returns>
        public bool IsConnected => !_isClosed && _conductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(_logMetaDataBuffer));

        /// <summary>
        /// Release resources used by this Publication when there are no more references.
        /// 
        /// Publications are reference counted and are only truly closed when the ref count reaches zero.
        /// </summary>
        public void Dispose()
        {
            _conductor.ClientLock().Lock();
            try
            {
                if (!_isClosed && --_refCount == 0)
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

                return _positionLimit.Volatile;
            }
        }

        /// <summary>
        /// Non-blocking publish of a buffer containing a message.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <returns> The new stream position, otherwise <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/> or <seealso cref="CLOSED"/>. </returns>
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
        /// <returns> The new stream position, otherwise a negative error value <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/> or <seealso cref="CLOSED"/>. </returns>
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
                var partitionIndex = LogBufferDescriptor.ActivePartitionIndex(_logMetaDataBuffer);
                var termAppender = _termAppenders[partitionIndex];
                var rawTail = termAppender.RawTailVolatile();
                var termOffset = rawTail & 0xFFFFFFFFL;
                var position = LogBufferDescriptor.ComputeTermBeginPosition(LogBufferDescriptor.TermId(rawTail), _positionBitsToShift, InitialTermId) + termOffset;

                if (position < limit)
                {
                    long result;
                    if (length <= MaxPayloadLength)
                    {
                        result = termAppender.AppendUnfragmentedMessage(_headerWriter, buffer, offset, length, reservedValueSupplier);
                    }
                    else
                    {
                        CheckForMaxMessageLength(length);
                        result = termAppender.AppendFragmentedMessage(_headerWriter, buffer, offset, length, MaxPayloadLength, reservedValueSupplier);
                    }

                    newPosition = NewPosition(partitionIndex, (int) termOffset, position, result);
                }
                else if (_conductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(_logMetaDataBuffer)))
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
        /// Once the message has been written then <seealso cref="BufferClaim.Commit()"/> should be called thus making it available.
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
        /// <returns> The new stream position, otherwise <seealso cref="NOT_CONNECTED"/>, <seealso cref="BACK_PRESSURED"/>,
        /// <seealso cref="ADMIN_ACTION"/> or <seealso cref="CLOSED"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than max payload length within an MTU. </exception>
        /// <seealso cref="BufferClaim.Commit()" />
        /// <seealso cref="BufferClaim.Abort()" />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            CheckForMaxPayloadLength(length);
            var newPosition = CLOSED;

            if (!_isClosed)
            {
                var limit = _positionLimit.Volatile;
                var partitionIndex = LogBufferDescriptor.ActivePartitionIndex(_logMetaDataBuffer);
                var termAppender = _termAppenders[partitionIndex];
                var rawTail = termAppender.RawTailVolatile();
                var termOffset = rawTail & 0xFFFFFFFFL;
                var position = LogBufferDescriptor.ComputeTermBeginPosition(LogBufferDescriptor.TermId(rawTail), _positionBitsToShift, InitialTermId) + termOffset;

                if (position < limit)
                {
                    var result = termAppender.Claim(_headerWriter, length, bufferClaim);
                    newPosition = NewPosition(partitionIndex, (int) termOffset, position, result);
                }
                else if (_conductor.IsPublicationConnected(LogBufferDescriptor.TimeOfLastStatusMessage(_logMetaDataBuffer)))
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

        /// <seealso cref="Dispose()" />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void IncRef()
        {
            ++_refCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NewPosition(int index, int currentTail, long position, long result)
        {
            var newPosition = ADMIN_ACTION;
            var termOffset = TermAppender.TermOffset(result);
            if (termOffset > 0)
            {
                newPosition = (position - currentTail) + termOffset;
            }
            else if (termOffset == TermAppender.TRIPPED)
            {
                var nextIndex = LogBufferDescriptor.NextPartitionIndex(index);
                
                _termAppenders[nextIndex].TailTermId(TermAppender.TermId(result) + 1);
                LogBufferDescriptor.ActivePartitionIndexOrdered(_logMetaDataBuffer, nextIndex);
            }

            return newPosition;
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
            private readonly Publication _publication;
            private long _timeOfLastStateChange;

            public PublicationManagedResource(Publication publication)
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