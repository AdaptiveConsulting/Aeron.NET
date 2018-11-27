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
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
    /// each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
    /// <para>
    /// <seealso cref="ExclusivePublication"/>s are created via the <seealso cref="Aeron.AddExclusivePublication(String, int)"/> method,
    /// and messages are sent via one of the <seealso cref="Publication.Offer(UnsafeBuffer)"/> methods, or a
    /// <seealso cref="TryClaim(int, BufferClaim)"/> and <seealso cref="BufferClaim.Commit()"/> method combination.
    /// </para>
    /// <para>
    /// <seealso cref="ExclusivePublication"/>s have the potential to provide greater throughput than the default <seealso cref="Publication"/>
    /// which supports concurrent access.
    /// </para>
    /// <para>
    /// The APIs used for tryClaim and offer are non-blocking.
    /// </para>
    /// <para>
    /// <b>Note:</b> Instances are NOT threadsafe for offer and tryClaim methods but are for the others.
    /// 
    /// </para>
    /// </summary>
    /// <seealso cref="Aeron.AddExclusivePublication(String, int)"></seealso>
    /// <seealso cref="BufferClaim"></seealso>
    public class ExclusivePublication : Publication
    {
        private long _termBeginPosition;
        private int _activePartitionIndex;
        private int _termId;
        private int _termOffset;

        private readonly ExclusiveTermAppender[] _termAppenders =
            new ExclusiveTermAppender[LogBufferDescriptor.PARTITION_COUNT];

        internal ExclusivePublication(
            ClientConductor clientConductor,
            string channel,
            int streamId,
            int sessionId,
            IReadablePosition positionLimit,
            int channelStatusId,
            LogBuffers logBuffers,
            long originalRegistrationId,
            long registrationId)
            : base(
                clientConductor,
                channel,
                streamId,
                sessionId,
                positionLimit,
                channelStatusId,
                logBuffers,
                originalRegistrationId,
                registrationId
            )
        {
            var buffers = logBuffers.DuplicateTermBuffers();
            var logMetaDataBuffer = logBuffers.MetaDataBuffer();

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                _termAppenders[i] = new ExclusiveTermAppender(buffers[i], logMetaDataBuffer, i);
            }

            var termCount = LogBufferDescriptor.ActiveTermCount(logMetaDataBuffer);
            var index = LogBufferDescriptor.IndexByTermCount(termCount);
            _activePartitionIndex = index;

            var rawTail = LogBufferDescriptor.RawTail(_logMetaDataBuffer, index);

            _termId = LogBufferDescriptor.TermId(rawTail);
            _termOffset = LogBufferDescriptor.TermOffset(rawTail);
            _termBeginPosition =
                LogBufferDescriptor.ComputeTermBeginPosition(_termId, PositionBitsToShift, InitialTermId);
        }

       
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long Offer(
            IDirectBuffer buffer,
            int offset,
            int length,
            ReservedValueSupplier reservedValueSupplier = null)
        {
            var newPosition = CLOSED;
            if (!_isClosed)
            {
                var limit = _positionLimit.GetVolatile();
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;

                if (position < limit)
                {
                    int result;
                    if (length <= MaxPayloadLength)
                    {
                        CheckPositiveLength(length);
                        result = termAppender.AppendUnfragmentedMessage(_termId, _termOffset, _headerWriter, buffer,
                            offset, length, reservedValueSupplier);
                    }
                    else
                    {
                        CheckMaxMessageLength(length);
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

      
        
        /// <inheritdoc />
        public override long Offer(IDirectBuffer bufferOne, int offsetOne, int lengthOne, IDirectBuffer bufferTwo, int offsetTwo, int lengthTwo, ReservedValueSupplier reservedValueSupplier = null)
        {
            long newPosition = CLOSED;
            if (!_isClosed)
            {
                long limit = _positionLimit.GetVolatile();
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;
                int length = ValidateAndComputeLength(lengthOne, lengthTwo);

                if (position < limit)
                {
                    int result;
                    if (length <= MaxPayloadLength)
                    {
                        CheckPositiveLength(length);
                        result = termAppender.AppendUnfragmentedMessage(
                            _termId,
                            _termOffset,
                            _headerWriter,
                            bufferOne,
                            offsetOne,
                            lengthOne,
                            bufferTwo,
                            offsetTwo,
                            lengthTwo,
                            reservedValueSupplier);
                    }
                    else
                    {
                        CheckMaxMessageLength(length);
                        result = termAppender.AppendFragmentedMessage(
                            _termId,
                            _termOffset,
                            _headerWriter,
                            bufferOne,
                            offsetOne,
                            lengthOne,
                            bufferTwo,
                            offsetTwo,
                            lengthTwo,
                            MaxPayloadLength,
                            reservedValueSupplier);
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


        /// <inheritdoc />
        public override long Offer(DirectBufferVector[] vectors, ReservedValueSupplier reservedValueSupplier = null)
        {
            int length = DirectBufferVector.ValidateAndComputeLength(vectors);
            var newPosition = CLOSED;
            if (!_isClosed)
            {
                var limit = _positionLimit.GetVolatile();
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;

                if (position < limit)
                {
                    int result;
                    if (length <= MaxPayloadLength)
                    {
                        result = termAppender.AppendUnfragmentedMessage(
                            _termId, _termOffset, _headerWriter, vectors, length, reservedValueSupplier);
                    }
                    else
                    {
                        CheckMaxMessageLength(length);
                        result = termAppender.AppendFragmentedMessage(
                            _termId,
                            _termOffset,
                            _headerWriter,
                            vectors,
                            length,
                            MaxPayloadLength,
                            reservedValueSupplier);
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
        /// Once the message has been written then <seealso cref="BufferClaim.Commit()"/> should be called thus making it available.
        /// 
        /// <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
        /// If the claim is held after the publication is closed, or the client dies, then it will be unblocked to reach
        /// end-of-stream (EOS)
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
        ///             bufferClaim.Commit();
        ///         }
        ///     }
        /// }</pre>
        ///     
        /// </summary>
        /// <param name="length">      of the range to claim, in bytes.. </param>
        /// <param name="bufferClaim"> to be populated if the claim succeeds. </param>
        /// <returns> The new stream position, otherwise <seealso cref="Publication.NOT_CONNECTED"/>, <seealso cref="Publication.BACK_PRESSURED"/>,
        /// <seealso cref="Publication.ADMIN_ACTION"/>, <seealso cref="Publication.CLOSED"/> or <see cref="Publication.MAX_POSITION_EXCEEDED"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than max payload length within an MTU. </exception>
        /// <seealso cref="BufferClaim.Commit()" />
        /// <seealso cref="BufferClaim.Abort()" />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long TryClaim(int length, BufferClaim bufferClaim)
        {
            CheckPayloadLength(length);
            var newPosition = CLOSED;

            if (!_isClosed)
            {
                var limit = _positionLimit.GetVolatile();
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
        /// <returns> The new stream position, otherwise a negative error value of <seealso cref="Publication.NOT_CONNECTED"/>,
        /// <seealso cref="Publication.BACK_PRESSURED"/>, <seealso cref="Publication.ADMIN_ACTION"/>, <seealso cref="Publication.CLOSED"/>, or <seealso cref="Publication.MAX_POSITION_EXCEEDED"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="Publication.MaxMessageLength()"/>. </exception>
        public long AppendPadding(int length)
        {
            CheckMaxMessageLength(length);
            long newPosition = CLOSED;

            if (!_isClosed)
            {
                long limit = _positionLimit.GetVolatile();
                ExclusiveTermAppender termAppender = _termAppenders[_activePartitionIndex];
                long position = _termBeginPosition + _termOffset;

                if (position < limit)
                {
                    CheckPositiveLength(length);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NewPosition(int resultingOffset)
        {
            if (resultingOffset > 0)
            {
                _termOffset = resultingOffset;

                return _termBeginPosition + resultingOffset;
            }

            if ((_termBeginPosition + TermBufferLength) >= MaxPossiblePosition)
            {
                return MAX_POSITION_EXCEEDED;
            }

            int nextIndex = LogBufferDescriptor.NextPartitionIndex(_activePartitionIndex);
            int nextTermId = _termId + 1;

            _activePartitionIndex = nextIndex;
            _termOffset = 0;
            _termId = nextTermId;
            _termBeginPosition =
                LogBufferDescriptor.ComputeTermBeginPosition(nextTermId, PositionBitsToShift, InitialTermId);

            var termCount = nextTermId - InitialTermId;

            LogBufferDescriptor.InitialiseTailWithTermId(_logMetaDataBuffer, nextIndex, nextTermId);
            LogBufferDescriptor.ActiveTermCountOrdered(_logMetaDataBuffer, termCount);

            return ADMIN_ACTION;
        }
    }
}