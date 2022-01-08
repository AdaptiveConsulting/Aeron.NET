/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;

namespace Adaptive.Agrona.Concurrent.RingBuffer
{
    /// <summary>
    /// Ring-buffer for the concurrent exchanging of binary encoded messages from producer to consumer in a FIFO manner.
    /// </summary>
    public interface IRingBuffer
    {
        /// <summary>
        /// Get the capacity of the ring-buffer in bytes for exchange.
        /// </summary>
        /// <returns> the capacity of the ring-buffer in bytes for exchange. </returns>
        int Capacity();

        /// <summary>
        /// Non-blocking write of an message to an underlying ring-buffer.
        /// </summary>
        /// <param name="msgTypeId"> type of the message encoding. </param>
        /// <param name="srcBuffer"> containing the encoded binary message. </param>
        /// <param name="srcIndex"> at which the encoded message begins. </param>
        /// <param name="length"> of the encoded message in bytes. </param>
        /// <returns> true if written to the ring-buffer, or false if insufficient space exists. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="IRingBuffer.MaxMsgLength()"/> </exception>
        bool Write(int msgTypeId, IDirectBuffer srcBuffer, int srcIndex, int length);

        /// <summary>
        /// Read as many messages as are available from the ring buffer.
        /// </summary>
        /// <param name="handler"> to be called for processing each message in turn. </param>
        /// <returns> the number of messages that have been processed. </returns>
        int Read(MessageHandler handler);

        /// <summary>
        /// Read as many messages as are available from the ring buffer to up a supplied maximum.
        /// </summary>
        /// <param name="handler"> to be called for processing each message in turn. </param>
        /// <param name="messageCountLimit"> the number of messages will be read in a single invocation. </param>
        /// <returns> the number of messages that have been processed. </returns>
        int Read(MessageHandler handler, int messageCountLimit);

        /// <summary>
        /// The maximum message length in bytes supported by the underlying ring buffer.
        /// </summary>
        /// <returns> the maximum message length in bytes supported by the underlying ring buffer. </returns>
        int MaxMsgLength();

        /// <summary>
        /// Get the next value that can be used for a correlation id on an message when a response needs to be correlated.
        /// 
        /// This method should be thread safe.
        /// </summary>
        /// <returns> the next value in the correlation sequence. </returns>
        long NextCorrelationId();

        /// <summary>
        /// Get the underlying buffer used by the RingBuffer for storage.
        /// </summary>
        /// <returns> the underlying buffer used by the RingBuffer for storage. </returns>
        IAtomicBuffer Buffer();

        /// <summary>
        /// Set the time of the last consumer heartbeat.
        /// 
        /// <b>Note:</b> The value for time must be valid across processes.
        /// </summary>
        /// <param name="time"> of the last consumer heartbeat. </param>
        void ConsumerHeartbeatTime(long time);

        /// <summary>
        /// The time of the last consumer heartbeat.
        /// </summary>
        /// <returns> the time of the last consumer heartbeat. </returns>
        long ConsumerHeartbeatTime();

        /// <summary>
        /// The position in bytes from start up of the producers.  The figure includes the headers.
        /// This is the range they are working with but could still be in the act of working with.
        /// </summary>
        /// <returns> number of bytes produced by the producers in claimed space. </returns>
        long ProducerPosition();

        /// <summary>
        /// The position in bytes from start up for the consumers.  The figure includes the headers.
        /// </summary>
        /// <returns> the count of bytes consumed by the consumers. </returns>
        long ConsumerPosition();

        /// <summary>
        /// Size of the buffer backlog in bytes between producers and consumers. The figure includes the size of headers.
        /// </summary>
        /// <returns> size of the backlog of bytes in the buffer between producers and consumers. </returns>
        int Size();

        /// <summary>
        /// Unblock a multi-producer ring buffer where a producer has died during the act of offering. The operation will
        /// scan from the consumer position up to the producer position.
        /// 
        /// If no action is required at the position then none will be taken.
        /// </summary>
        /// <returns> true of an unblocking action was taken otherwise false. </returns>
        bool Unblock();

        /// <summary>
        /// Try to claim a space in the underlying ring-buffer into which a message can be written with zero copy semantics.
        /// Once the message has been written then <seealso cref="Commit(int)"/> should be called thus making it available to be
        /// consumed. Alternatively a claim can be aborted using <seealso cref="Abort(int)"/> method.
        /// <para>
        /// Claiming a space in the ring-buffer means that the consumer will not be able to consume past the claim until
        /// the claimed space is either committed or aborted. Producers will be able to write message even when outstanding
        /// claims exist.
        /// </para>
        /// <para>
        /// An example of using {@code TryClaim}:
        /// <pre>
        /// <code>
        ///     final IRingBuffer ringBuffer = ...;
        /// 
        ///     final int index = ringBuffer.TryClaim(msgTypeId, messageLength);
        ///     if (index > 0)
        ///     {
        ///         try
        ///         {
        ///             final AtomicBuffer buffer = ringBuffer.Buffer();
        ///             // Work with the buffer directly using the index
        ///             ...
        ///         }
        ///         finally
        ///         {
        ///             ringBuffer.Commit(index); // commit message
        ///         }
        ///     }
        /// </code>
        /// </pre>
        /// </para>
        /// <para>
        /// Ensure that claimed space is released even in case of an exception:
        /// <pre>
        /// <code>
        ///     final IRingBuffer ringBuffer = ...;
        /// 
        ///     final int index = ringBuffer.TryClaim(msgTypeId, messageLength);
        ///     if (index > 0)
        ///     {
        ///         try
        ///         {
        ///             final IAtomicBuffer buffer = ringBuffer.buffer();
        ///             // Work with the buffer directly using the index
        ///             ...
        ///             ringBuffer.commit(index); // commit message
        ///         }
        ///         catch (final Throwable t)
        ///         {
        ///             ringBuffer.abort(index); // allow consumer to proceed
        ///             ...
        ///         }
        ///     }
        /// </code>
        /// </pre>
        /// 
        /// </para>
        /// </summary>
        /// <param name="msgTypeId"> type of the message encoding. Will be written into the header upon successful claim. </param>
        /// <param name="length">    of the claim in bytes. A claim length cannot be greater than <seealso cref="MaxMsgLength()"/>. </param>
        /// <returns> a non-zero index into the underlying ring-buffer at which encoded message begins, otherwise returns
        /// <seealso cref="ManyToOneRingBuffer.InsufficientCapacity"/> indicating that there is not enough free space in the buffer. </returns>
        /// <exception cref="ArgumentException"> if the {@code msgTypeId} is less than {@code 1}. </exception>
        /// <exception cref="InvalidOperationException"> if the {@code length} is negative or is greater than <seealso cref="MaxMsgLength()"/>. </exception>
        /// <seealso cref="Commit(int)"/>
        /// <seealso cref="Abort(int)"/>
        int TryClaim(int msgTypeId, int length);

        /// <summary>
        /// Commit message that was written in the previously claimed space thus making it available to the consumer.
        /// </summary>
        /// <param name="index"> at which the encoded message begins, i.e. value returned from the <seealso cref="TryClaim(int, int)"/> call. </param>
        /// <exception cref="ArgumentException"> if the {@code index} is out of bounds. </exception>
        /// <exception cref="InvalidOperationException">    if this method is called after <seealso cref="Commit(int)"/> or <seealso cref="Abort(int)"/> was
        ///                                  already invoked for the given {@code index}. </exception>
        /// <seealso cref="TryClaim(int, int)"/>
        void Commit(int index);

        /// <summary>
        /// Abort claim and allow consumer to proceed after the claimed length. Aborting turns unused space into padding,
        /// i.e. changes type of the message to <seealso cref="ManyToOneRingBuffer.PaddingMsgTypeId"/>.
        /// </summary>
        /// <param name="index"> at which the encoded message begins, i.e. value returned from the <seealso cref="TryClaim(int, int)"/> call. </param>
        /// <exception cref="ArgumentException"> if the {@code index} is out of bounds. </exception>
        /// <exception cref="InvalidOperationException">    if this method is called after <seealso cref="Commit(int)"/> or <seealso cref="Abort(int)"/> was
        ///                                  already invoked for the given {@code index}. </exception>
        /// <seealso cref="TryClaim(int, int)"/>
        void Abort(int index);
    }
}