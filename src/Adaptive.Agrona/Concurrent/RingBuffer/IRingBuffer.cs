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
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="IRingBuffer#maxMsgLength()"/> </exception>
        bool Write(int msgTypeId, IDirectBuffer srcBuffer, int srcIndex, int length);

        /// <summary>
        /// Read as many messages as are available from the ring buffer.
        /// </summary>
        /// <param name="handler"> to be called for processing each message in turn. </param>
        /// <returns> the number of messages that have been processed. </returns>
        int Read(IMessageHandler handler);

        /// <summary>
        /// Read as many messages as are available from the ring buffer to up a supplied maximum.
        /// </summary>
        /// <param name="handler"> to be called for processing each message in turn. </param>
        /// <param name="messageCountLimit"> the number of messages will be read in a single invocation. </param>
        /// <returns> the number of messages that have been processed. </returns>
        int Read(IMessageHandler handler, int messageCountLimit);

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
        /// TODO JW not applicable in .net
        /// <b>Note:</b> The value for time must be valid across processes which means <seealso cref="System#nanoTime()"/>
        /// is not a valid option.
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
        /// Size of the backlog of bytes in the buffer between producers and consumers. The figure includes the size of headers.
        /// </summary>
        /// <returns> size of the backlog of bytes in the buffer between producers and consumers. </returns>
        int Size();

        /// <summary>
        /// Unblock a multi-producer ring buffer where a producer has died during the act of offering. The operation will scan from
        /// the consumer position up to the producer position.
        /// 
        /// If no action is required at the position then none will be taken.
        /// </summary>
        /// <returns> true of an unblocking action was taken otherwise false. </returns>
        bool Unblock();
    }
}