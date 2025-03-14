using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Message to denote that a Subscription has been successfully set up.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents">
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                        Correlation ID                         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                  Channel Status Indicator ID                  |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </seealso>
    public class SubscriptionReadyFlyweight
    {
        /// <summary>
        /// Length of the header.
        /// </summary>
        public static readonly int LENGTH = BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_INT;
        private const int CORRELATION_ID_OFFSET = 0;
        private static readonly int CHANNEL_STATUS_INDICATOR_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        private IMutableDirectBuffer buffer;
        private int offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public SubscriptionReadyFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this.buffer = buffer;
            this.offset = offset;

            return this;
        }

        /// <summary>
        /// Get the correlation id field.
        /// </summary>
        /// <returns> correlation id field. </returns>
        public long CorrelationId()
        {
            return buffer.GetLong(offset + CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation id field.
        /// </summary>
        /// <param name="correlationId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public SubscriptionReadyFlyweight CorrelationId(long correlationId)
        {
            buffer.PutLong(offset + CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// The channel status counter id
        /// </summary>
        /// <returns> channel status counter id </returns>
        public int ChannelStatusCounterId()
        {
            return buffer.GetInt(offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET);
        }

        /// <summary>
        /// Set channel status counter id field.
        /// </summary>
        /// <param name="counterId"> field value </param>
        /// <returns> this for a fluent API. </returns>
        public SubscriptionReadyFlyweight ChannelStatusCounterId(int counterId)
        {
            buffer.PutInt(offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET, counterId);

            return this;
        }
    }
}