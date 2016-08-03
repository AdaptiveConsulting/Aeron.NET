using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message for adding or removing a subscription.
    /// 
    /// <para>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                    Command Correlation ID                     |
    /// +---------------------------------------------------------------+
    /// |                  Registration Correlation ID                  |
    /// +---------------------------------------------------------------+
    /// |                           Stream Id                           |
    /// +---------------------------------------------------------------+
    /// |       Channel Length         |   Channel                     ...
    /// |                                                              ...
    /// +---------------------------------------------------------------+
    /// </para>
    /// </summary>
    public class SubscriptionMessageFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int REGISTRATION_CORRELATION_ID_OFFSET = CORRELATION_ID_FIELD_OFFSET +
                                                                         BitUtil.SIZE_OF_LONG;

        private static readonly int STREAM_ID_OFFSET = REGISTRATION_CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int CHANNEL_OFFSET = STREAM_ID_OFFSET + BitUtil.SIZE_OF_INT;

        private int _lengthOfChannel;

        /// <summary>
        /// return correlation id used in registration field
        /// </summary>
        /// <returns> correlation id field </returns>
        public long RegistrationCorrelationId()
        {
            return buffer.GetLong(offset + REGISTRATION_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// set registration correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> flyweight </returns>
        public SubscriptionMessageFlyweight RegistrationCorrelationId(long correlationId)
        {
            buffer.PutLong(offset + REGISTRATION_CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// return the stream id
        /// </summary>
        /// <returns> the stream id </returns>
        public int StreamId()
        {
            return buffer.GetInt(offset + STREAM_ID_OFFSET);
        }

        /// <summary>
        /// Set the stream id
        /// </summary>
        /// <param name="streamId"> the channel id </param>
        /// <returns> flyweight </returns>
        public SubscriptionMessageFlyweight StreamId(int streamId)
        {
            buffer.PutInt(offset + STREAM_ID_OFFSET, streamId);

            return this;
        }

        /// <summary>
        /// return the channel field
        /// </summary>
        /// <returns> channel field </returns>
        public string Channel()
        {
            return buffer.GetStringUtf8(offset + CHANNEL_OFFSET);
        }

        /// <summary>
        /// Set channel field
        /// </summary>
        /// <param name="channel"> field value </param>
        /// <returns> flyweight </returns>
        public SubscriptionMessageFlyweight Channel(string channel)
        {
            _lengthOfChannel = buffer.PutStringUtf8(offset + CHANNEL_OFFSET, channel);

            return this;
        }

        public int Length()
        {
            return CHANNEL_OFFSET + _lengthOfChannel;
        }
    }
}