using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Message to denote that a Counter has become available or unavailable.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents">
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                         Correlation ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                           Counter ID                          |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </seealso>
    public class CounterUpdateFlyweight
    {
        private const int CORRELATION_ID_OFFSET = 0;
        private static readonly int COUNTER_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;

        public static readonly int LENGTH = BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_INT;

        private IMutableDirectBuffer buffer;
        private int offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public CounterUpdateFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this.buffer = buffer;
            this.offset = offset;

            return this;
        }

        /// <summary>
        /// Get the correlation id field
        /// </summary>
        /// <returns> correlation id field </returns>
        public long CorrelationId()
        {
            return buffer.GetLong(offset + CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> flyweight </returns>
        public CounterUpdateFlyweight CorrelationId(long correlationId)
        {
            buffer.PutLong(offset + CORRELATION_ID_OFFSET, correlationId);

            return this;
        }

        /// <summary>
        /// The counter id
        /// </summary>
        /// <returns> counter id </returns>
        public int CounterId()
        {
            return buffer.GetInt(offset + COUNTER_ID_OFFSET);
        }

        /// <summary>
        /// Set counter id field
        /// </summary>
        /// <param name="counterId"> field value </param>
        /// <returns> flyweight </returns>
        public CounterUpdateFlyweight CounterId(int counterId)
        {
            buffer.PutInt(offset + COUNTER_ID_OFFSET, counterId);

            return this;
        }
    }
}