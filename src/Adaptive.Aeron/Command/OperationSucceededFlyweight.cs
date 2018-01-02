using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Indicate a given operation is done and has succeeded.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents">
    /// <pre>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                         Correlation ID                        |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// </pre>
    /// </seealso>
    public class OperationSucceededFlyweight
    {
        private const int CORRELATION_ID_FIELD_OFFSET = 0;

        public static readonly int LENGTH = BitUtil.SIZE_OF_LONG;

        private IMutableDirectBuffer buffer;
        private int offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public OperationSucceededFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this.buffer = buffer;
            this.offset = offset;

            return this;
        }

        /// <summary>
        /// return correlation id field
        /// </summary>
        /// <returns> correlation id field </returns>
        public virtual long CorrelationId()
        {
            return buffer.GetLong(offset + CORRELATION_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set correlation id field
        /// </summary>
        /// <param name="correlationId"> field value </param>
        /// <returns> for fluent API </returns>
        public virtual OperationSucceededFlyweight CorrelationId(long correlationId)
        {
            buffer.PutLong(offset + CORRELATION_ID_FIELD_OFFSET, correlationId);

            return this;
        }
    }
}