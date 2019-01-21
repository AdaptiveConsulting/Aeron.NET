using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Indicate a client has timed out by the driver.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents"/>
    /// <pre>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |                         Client Id                             |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// </pre>
    public class ClientTimeoutFlyweight
    {
        private const int CLIENT_ID_FIELD_OFFSET = 0;

        public static readonly int LENGTH = BitUtil.SIZE_OF_LONG;

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> for fluent API </returns>
        public ClientTimeoutFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// return client id field
        /// </summary>
        /// <returns> client id field </returns>
        public long ClientId()
        {
            return _buffer.GetLong(_offset + CLIENT_ID_FIELD_OFFSET);
        }

        /// <summary>
        /// set client id field
        /// </summary>
        /// <param name="clientId"> field value </param>
        /// <returns> for fluent API </returns>
        public ClientTimeoutFlyweight ClientId(long clientId)
        {
            _buffer.PutLong(_offset + CLIENT_ID_FIELD_OFFSET, clientId);

            return this;
        }
    }
}