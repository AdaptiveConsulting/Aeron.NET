using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Command message flyweight to ask the driver process to terminate.
    /// </summary>
    /// <seealso cref="ControlProtocolEvents"/>
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                          Client ID                            |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                        Correlation ID                         |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                         Token Length                          |
    ///  +---------------------------------------------------------------+
    ///  |                         Token Buffer                         ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// <see cref="ControlProtocolEvents"/>
    public class TerminateDriverFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int TOKEN_LENGTH_OFFSET = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;
        static readonly int TOKEN_BUFFER_OFFSET = TOKEN_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int MINIMUM_LENGTH = TOKEN_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public new TerminateDriverFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            base.Wrap(buffer, offset);
            return this;
        }
        
        /// <summary>
        /// Relative offset of the token buffer.
        /// </summary>
        /// <returns> relative offset of the token buffer. </returns>
        public int TokenBufferOffset()
        {
            return TOKEN_BUFFER_OFFSET;
        }

        /// <summary>
        /// Length of the token buffer in bytes.
        /// </summary>
        /// <returns> length of token buffer in bytes. </returns>
        public int TokenBufferLength()
        {
            return buffer.GetInt(offset + TOKEN_LENGTH_OFFSET);
        }

        /// <summary>
        /// Fill the token buffer.
        /// </summary>
        /// <param name="tokenBuffer"> containing the optional token for the request. </param>
        /// <param name="tokenOffset"> within the tokenBuffer at which the token begins. </param>
        /// <param name="tokenLength"> of the token in the tokenBuffer. </param>
        /// <returns> this for a fluent API. </returns>
        public TerminateDriverFlyweight TokenBuffer(IDirectBuffer tokenBuffer, int tokenOffset, int tokenLength)
        {
            buffer.PutInt(offset + TOKEN_LENGTH_OFFSET, tokenLength);
            if (null != tokenBuffer && tokenLength > 0)
            {
                buffer.PutBytes(offset + TokenBufferOffset(), tokenBuffer, tokenOffset, tokenLength);
            }

            return this;
        }

        /// <summary>
        /// Get the length of the current message.
        /// <para>
        /// NB: must be called after the data is written in order to be correct.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the length of the current message </returns>
        public int Length()
        {
            return TokenBufferOffset() + TokenBufferLength();
        }

        /// <summary>
        /// Compute the length of the command message for a given token length.
        /// </summary>
        /// <param name="tokenLength"> to be appended to the header. </param>
        /// <returns> the length of the command message for a given token length. </returns>
        public static int ComputeLength(int tokenLength)
        {
            if (tokenLength < 0)
            {
                throw new ConfigurationException("token length must be >= 0: " + tokenLength);
            }

            return LENGTH + BitUtil.SIZE_OF_INT + tokenLength;
        }
    }
}