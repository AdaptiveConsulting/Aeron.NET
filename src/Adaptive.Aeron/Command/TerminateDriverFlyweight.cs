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
    ///  |                         Correlation ID                        |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                         Token Length                          |
    ///  +---------------------------------------------------------------+
    ///  |                         Token Buffer                         ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    public class TerminateDriverFlyweight : CorrelatedMessageFlyweight
    {
        private static readonly int TOKEN_LENGTH_OFFSET = CORRELATION_ID_FIELD_OFFSET + BitUtil.SIZE_OF_LONG;

        /// <summary>
        /// Offset of the token buffer
        /// </summary>
        /// <returns> offset of the token buffer </returns>
        public int TokenBufferOffset()
        {
            return TOKEN_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
        }

        /// <summary>
        /// Length of the token buffer in bytes
        /// </summary>
        /// <returns> length of token buffer in bytes </returns>
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
        /// <returns> flyweight </returns>
        public TerminateDriverFlyweight TokenBuffer(IDirectBuffer tokenBuffer, int tokenOffset, int tokenLength)
        {
            buffer.PutInt(TOKEN_LENGTH_OFFSET, tokenLength);
            if (null != tokenBuffer && tokenLength > 0)
            {
                buffer.PutBytes(TokenBufferOffset(), tokenBuffer, tokenOffset, tokenLength);
            }

            return this;
        }

        /// <summary>
        /// Get the length of the current message.
        /// <para>
        /// NB: must be called after the data is written in order to be accurate.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the length of the current message </returns>
        public int Length()
        {
            return TokenBufferOffset() + buffer.GetInt(offset + TOKEN_LENGTH_OFFSET);
        }
    }
}