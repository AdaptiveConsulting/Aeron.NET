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
        static readonly int TOKEN_BUFFER_OFFSET = TOKEN_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;
        private static readonly int MINIMUM_LENGTH = TOKEN_LENGTH_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Relative offset of the token buffer
        /// </summary>
        /// <returns> relative offset of the token buffer </returns>
        public int TokenBufferOffset()
        {
            return TOKEN_BUFFER_OFFSET;
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
        /// NB: must be called after the data is written in order to be accurate.
        /// 
        /// </para>
        /// </summary>
        /// <returns> the length of the current message </returns>
        public int Length()
        {
            return TokenBufferOffset() + TokenBufferLength();
        }

        /// <summary>
        /// Validate buffer length is long enough for message.
        /// </summary>
        /// <param name="msgTypeId"> type of message. </param>
        /// <param name="length">    of message in bytes to validate. </param>
        public new void ValidateLength(int msgTypeId, int length)
        {
            if (length < MINIMUM_LENGTH)
            {
                throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND,
                    "command=" + msgTypeId + " too short: length=" + length);
            }

            if ((length - MINIMUM_LENGTH) < buffer.GetInt(offset + TOKEN_LENGTH_OFFSET))
            {
                throw new ControlProtocolException(ErrorCode.MALFORMED_COMMAND,
                    "command=" + msgTypeId + " too short for token buffer: length=" + length);
            }
        }
    }
}