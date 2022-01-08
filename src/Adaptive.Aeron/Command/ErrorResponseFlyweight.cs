/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Text;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message flyweight for any errors sent from driver to clients
    /// 
    /// <para>
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |              Offending Command Correlation ID                 |
    /// |                                                               |
    /// +---------------------------------------------------------------+
    /// |                         Error Code                            |
    /// +---------------------------------------------------------------+
    /// |                   Error Message Length                        |
    /// +---------------------------------------------------------------+
    /// |                       Error Message                          ...
    /// ...                                                             |
    /// +---------------------------------------------------------------+
    /// </para>
    /// </summary>
    public class ErrorResponseFlyweight
    {
        private const int OFFENDING_COMMAND_CORRELATION_ID_OFFSET = 0;
        private static readonly int ERROR_CODE_OFFSET = OFFENDING_COMMAND_CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int ERROR_MESSAGE_OFFSET = ERROR_CODE_OFFSET + BitUtil.SIZE_OF_INT;

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for fluent API. </returns>
        public ErrorResponseFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this._buffer = buffer;
            this._offset = offset;

            return this;
        }

        /// <summary>
        /// Return correlation ID of the offending command.
        /// </summary>
        /// <returns> correlation ID of the offending command </returns>
        public long OffendingCommandCorrelationId()
        {
            return _buffer.GetLong(_offset + OFFENDING_COMMAND_CORRELATION_ID_OFFSET);
        }

        /// <summary>
        /// Set correlation ID of the offending command.
        /// </summary>
        /// <param name="correlationId"> of the offending command. </param>
        /// <returns> this for a fluent API. </returns>
        public ErrorResponseFlyweight OffendingCommandCorrelationId(long correlationId)
        {
            _buffer.PutLong(_offset + OFFENDING_COMMAND_CORRELATION_ID_OFFSET, correlationId);
            return this;
        }

        /// <summary>
        /// Error code for the command.
        /// </summary>
        /// <returns> error code for the command. </returns>
        public ErrorCode ErrorCode()
        {
            
            
            return (ErrorCode)_buffer.GetInt(_offset + ERROR_CODE_OFFSET);
        }
        
        /// <summary>
        /// Error code value for the command.
        /// </summary>
        /// <returns>error code value for the command.</returns>
        public int ErrorCodeValue()
        {
            return _buffer.GetInt(_offset + ERROR_CODE_OFFSET);
        }

        /// <summary>
        /// Set the error code for the command.
        /// </summary>
        /// <param name="code"> for the error.</param>
        /// <returns> this for a fluent API. </returns>
        public ErrorResponseFlyweight ErrorCode(ErrorCode code)
        {
            _buffer.PutInt(_offset + ERROR_CODE_OFFSET, (int)code);
            return this;
        }

        /// <summary>
        /// Error message associated with the error.
        /// </summary>
        /// <returns> error message </returns>
        public string ErrorMessage()
        {
            return _buffer.GetStringAscii(_offset + ERROR_MESSAGE_OFFSET);
        }

        /// <summary>
        /// Append the error message to an appendable without allocation.
        /// </summary>
        /// <param name="stringBuilder"> to append error message to. </param>
        /// <returns> number bytes copied. </returns>
        public int AppendMessage(StringBuilder stringBuilder)
        {
            return _buffer.GetStringAscii(_offset + ERROR_MESSAGE_OFFSET, stringBuilder);
        }
        
        /// <summary>
        /// Set the error message
        /// </summary>
        /// <param name="message"> to associate with the error </param>
        /// <returns> this for a fluent API. </returns>
        public ErrorResponseFlyweight ErrorMessage(string message)
        {
            _buffer.PutStringAscii(_offset + ERROR_MESSAGE_OFFSET, message);
            return this;
        }

        /// <summary>
        /// Length of the error response in bytes.
        /// </summary>
        /// <returns> length of the error response in bytes. </returns>
        public int Length()
        {
            return ERROR_MESSAGE_OFFSET + BitUtil.SIZE_OF_INT + _buffer.GetInt(_offset + ERROR_MESSAGE_OFFSET);
        }
    }
}