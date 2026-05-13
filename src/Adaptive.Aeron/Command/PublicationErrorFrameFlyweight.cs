/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Net;
using Adaptive.Agrona;

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// Control message flyweight error frames received by a publication to be reported to the client.
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                 Publication Registration Id                   |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                 Destination Registration Id                   |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                          Session ID                           |
    ///  +---------------------------------------------------------------+
    ///  |                           Stream ID                           |
    ///  +---------------------------------------------------------------+
    ///  |                          Receiver ID                          |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                           Group Tag                           |
    ///  |                                                               |
    ///  +-------------------------------+-------------------------------+
    ///  |          Address Type         |            UDP Port           |
    ///  +-------------------------------+-------------------------------+
    ///  |           IPv4 or IPv6 Address padded out to 16 bytes         |
    ///  |                                                               |
    ///  |                                                               |
    ///  |                                                               |
    ///  +---------------------------------------------------------------+
    ///  |                          Error Code                           |
    ///  +---------------------------------------------------------------+
    ///  |                      Error Message Length                     |
    ///  +---------------------------------------------------------------+
    ///  |                         Error Message                        ...
    /// ...                                                              |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// @since 1.47.0
    /// </summary>
    public class PublicationErrorFrameFlyweight
    {
        private const int RegistrationIdOffset = 0;
        private const int Ipv6AddressLength = 16;
        private static readonly int Ipv4AddressLength = BitUtil.SIZE_OF_INT;
        private static readonly int DestinationRegistrationIdOffset = RegistrationIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int SessionIdOffset = DestinationRegistrationIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int StreamIdOffset = SessionIdOffset + BitUtil.SIZE_OF_INT;
        private static readonly int ReceiverIdOffset = StreamIdOffset + BitUtil.SIZE_OF_INT;
        private static readonly int GroupTagOffset = ReceiverIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int AddressTypeOffset = GroupTagOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int AddressPortOffset = AddressTypeOffset + BitUtil.SIZE_OF_SHORT;
        private static readonly int AddressOffset = AddressPortOffset + BitUtil.SIZE_OF_SHORT;
        private static readonly int ErrorCodeOffset = AddressOffset + Ipv6AddressLength;
        private static readonly int ErrorMessageOffset = ErrorCodeOffset + BitUtil.SIZE_OF_INT;
        private const short AddressTypeIpv4 = 1;
        private const short AddressTypeIpv6 = 2;

        private IMutableDirectBuffer _buffer;
        private int _offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            _buffer = buffer;
            _offset = offset;

            return this;
        }

        /// <summary>
        /// Return registration ID of the publication that received the error frame.
        /// </summary>
        /// <returns> registration ID of the publication. </returns>
        public long RegistrationId()
        {
            return _buffer.GetLong(_offset + RegistrationIdOffset);
        }

        /// <summary>
        /// Set the registration ID of the publication that received the error frame.
        /// </summary>
        /// <param name="registrationId"> of the publication. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight RegistrationId(long registrationId)
        {
            _buffer.PutLong(_offset + RegistrationIdOffset, registrationId);
            return this;
        }

        /// <summary>
        /// Return registration id of the destination that received the error frame. This will only be set if the
        /// publication is using manual MDC.
        /// </summary>
        /// <returns> registration ID of the publication or <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public long DestinationRegistrationId()
        {
            return _buffer.GetLong(_offset + DestinationRegistrationIdOffset);
        }

        /// <summary>
        /// Set the registration ID of the destination that received the error frame. Use
        /// <seealso cref="Aeron.NULL_VALUE"/>
        /// if not set.
        /// </summary>
        /// <param name="registrationId"> of the destination. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight DestinationRegistrationId(long registrationId)
        {
            _buffer.PutLong(_offset + DestinationRegistrationIdOffset, registrationId);
            return this;
        }

        /// <summary>
        /// Get the stream id field.
        /// </summary>
        /// <returns> stream id field. </returns>
        public int StreamId()
        {
            return _buffer.GetInt(_offset + StreamIdOffset);
        }

        /// <summary>
        /// Set the stream id field.
        /// </summary>
        /// <param name="streamId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight StreamId(int streamId)
        {
            _buffer.PutInt(_offset + StreamIdOffset, streamId);

            return this;
        }

        /// <summary>
        /// Get the session id field.
        /// </summary>
        /// <returns> session id field. </returns>
        public int SessionId()
        {
            return _buffer.GetInt(_offset + SessionIdOffset);
        }

        /// <summary>
        /// Set session id field.
        /// </summary>
        /// <param name="sessionId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight SessionId(int sessionId)
        {
            _buffer.PutInt(_offset + SessionIdOffset, sessionId);

            return this;
        }

        /// <summary>
        /// Get the receiver id field.
        /// </summary>
        /// <returns> get the receiver id field. </returns>
        public long ReceiverId()
        {
            return _buffer.GetLong(_offset + ReceiverIdOffset);
        }

        /// <summary>
        /// Set receiver id field.
        /// </summary>
        /// <param name="receiverId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight ReceiverId(long receiverId)
        {
            _buffer.PutLong(_offset + ReceiverIdOffset, receiverId);

            return this;
        }

        /// <summary>
        /// Get the group tag field.
        /// </summary>
        /// <returns> the group tag field. </returns>
        public long GroupTag()
        {
            return _buffer.GetLong(_offset + GroupTagOffset);
        }

        /// <summary>
        /// Set the group tag field.
        /// </summary>
        /// <param name="groupTag"> the group tag value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight GroupTag(long groupTag)
        {
            _buffer.PutLong(_offset + GroupTagOffset, groupTag);

            return this;
        }

        /// <summary>
        /// Get the source address of this error frame.
        /// </summary>
        /// <returns> source address of the error frame. </returns>
        public IPEndPoint SourceAddress()
        {
            short addressType = _buffer.GetShort(_offset + AddressTypeOffset);
            int port = _buffer.GetShort(_offset + AddressPortOffset) & 0xFFFF;

            byte[] address;
            if (AddressTypeIpv4 == addressType)
            {
                address = new byte[Ipv4AddressLength];
            }
            else if (AddressTypeIpv6 == addressType)
            {
                address = new byte[Ipv6AddressLength];
            }
            else
            {
                throw new ArgumentException("Unknown address type:" + addressType);
            }

            _buffer.GetBytes(_offset + AddressOffset, address);
            try
            {
                return new IPEndPoint(new IPAddress(address), port);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Unknown address type:" + addressType, ex);
            }
        }

        /// <summary>
        /// Error code for the command.
        /// </summary>
        /// <returns> error code for the command. </returns>
        public ErrorCode ErrorCode()
        {
            return (ErrorCode)_buffer.GetInt(_offset + ErrorCodeOffset);
        }

        /// <summary>
        /// Error code value for the command.
        /// </summary>
        /// <returns> error code value for the command. </returns>
        public int ErrorCodeValue()
        {
            return _buffer.GetInt(_offset + ErrorCodeOffset);
        }

        /// <summary>
        /// Set the error code for the command.
        /// </summary>
        /// <param name="code"> for the error. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight ErrorCode(ErrorCode code)
        {
            _buffer.PutInt(_offset + ErrorCodeOffset, (int)code);
            return this;
        }

        /// <summary>
        /// Error message associated with the error.
        /// </summary>
        /// <returns> error message. </returns>
        public string ErrorMessage()
        {
            return _buffer.GetStringAscii(_offset + ErrorMessageOffset);
        }

        /// <summary>
        /// Set the error message.
        /// </summary>
        /// <param name="message"> to associate with the error. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight ErrorMessage(string message)
        {
            _buffer.PutStringAscii(_offset + ErrorMessageOffset, message);
            return this;
        }

        /// <summary>
        /// Length of the error response in bytes.
        /// </summary>
        /// <returns> length of the error response in bytes. </returns>
        public int Length()
        {
            return ErrorMessageOffset + BitUtil.SIZE_OF_INT + _buffer.GetInt(_offset + ErrorMessageOffset);
        }
    }
}
