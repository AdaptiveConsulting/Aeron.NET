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
        private const int REGISTRATION_ID_OFFSET = 0;
        private const int IPV6_ADDRESS_LENGTH = 16;
        private static readonly int Ipv4AddressLength = BitUtil.SIZE_OF_INT;
        private static readonly int DestinationRegistrationIdOffset = REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
        private static readonly int SessionIdOffset = DestinationRegistrationIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int StreamIdOffset = SessionIdOffset + BitUtil.SIZE_OF_INT;
        private static readonly int ReceiverIdOffset = StreamIdOffset + BitUtil.SIZE_OF_INT;
        private static readonly int GroupTagOffset = ReceiverIdOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int AddressTypeOffset = GroupTagOffset + BitUtil.SIZE_OF_LONG;
        private static readonly int AddressPortOffset = AddressTypeOffset + BitUtil.SIZE_OF_SHORT;
        private static readonly int AddressOffset = AddressPortOffset + BitUtil.SIZE_OF_SHORT;
        private static readonly int ErrorCodeOffset = AddressOffset + IPV6_ADDRESS_LENGTH;
        private static readonly int ErrorMessageOffset = ErrorCodeOffset + BitUtil.SIZE_OF_INT;
        private const short ADDRESS_TYPE_IPV4 = 1;
        private const short ADDRESS_TYPE_IPV6 = 2;

        private IMutableDirectBuffer Buffer;
        private int Offset;

        /// <summary>
        /// Wrap the buffer at a given offset for updates.
        /// </summary>
        /// <param name="buffer"> to wrap. </param>
        /// <param name="offset"> at which the message begins. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight Wrap(IMutableDirectBuffer buffer, int offset)
        {
            this.Buffer = buffer;
            this.Offset = offset;

            return this;
        }

        /// <summary>
        /// Return registration ID of the publication that received the error frame.
        /// </summary>
        /// <returns> registration ID of the publication. </returns>
        public long RegistrationId()
        {
            return Buffer.GetLong(Offset + REGISTRATION_ID_OFFSET);
        }

        /// <summary>
        /// Set the registration ID of the publication that received the error frame.
        /// </summary>
        /// <param name="registrationId"> of the publication. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight RegistrationId(long registrationId)
        {
            Buffer.PutLong(Offset + REGISTRATION_ID_OFFSET, registrationId);
            return this;
        }

        /// <summary>
        /// Return registration id of the destination that received the error frame. This will only be set if the publication
        /// is using manual MDC.
        /// </summary>
        /// <returns> registration ID of the publication or <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public long DestinationRegistrationId()
        {
            return Buffer.GetLong(Offset + DestinationRegistrationIdOffset);
        }

        /// <summary>
        /// Set the registration ID of the destination that received the error frame. Use <seealso cref="Aeron.NULL_VALUE"/>
        /// if not set.
        /// </summary>
        /// <param name="registrationId"> of the destination. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight DestinationRegistrationId(long registrationId)
        {
            Buffer.PutLong(Offset + DestinationRegistrationIdOffset, registrationId);
            return this;
        }

        /// <summary>
        /// Get the stream id field.
        /// </summary>
        /// <returns> stream id field. </returns>
        public int StreamId()
        {
            return Buffer.GetInt(Offset + StreamIdOffset);
        }

        /// <summary>
        /// Set the stream id field.
        /// </summary>
        /// <param name="streamId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight StreamId(int streamId)
        {
            Buffer.PutInt(Offset + StreamIdOffset, streamId);

            return this;
        }

        /// <summary>
        /// Get the session id field.
        /// </summary>
        /// <returns> session id field. </returns>
        public int SessionId()
        {
            return Buffer.GetInt(Offset + SessionIdOffset);
        }

        /// <summary>
        /// Set session id field.
        /// </summary>
        /// <param name="sessionId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight SessionId(int sessionId)
        {
            Buffer.PutInt(Offset + SessionIdOffset, sessionId);

            return this;
        }

        /// <summary>
        /// Get the receiver id field.
        /// </summary>
        /// <returns> get the receiver id field. </returns>
        public long ReceiverId()
        {
            return Buffer.GetLong(Offset + ReceiverIdOffset);
        }

        /// <summary>
        /// Set receiver id field.
        /// </summary>
        /// <param name="receiverId"> field value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight ReceiverId(long receiverId)
        {
            Buffer.PutLong(Offset + ReceiverIdOffset, receiverId);

            return this;
        }

        /// <summary>
        /// Get the group tag field.
        /// </summary>
        /// <returns> the group tag field. </returns>
        public long GroupTag()
        {
            return Buffer.GetLong(Offset + GroupTagOffset);
        }

        /// <summary>
        /// Set the group tag field.
        /// </summary>
        /// <param name="groupTag"> the group tag value. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight GroupTag(long groupTag)
        {
            Buffer.PutLong(Offset + GroupTagOffset, groupTag);

            return this;
        }

        /// <summary>
        /// Get the source address of this error frame.
        /// </summary>
        /// <returns> source address of the error frame. </returns>
        public IPEndPoint SourceAddress()
        {
            short addressType = Buffer.GetShort(Offset + AddressTypeOffset);
            int port = Buffer.GetShort(Offset + AddressPortOffset) & 0xFFFF;

            byte[] address;
            if (ADDRESS_TYPE_IPV4 == addressType)
            {
                address = new byte[Ipv4AddressLength];
            }
            else if (ADDRESS_TYPE_IPV6 == addressType)
            {
                address = new byte[IPV6_ADDRESS_LENGTH];
            }
            else
            {
                throw new ArgumentException("Unknown address type:" + addressType);
            }

            Buffer.GetBytes(Offset + AddressOffset, address);
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
            return (ErrorCode)Buffer.GetInt(Offset + ErrorCodeOffset);
        }

        /// <summary>
        /// Error code value for the command.
        /// </summary>
        /// <returns> error code value for the command. </returns>
        public int ErrorCodeValue()
        {
            return Buffer.GetInt(Offset + ErrorCodeOffset);
        }

        /// <summary>
        /// Set the error code for the command.
        /// </summary>
        /// <param name="code"> for the error. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight ErrorCode(ErrorCode code)
        {
            Buffer.PutInt(Offset + ErrorCodeOffset, (int)code);
            return this;
        }

        /// <summary>
        /// Error message associated with the error.
        /// </summary>
        /// <returns> error message. </returns>
        public string ErrorMessage()
        {
            return Buffer.GetStringAscii(Offset + ErrorMessageOffset);
        }

        /// <summary>
        /// Set the error message.
        /// </summary>
        /// <param name="message"> to associate with the error. </param>
        /// <returns> this for a fluent API. </returns>
        public PublicationErrorFrameFlyweight ErrorMessage(string message)
        {
            Buffer.PutStringAscii(Offset + ErrorMessageOffset, message);
            return this;
        }

        /// <summary>
        /// Length of the error response in bytes.
        /// </summary>
        /// <returns> length of the error response in bytes. </returns>
        public int Length()
        {
            return ErrorMessageOffset + BitUtil.SIZE_OF_INT + Buffer.GetInt(Offset + ErrorMessageOffset);
        }
    }
}