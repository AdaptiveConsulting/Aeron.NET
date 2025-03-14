using System;
using System.Net;
using Adaptive.Aeron.Command;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Encapsulates the data received when a publication receives an error frame.
    /// </summary>
    public class PublicationErrorFrame : ICloneable
    {
        private long registrationId;
        private int sessionId;
        private int streamId;
        private long receiverId;
        private long destinationRegistrationId;
        private long groupTag;
        private ErrorCode errorCode;
        private string errorMessage;
        private IPEndPoint sourceAddress;

        /// <summary>
        /// Registration id of the publication that received the error frame.
        /// </summary>
        /// <returns> registration id of the publication. </returns>
        public long RegistrationId()
        {
            return registrationId;
        }

        /// <summary>
        /// Session id of the publication that received the error frame.
        /// </summary>
        /// <returns> session id of the publication. </returns>
        public int SessionId()
        {
            return sessionId;
        }

        /// <summary>
        /// Stream id of the publication that received the error frame.
        /// </summary>
        /// <returns> stream id of the publication. </returns>
        public int StreamId()
        {
            return streamId;
        }

        /// <summary>
        /// Receiver id of the source that send the error frame.
        /// </summary>
        /// <returns> receiver id of the source that send the error frame. </returns>
        public long ReceiverId()
        {
            return receiverId;
        }

        /// <summary>
        /// Group tag of the source that sent the error frame.
        /// </summary>
        /// <returns> group tag of the source that sent the error frame or <seealso cref="Aeron.NULL_VALUE"/> if the source did not have a group
        /// tag set. </returns>
        public long GroupTag()
        {
            return groupTag;
        }

        /// <summary>
        /// The error code of the error frame received.
        /// </summary>
        /// <returns> the error code. </returns>
        public ErrorCode ErrorCode()
        {
            return errorCode;
        }

        /// <summary>
        /// The error message of the error frame received.
        /// </summary>
        /// <returns> the error message. </returns>
        public string ErrorMessage()
        {
            return errorMessage;
        }

        /// <summary>
        /// The address of the remote source that sent the error frame.
        /// </summary>
        /// <returns> address of the remote source. </returns>
        public IPEndPoint SourceAddress()
        {
            return sourceAddress;
        }

        /// <summary>
        /// The registrationId of the destination. Only used with manual MDC publications. Will be
        /// <seealso cref="Aeron.NULL_VALUE"/> otherwise.
        /// </summary>
        /// <returns> registrationId of the destination or <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public long DestinationRegistrationId()
        {
            return destinationRegistrationId;
        }

        /// <summary>
        /// Set the fields of the publication error frame from the flyweight.
        /// </summary>
        /// <param name="frameFlyweight"> that was received from the client message buffer. </param>
        /// <returns> this for fluent API. </returns>
        public PublicationErrorFrame Set(PublicationErrorFrameFlyweight frameFlyweight)
        {
            registrationId = frameFlyweight.RegistrationId();
            sessionId = frameFlyweight.SessionId();
            streamId = frameFlyweight.StreamId();
            receiverId = frameFlyweight.ReceiverId();
            groupTag = frameFlyweight.GroupTag();
            sourceAddress = frameFlyweight.SourceAddress();
            errorCode = frameFlyweight.ErrorCode();
            errorMessage = frameFlyweight.ErrorMessage();
            destinationRegistrationId = frameFlyweight.DestinationRegistrationId();

            return this;
        }

        /// <summary>
        /// Return a copy of this message. Useful if a callback is reusing an instance of this class to avoid unnecessary
        /// allocation.
        /// </summary>
        /// <returns> a copy of this instance's data. </returns>
        public object Clone()
        {
            return MemberwiseClone();
        }
    }
}