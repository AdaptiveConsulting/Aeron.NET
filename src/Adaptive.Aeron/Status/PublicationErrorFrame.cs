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
using Adaptive.Aeron.Command;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Encapsulates the data received when a publication receives an error frame.
    /// </summary>
    public class PublicationErrorFrame : ICloneable
    {
        private long _registrationId;
        private int _sessionId;
        private int _streamId;
        private long _receiverId;
        private long _destinationRegistrationId;
        private long _groupTag;
        private ErrorCode _errorCode;
        private string _errorMessage;
        private IPEndPoint _sourceAddress;

        /// <summary>
        /// Registration id of the publication that received the error frame.
        /// </summary>
        /// <returns> registration id of the publication. </returns>
        public long RegistrationId()
        {
            return _registrationId;
        }

        /// <summary>
        /// Session id of the publication that received the error frame.
        /// </summary>
        /// <returns> session id of the publication. </returns>
        public int SessionId()
        {
            return _sessionId;
        }

        /// <summary>
        /// Stream id of the publication that received the error frame.
        /// </summary>
        /// <returns> stream id of the publication. </returns>
        public int StreamId()
        {
            return _streamId;
        }

        /// <summary>
        /// Receiver id of the source that send the error frame.
        /// </summary>
        /// <returns> receiver id of the source that send the error frame. </returns>
        public long ReceiverId()
        {
            return _receiverId;
        }

        /// <summary>
        /// Group tag of the source that sent the error frame.
        /// </summary>
        /// <returns> group tag of the source that sent the error frame or <seealso cref="Aeron.NULL_VALUE"/> if the
        /// source did not have a group tag set. </returns>
        public long GroupTag()
        {
            return _groupTag;
        }

        /// <summary>
        /// The error code of the error frame received.
        /// </summary>
        /// <returns> the error code. </returns>
        public ErrorCode ErrorCode()
        {
            return _errorCode;
        }

        /// <summary>
        /// The error message of the error frame received.
        /// </summary>
        /// <returns> the error message. </returns>
        public string ErrorMessage()
        {
            return _errorMessage;
        }

        /// <summary>
        /// The address of the remote source that sent the error frame.
        /// </summary>
        /// <returns> address of the remote source. </returns>
        public IPEndPoint SourceAddress()
        {
            return _sourceAddress;
        }

        /// <summary>
        /// The registrationId of the destination. Only used with manual MDC publications. Will be
        /// <seealso cref="Aeron.NULL_VALUE"/> otherwise.
        /// </summary>
        /// <returns> registrationId of the destination or <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public long DestinationRegistrationId()
        {
            return _destinationRegistrationId;
        }

        /// <summary>
        /// Set the fields of the publication error frame from the flyweight.
        /// </summary>
        /// <param name="frameFlyweight"> that was received from the client message buffer. </param>
        /// <returns> this for fluent API. </returns>
        public PublicationErrorFrame Set(PublicationErrorFrameFlyweight frameFlyweight)
        {
            _registrationId = frameFlyweight.RegistrationId();
            _sessionId = frameFlyweight.SessionId();
            _streamId = frameFlyweight.StreamId();
            _receiverId = frameFlyweight.ReceiverId();
            _groupTag = frameFlyweight.GroupTag();
            _sourceAddress = frameFlyweight.SourceAddress();
            _errorCode = frameFlyweight.ErrorCode();
            _errorMessage = frameFlyweight.ErrorMessage();
            _destinationRegistrationId = frameFlyweight.DestinationRegistrationId();

            return this;
        }

        /// <summary>
        /// Return a copy of this message. Useful if a callback is reusing an instance of this class to avoid
        /// unnecessary allocation.
        /// </summary>
        /// <returns> a copy of this instance's data. </returns>
        public object Clone()
        {
            return MemberwiseClone();
        }

        /// <summary>
        /// Build a String representation of the error frame.
        /// </summary>
        /// <returns> a String representation of the error frame. </returns>
        public override string ToString()
        {
            return
                "PublicationErrorFrame{" +
                "registrationId=" + _registrationId +
                ", sessionId=" + _sessionId +
                ", streamId=" + _streamId +
                ", receiverId=" + _receiverId +
                ", destinationRegistrationId=" + _destinationRegistrationId +
                ", groupTag=" + _groupTag +
                ", errorCode=" + _errorCode +
                ", errorMessage=" + _errorMessage +
                ", sourceAddress=" + _sourceAddress +
                "}";
        }
    }
}
