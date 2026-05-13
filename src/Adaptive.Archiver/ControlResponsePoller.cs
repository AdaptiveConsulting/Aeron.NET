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

using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.LogBuffer.ControlledFragmentHandlerAction;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling and decoding of archive control protocol response messages.
    /// </summary>
    public class ControlResponsePoller : IControlledFragmentHandler
    {
        /// <summary>
        /// Limit to apply when polling response messages.
        /// </summary>
        public const int FRAGMENT_LIMIT = 10;

        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder _controlResponseDecoder = new ControlResponseDecoder();
        private readonly ChallengeDecoder _challengeDecoder = new ChallengeDecoder();
        private readonly RecordingSignalEventDecoder _recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly Subscription _subscription;
        private ControlledFragmentAssembler _fragmentAssembler;
        private readonly int _fragmentLimit;

        private long _controlSessionId = Aeron.Aeron.NULL_VALUE;
        private long _correlationId = Aeron.Aeron.NULL_VALUE;
        private long _relevantId = Aeron.Aeron.NULL_VALUE;
        private int _templateId = Aeron.Aeron.NULL_VALUE;
        private int _version = 0;
        private ControlResponseCode _code = ControlResponseCode.NULL_VALUE;
        private string _errorMessage = null;
        private long _recordingId = Aeron.Aeron.NULL_VALUE;
        private long _subscriptionId = Aeron.Aeron.NULL_VALUE;
        private long _position = Aeron.Aeron.NULL_VALUE;
        private RecordingSignal _recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
        private byte[] _encodedChallenge = null;
        private bool _isPollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages with a default fragment
        /// limit for polling as <seealso cref="FRAGMENT_LIMIT"/> .
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        public ControlResponsePoller(Subscription subscription)
            : this(subscription, FRAGMENT_LIMIT)
        {
        }

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply when polling. </param>
        public ControlResponsePoller(Subscription subscription, int fragmentLimit)
        {
            this._fragmentAssembler = new ControlledFragmentAssembler(this);

            this._subscription = Objects.RequireNonNull(subscription);
            this._fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling responses.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling responses. </returns>
        public Subscription Subscription()
        {
            return _subscription;
        }

        /// <summary>
        /// Poll for control response events.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            if (_isPollComplete)
            {
                _controlSessionId = Aeron.Aeron.NULL_VALUE;
                _correlationId = Aeron.Aeron.NULL_VALUE;
                _relevantId = Aeron.Aeron.NULL_VALUE;
                _templateId = Aeron.Aeron.NULL_VALUE;
                _version = 0;
                _code = ControlResponseCode.NULL_VALUE;
                _errorMessage = null;
                _recordingId = Aeron.Aeron.NULL_VALUE;
                _subscriptionId = Aeron.Aeron.NULL_VALUE;
                _position = Aeron.Aeron.NULL_VALUE;
                _encodedChallenge = null;
                _recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
                _isPollComplete = false;
            }

            return _subscription.ControlledPoll(_fragmentAssembler, _fragmentLimit);
        }

        /// <summary>
        /// SBE template id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned
        /// nothing.
        /// </summary>
        /// <returns> SBE template id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing. </returns>
        public int TemplateId()
        {
            return _templateId;
        }

        /// <summary>
        /// Control session id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned
        /// nothing.
        /// </summary>
        /// <returns> control session id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing. </returns>
        public long ControlSessionId()
        {
            return _controlSessionId;
        }

        /// <summary>
        /// Correlation id of the message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> correlation id of polled message or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing. </returns>
        public long CorrelationId()
        {
            return _correlationId;
        }

        /// <summary>
        /// Get the relevant id returned with the response, e.g. replay session id.
        /// </summary>
        /// <returns> the relevant id returned with the response. </returns>
        public long RelevantId()
        {
            return _relevantId;
        }

        /// <summary>
        /// Recording id of polled <seealso cref="RecordingSignal"/> or
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> recording id of polled <seealso cref="RecordingSignal"/> or
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long RecordingId()
        {
            return _recordingId;
        }

        /// <summary>
        /// Subscription id of polled <seealso cref="RecordingSignal"/> or
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing.
        /// </summary>
        /// <returns> subscription id of polled <seealso cref="RecordingSignal"/> or
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long SubscriptionId()
        {
            return _subscriptionId;
        }

        /// <summary>
        /// Position of polled <seealso cref="RecordingSignal"/> or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if
        /// poll returned nothing.
        /// </summary>
        /// <returns> position of polled <seealso cref="RecordingSignal"/> or
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if poll returned nothing. </returns>
        public long Position()
        {
            return _position;
        }

        /// <summary>
        /// Enum of polled <seealso cref="RecordingSignal"/> or null if poll returned nothing.
        /// </summary>
        /// <returns> enum of polled <seealso cref="RecordingSignal"/> or null if poll returned nothing. </returns>
        public RecordingSignal RecordingSignal()
        {
            return _recordingSignal;
        }

        /// <summary>
        /// Version response from the server in semantic version form.
        /// </summary>
        /// <returns> response from the server in semantic version form. </returns>
        public int Version()
        {
            return _version;
        }

        /// <summary>
        /// Has the last polling action received a complete message?
        /// </summary>
        /// <returns> true if the last polling action received a complete message? </returns>
        public bool PollComplete
        {
            get { return _isPollComplete; }
        }

        /// <summary>
        /// Get the response code of the last response.
        /// </summary>
        /// <returns> the response code of the last response. </returns>
        public ControlResponseCode Code()
        {
            return _code;
        }

        /// <summary>
        /// Get the error message of the response.
        /// </summary>
        /// <returns> the error message of the response. </returns>
        public string ErrorMessage()
        {
            return _errorMessage;
        }

        /// <summary>
        /// Was the last polling action received a challenge message?
        /// </summary>
        /// <returns> true if the last polling action received was a challenge message, false if not. </returns>
        public bool WasChallenged()
        {
            return null != _encodedChallenge;
        }

        /// <summary>
        /// Get the encoded challenge of the last challenge.
        /// </summary>
        /// <returns> the encoded challenge of the last challenge. </returns>
        public byte[] EncodedChallenge()
        {
            return _encodedChallenge;
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (_isPollComplete)
            {
                return ABORT;
            }

            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId
                );
            }

            _templateId = _messageHeaderDecoder.TemplateId();
            switch (_templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                {
                    _controlResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _controlSessionId = _controlResponseDecoder.ControlSessionId();
                    _correlationId = _controlResponseDecoder.CorrelationId();
                    _relevantId = _controlResponseDecoder.RelevantId();
                    _code = _controlResponseDecoder.Code();
                    _version = _controlResponseDecoder.Version();
                    _errorMessage = _controlResponseDecoder.ErrorMessage();
                    _isPollComplete = true;

                    return BREAK;
                }

                case ChallengeDecoder.TEMPLATE_ID:
                {
                    _challengeDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _controlSessionId = _challengeDecoder.ControlSessionId();
                    _correlationId = _challengeDecoder.CorrelationId();
                    _relevantId = Aeron.Aeron.NULL_VALUE;
                    _code = ControlResponseCode.NULL_VALUE;
                    _version = _challengeDecoder.Version();
                    _errorMessage = "";

                    int encodedChallengeLength = _challengeDecoder.EncodedChallengeLength();
                    _encodedChallenge = new byte[encodedChallengeLength];
                    _challengeDecoder.GetEncodedChallenge(_encodedChallenge, 0, encodedChallengeLength);

                    _isPollComplete = true;

                    return BREAK;
                }

                case RecordingSignalEventDecoder.TEMPLATE_ID:
                {
                    _recordingSignalEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _controlSessionId = _recordingSignalEventDecoder.ControlSessionId();
                    _correlationId = _recordingSignalEventDecoder.CorrelationId();
                    _recordingId = _recordingSignalEventDecoder.RecordingId();
                    _subscriptionId = _recordingSignalEventDecoder.SubscriptionId();
                    _position = _recordingSignalEventDecoder.Position();
                    _recordingSignal = _recordingSignalEventDecoder.Signal();

                    _isPollComplete = true;

                    return BREAK;
                }

                default:
                    return CONTINUE;
            }
        }

        public override string ToString()
        {
            return
                "ControlResponsePoller{" +
                "templateId=" + _templateId +
                ", controlSessionId=" + _controlSessionId +
                ", correlationId=" + _correlationId +
                ", relevantId=" + _relevantId +
                ", recordingId=" + _recordingId +
                ", subscriptionId=" + _subscriptionId +
                ", position=" + _position +
                ", recordingSignal=" + _recordingSignal +
                ", code=" + _code +
                ", version=" + SemanticVersion.ToString(_version) +
                ", errorMessage='" + _errorMessage + '\'' +
                ", isPollComplete=" + _isPollComplete +
                '}';
        }
    }
}
