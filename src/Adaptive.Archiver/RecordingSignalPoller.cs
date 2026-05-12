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

using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;
using ControlledFragmentAssembler = Adaptive.Aeron.ControlledFragmentAssembler;
using Subscription = Adaptive.Aeron.Subscription;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling and decoding of archive control protocol response and recording signal messages.
    /// </summary>
    public sealed class RecordingSignalPoller : IControlledFragmentHandler
    {
        /// <summary>
        /// Limit to apply when polling messages.
        /// </summary>
        public const int FRAGMENT_LIMIT = 10;

        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder _controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingSignalEventDecoder _recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly Subscription _subscription;
        private ControlledFragmentAssembler _fragmentAssembler;
        private readonly long _controlSessionId;
        private long _correlationId = Aeron.Aeron.NULL_VALUE;
        private long _relevantId = Aeron.Aeron.NULL_VALUE;
        private int _templateId = Aeron.Aeron.NULL_VALUE;
        private int _version = 0;
        private long _recordingId = Aeron.Aeron.NULL_VALUE;
        private long _recordingSubscriptionId = Aeron.Aeron.NULL_VALUE;
        private long _recordingPosition = Aeron.Aeron.NULL_VALUE;
        private RecordingSignal _recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
        private ControlResponseCode _code;
        private string _errorMessage;
        private readonly int _fragmentLimit;
        private bool _isPollComplete = false;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control messages.
        /// </summary>
        /// <param name="controlSessionId"> to listen for associated asynchronous control events, such as errors.
        /// </param>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="fragmentLimit">    to apply when polling. </param>
        private RecordingSignalPoller(long controlSessionId, Subscription subscription, int fragmentLimit)
        {
            _fragmentAssembler = new ControlledFragmentAssembler(this);

            this._controlSessionId = controlSessionId;
            this._subscription = subscription;
            this._fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages with a default fragment
        /// limit for polling as <seealso cref="FRAGMENT_LIMIT"/> .
        /// </summary>
        /// <param name="controlSessionId"> to listen for associated asynchronous control events, such as errors.
        /// </param>
        /// <param name="subscription"> to poll for new events. </param>
        public RecordingSignalPoller(long controlSessionId, Subscription subscription)
            : this(controlSessionId, subscription, FRAGMENT_LIMIT) { }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used for polling messages.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used for polling messages. </returns>
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
                _isPollComplete = false;
                _templateId = Aeron.Aeron.NULL_VALUE;
                _correlationId = Aeron.Aeron.NULL_VALUE;
                _relevantId = Aeron.Aeron.NULL_VALUE;
                _version = 0;
                _errorMessage = null;
                _recordingId = Aeron.Aeron.NULL_VALUE;
                _recordingSubscriptionId = Aeron.Aeron.NULL_VALUE;
                _recordingPosition = Aeron.Aeron.NULL_VALUE;
                _recordingSignal = Codecs.RecordingSignal.NULL_VALUE;
            }

            return _subscription.ControlledPoll(_fragmentAssembler, _fragmentLimit);
        }

        /// <summary>
        /// Control session id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll returned
        /// nothing.
        /// </summary>
        /// <returns> control session id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll
        /// returned nothing. </returns>
        public long ControlSessionId()
        {
            return _controlSessionId;
        }

        /// <summary>
        /// Correlation id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll returned
        /// nothing.
        /// </summary>
        /// <returns> correlation id of the last polled message or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if poll
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
        /// Get the template id of the last received message.
        /// </summary>
        /// <returns> the template id of the last received message. </returns>
        public int TemplateId()
        {
            return _templateId;
        }

        /// <summary>
        /// Get the recording id of the last received message.
        /// </summary>
        /// <returns> the recording id of the last received message. </returns>
        public long RecordingId()
        {
            return _recordingId;
        }

        /// <summary>
        /// Get the recording subscription id of the last received message.
        /// </summary>
        /// <returns> the recording subscription id of the last received message. </returns>
        public long RecordingSubscriptionId()
        {
            return _recordingSubscriptionId;
        }

        /// <summary>
        /// Get the recording position of the last received message.
        /// </summary>
        /// <returns> the recording position of the last received message. </returns>
        public long RecordingPosition()
        {
            return _recordingPosition;
        }

        /// <summary>
        /// Get the recording signal of the last received message.
        /// </summary>
        /// <returns> the recording signal of the last received message. </returns>
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
        /// Get the error message of the last response.
        /// </summary>
        /// <returns> the error message of the last response. </returns>
        public string ErrorMessage()
        {
            return _errorMessage;
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (_isPollComplete)
            {
                return ControlledFragmentHandlerAction.ABORT;
            }

            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId
                );
            }

            int templateId = _messageHeaderDecoder.TemplateId();

            if (ControlResponseDecoder.TEMPLATE_ID == templateId)
            {
                _controlResponseDecoder.Wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    _messageHeaderDecoder.BlockLength(),
                    _messageHeaderDecoder.Version()
                );

                if (_controlResponseDecoder.ControlSessionId() == _controlSessionId)
                {
                    this._templateId = templateId;
                    _correlationId = _controlResponseDecoder.CorrelationId();
                    _relevantId = _controlResponseDecoder.RelevantId();
                    _code = _controlResponseDecoder.Code();
                    _version = _controlResponseDecoder.Version();
                    _errorMessage = _controlResponseDecoder.ErrorMessage();
                    _isPollComplete = true;

                    return ControlledFragmentHandlerAction.BREAK;
                }
            }
            else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId)
            {
                _recordingSignalEventDecoder.Wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    _messageHeaderDecoder.BlockLength(),
                    _messageHeaderDecoder.Version()
                );

                if (_recordingSignalEventDecoder.ControlSessionId() == _controlSessionId)
                {
                    this._templateId = templateId;
                    _correlationId = _recordingSignalEventDecoder.CorrelationId();
                    _recordingId = _recordingSignalEventDecoder.RecordingId();
                    _recordingSubscriptionId = _recordingSignalEventDecoder.SubscriptionId();
                    _recordingPosition = _recordingSignalEventDecoder.Position();
                    _recordingSignal = _recordingSignalEventDecoder.Signal();
                    _isPollComplete = true;

                    return ControlledFragmentHandlerAction.BREAK;
                }
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "RecordingSignalPoller{"
                + "controlSessionId="
                + _controlSessionId
                + ", correlationId="
                + _correlationId
                + ", relevantId="
                + _relevantId
                + ", code="
                + _code
                + ", templateId="
                + _templateId
                + ", version="
                + SemanticVersion.ToString(_version)
                + ", errorMessage='"
                + _errorMessage
                + '\''
                + ", recordingId="
                + _recordingId
                + ", recordingSubscriptionId="
                + _recordingSubscriptionId
                + ", recordingPosition="
                + _recordingPosition
                + ", recordingSignal="
                + _recordingSignal
                + ", isPollComplete="
                + _isPollComplete
                + '}';
        }
    }
}
