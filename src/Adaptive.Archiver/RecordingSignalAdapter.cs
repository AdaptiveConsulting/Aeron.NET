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
    /// Encapsulate the polling, decoding, and dispatching of recording transition events for a session plus the
    /// asynchronous events to check for errors.
    /// <para>
    /// Important: set the underlying <seealso cref="IRecordingSignalConsumer"/> instance on the
    /// <seealso cref="AeronArchive"/> using the
    /// <seealso cref="AeronArchive.Context.RecordingSignalConsumer(IRecordingSignalConsumer)"/> method to avoid missing
    /// signals.
    ///
    /// </para>
    /// </summary>
    /// <seealso cref="RecordingSignal"/>
    public class RecordingSignalAdapter : IControlledFragmentHandler
    {
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder _controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingSignalEventDecoder _recordingSignalEventDecoder = new RecordingSignalEventDecoder();
        private ControlledFragmentAssembler _assembler;
        private readonly IControlEventListener _controlEventListener;
        private readonly IRecordingSignalConsumer _recordingSignalConsumer;
        private readonly Subscription _subscription;
        private readonly int _fragmentLimit;
        private readonly long _controlSessionId;
        private bool _isDone = false;

        /// <summary>
        /// Create an adapter for a given subscription to an archive for recording events.
        /// </summary>
        /// <param name="controlSessionId"> to listen for associated asynchronous control events, such as errors.
        /// </param>
        /// <param name="controlEventListener"> listener for control events which may indicate an error on the session.
        /// </param>
        /// <param name="recordingSignalConsumer"> consumer of recording transition events. </param>
        /// <param name="subscription">                to poll for new events. </param>
        /// <param name="fragmentLimit">               to apply for each polling operation. </param>
        public RecordingSignalAdapter(
            long controlSessionId,
            IControlEventListener controlEventListener,
            IRecordingSignalConsumer recordingSignalConsumer,
            Subscription subscription,
            int fragmentLimit
        )
        {
            _assembler = new ControlledFragmentAssembler(this);

            this._controlSessionId = controlSessionId;
            this._controlEventListener = controlEventListener;
            this._recordingSignalConsumer = recordingSignalConsumer;
            this._subscription = subscription;
            this._fragmentLimit = fragmentLimit;
        }

        /// <summary>
        /// Poll for recording transitions and dispatch them to the <seealso cref="IRecordingSignalConsumer"/> for this
        /// instance, plus check for async responses for this control session which may have an exception and dispatch
        /// to the
        /// <seealso cref="IControlResponseListener"/>.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            if (_isDone)
            {
                _isDone = false;
            }

            return _subscription.ControlledPoll(_assembler, _fragmentLimit);
        }

        /// <summary>
        /// Indicate that poll was successful and a signal or control response was received.
        /// </summary>
        /// <returns> true if a signal or control response was received. </returns>
        public bool Done
        {
            get { return _isDone; }
        }

        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (_isDone)
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

            switch (_messageHeaderDecoder.TemplateId())
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    _controlResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    if (_controlResponseDecoder.ControlSessionId() == _controlSessionId)
                    {
                        _controlEventListener.OnResponse(
                            _controlSessionId,
                            _controlResponseDecoder.CorrelationId(),
                            _controlResponseDecoder.RelevantId(),
                            _controlResponseDecoder.Code(),
                            _controlResponseDecoder.ErrorMessage()
                        );

                        _isDone = true;
                        return BREAK;
                    }

                    break;

                case RecordingSignalEventDecoder.TEMPLATE_ID:
                    _recordingSignalEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    if (_recordingSignalEventDecoder.ControlSessionId() == _controlSessionId)
                    {
                        _recordingSignalConsumer.OnSignal(
                            _recordingSignalEventDecoder.ControlSessionId(),
                            _recordingSignalEventDecoder.CorrelationId(),
                            _recordingSignalEventDecoder.RecordingId(),
                            _recordingSignalEventDecoder.SubscriptionId(),
                            _recordingSignalEventDecoder.Position(),
                            _recordingSignalEventDecoder.Signal()
                        );

                        _isDone = true;
                        return BREAK;
                    }

                    break;
            }

            return CONTINUE;
        }
    }
}
