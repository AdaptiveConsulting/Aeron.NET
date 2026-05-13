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

namespace Adaptive.Archiver
{
    /// <summary>
    /// Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
    /// </summary>
    public class RecordingDescriptorPoller : IControlledFragmentHandler
    {
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder _controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder _recordingDescriptorDecoder = new RecordingDescriptorDecoder();
        private readonly RecordingSignalEventDecoder _recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly long _controlSessionId;
        private readonly int _fragmentLimit;
        private readonly Subscription _subscription;
        private readonly ControlledFragmentAssembler _fragmentAssembler;
        private readonly IErrorHandler _errorHandler;
        private readonly IRecordingSignalConsumer _recordingSignalConsumer;

        private long _correlationId;
        private int _remainingRecordCount;
        private bool _isDispatchComplete = false;
        private IRecordingDescriptorConsumer _recordingDescriptorConsumer;

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="errorHandler">     to call for asynchronous errors. </param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        public RecordingDescriptorPoller(
            Subscription subscription,
            IErrorHandler errorHandler,
            long controlSessionId,
            int fragmentLimit
        )
            : this(
                subscription,
                errorHandler,
                AeronArchive.Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER,
                controlSessionId,
                fragmentLimit
            )
        {
        }

        /// <summary>
        /// Create a poller for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="subscription">     to poll for new events. </param>
        /// <param name="errorHandler">     to call for asynchronous errors.</param>
        /// <param name="recordingSignalConsumer"> for consuming interleaved recording signals on the control
        /// session.</param>
        /// <param name="controlSessionId"> to filter the responses. </param>
        /// <param name="fragmentLimit">    to apply for each polling operation. </param>
        public RecordingDescriptorPoller(
            Subscription subscription,
            IErrorHandler errorHandler,
            IRecordingSignalConsumer recordingSignalConsumer,
            long controlSessionId,
            int fragmentLimit
        )
        {
            this._subscription = subscription;
            this._errorHandler = errorHandler;
            this._recordingSignalConsumer = recordingSignalConsumer;
            this._fragmentLimit = fragmentLimit;
            this._controlSessionId = controlSessionId;

            this._fragmentAssembler = new ControlledFragmentAssembler(this);
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
        /// Poll for recording events.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            if (_isDispatchComplete)
            {
                _isDispatchComplete = false;
            }

            return _subscription.ControlledPoll(_fragmentAssembler, _fragmentLimit);
        }

        /// <summary>
        /// Control session id for filtering responses.
        /// </summary>
        /// <returns> control session id for filtering responses. </returns>
        public long ControlSessionId()
        {
            return _controlSessionId;
        }

        /// <summary>
        /// Is the dispatch of descriptors complete?
        /// </summary>
        /// <returns> true if the dispatch of descriptors complete? </returns>
        public bool IsDispatchComplete()
        {
            return _isDispatchComplete;
        }

        /// <summary>
        /// Get the number of remaining records are expected.
        /// </summary>
        /// <returns> the number of remaining records are expected. </returns>
        public int RemainingRecordCount()
        {
            return _remainingRecordCount;
        }

        /// <summary>
        /// Reset the poller to dispatch the descriptors returned from a query.
        /// </summary>
        /// <param name="correlationId"> for the response. </param>
        /// <param name="recordCount">           of descriptors to expect. </param>
        /// <param name="consumer">              to which the recording descriptors are to be dispatched. </param>
        public void Reset(long correlationId, int recordCount, IRecordingDescriptorConsumer consumer)
        {
            this._correlationId = correlationId;
            this._recordingDescriptorConsumer = consumer;
            this._remainingRecordCount = recordCount;
            _isDispatchComplete = false;
        }

        // Upstream: io.aeron.archive.client.RecordingDescriptorPoller#onFragment
        // is @SuppressWarnings("MethodLength").
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Major Code Smell",
            "S138:Functions should not have too many lines",
            Justification = "Upstream Java parity; method is itself @SuppressWarnings(\"MethodLength\")."
        )]
        public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (_isDispatchComplete)
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
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                {
                    _controlResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    if (_controlResponseDecoder.ControlSessionId() == _controlSessionId)
                    {
                        ControlResponseCode code = _controlResponseDecoder.Code();
                        long responseCorrelationId = _controlResponseDecoder.CorrelationId();

                        if (
                            ControlResponseCode.RECORDING_UNKNOWN == code
                            && responseCorrelationId == this._correlationId
                        )
                        {
                            _isDispatchComplete = true;
                            return ControlledFragmentHandlerAction.BREAK;
                        }

                        if (ControlResponseCode.ERROR == code)
                        {
                            ArchiveException ex = new ArchiveException(
                                "response for correlationId="
                                    + this._correlationId
                                    + ", error: "
                                    + _controlResponseDecoder.ErrorMessage(),
                                (int)_controlResponseDecoder.RelevantId(),
                                responseCorrelationId
                            );

                            if (responseCorrelationId == this._correlationId)
                            {
                                throw ex;
                            }
                            else
                            {
                                _errorHandler?.OnError(ex);
                            }
                        }
                    }

                    break;
                }

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                {
                    _recordingDescriptorDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    if (
                        _recordingDescriptorDecoder.ControlSessionId() == _controlSessionId
                        && _recordingDescriptorDecoder.CorrelationId() == _correlationId
                    )
                    {
                        _recordingDescriptorConsumer.OnRecordingDescriptor(
                            _controlSessionId,
                            _recordingDescriptorDecoder.CorrelationId(),
                            _recordingDescriptorDecoder.RecordingId(),
                            _recordingDescriptorDecoder.StartTimestamp(),
                            _recordingDescriptorDecoder.StopTimestamp(),
                            _recordingDescriptorDecoder.StartPosition(),
                            _recordingDescriptorDecoder.StopPosition(),
                            _recordingDescriptorDecoder.InitialTermId(),
                            _recordingDescriptorDecoder.SegmentFileLength(),
                            _recordingDescriptorDecoder.TermBufferLength(),
                            _recordingDescriptorDecoder.MtuLength(),
                            _recordingDescriptorDecoder.SessionId(),
                            _recordingDescriptorDecoder.StreamId(),
                            _recordingDescriptorDecoder.StrippedChannel(),
                            _recordingDescriptorDecoder.OriginalChannel(),
                            _recordingDescriptorDecoder.SourceIdentity()
                        );

                        if (0 == --_remainingRecordCount)
                        {
                            _isDispatchComplete = true;
                            return ControlledFragmentHandlerAction.BREAK;
                        }
                    }

                    break;
                }

                case RecordingSignalEventDecoder.TEMPLATE_ID:
                    _recordingSignalEventDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    if (_controlSessionId == _recordingSignalEventDecoder.ControlSessionId())
                    {
                        _recordingSignalConsumer.OnSignal(
                            _recordingSignalEventDecoder.ControlSessionId(),
                            _recordingSignalEventDecoder.CorrelationId(),
                            _recordingSignalEventDecoder.RecordingId(),
                            _recordingSignalEventDecoder.SubscriptionId(),
                            _recordingSignalEventDecoder.Position(),
                            _recordingSignalEventDecoder.Signal()
                        );
                    }
                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return
                "RecordingDescriptorPoller{" +
                "controlSessionId=" + _controlSessionId +
                ", correlationId=" + _correlationId +
                ", remainingRecordCount=" + _remainingRecordCount +
                ", isDispatchComplete=" + _isDispatchComplete +
                '}';
        }
    }
}
