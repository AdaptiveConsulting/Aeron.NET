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
    /// Encapsulate the polling, decoding, and dispatching of archive control protocol response messages.
    /// </summary>
    public class ControlResponseAdapter
    {
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder _controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder _recordingDescriptorDecoder = new RecordingDescriptorDecoder();
        private readonly RecordingSignalEventDecoder _recordingSignalEventDecoder = new RecordingSignalEventDecoder();

        private readonly int _fragmentLimit;
        private readonly IControlResponseListener _controlResponseListener;
        private readonly IRecordingSignalConsumer _recordingSignalConsumer;
        private readonly Subscription _subscription;
        private readonly FragmentAssembler _fragmentAssembler;

        /// <summary>
        /// Create an adapter for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="controlResponseListener">      to which responses are dispatched. </param>
        /// <param name="subscription">  to poll for new events. </param>
        /// <param name="fragmentLimit"> to apply for each polling operation. </param>
        public ControlResponseAdapter(
            IControlResponseListener controlResponseListener,
            Subscription subscription,
            int fragmentLimit
        )
            : this(
                controlResponseListener,
                AeronArchive.Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER,
                subscription,
                fragmentLimit
            ) { }

        /// <summary>
        /// Create an adapter for a given subscription to an archive for control response messages.
        /// </summary>
        /// <param name="controlResponseListener"> for dispatching responses. </param>
        /// <param name="recordingSignalConsumer"> for dispatching recording signals. </param>
        /// <param name="subscription">            to poll for responses. </param>
        /// <param name="fragmentLimit">           to apply for each polling operation. </param>
        public ControlResponseAdapter(
            IControlResponseListener controlResponseListener,
            IRecordingSignalConsumer recordingSignalConsumer,
            Subscription subscription,
            int fragmentLimit
        )
        {
            _fragmentAssembler = new FragmentAssembler(OnFragment);

            this._fragmentLimit = fragmentLimit;
            this._controlResponseListener = controlResponseListener;
            this._recordingSignalConsumer = recordingSignalConsumer;
            this._subscription = subscription;
        }

        /// <summary>
        /// Poll for recording events and dispatch them to the <seealso cref="IControlResponseListener"/> for this
        /// instance.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            return _subscription.Poll(_fragmentAssembler, _fragmentLimit);
        }

        /// <summary>
        /// Dispatch a descriptor message to a consumer by reading the fields in the correct order.
        /// </summary>
        /// <param name="decoder">  which wraps the encoded message ready for reading. </param>
        /// <param name="consumer"> to which the decoded fields should be passed. </param>
        public static void DispatchDescriptor(RecordingDescriptorDecoder decoder, IRecordingDescriptorConsumer consumer)
        {
            consumer.OnRecordingDescriptor(
                decoder.ControlSessionId(),
                decoder.CorrelationId(),
                decoder.RecordingId(),
                decoder.StartTimestamp(),
                decoder.StopTimestamp(),
                decoder.StartPosition(),
                decoder.StopPosition(),
                decoder.InitialTermId(),
                decoder.SegmentFileLength(),
                decoder.TermBufferLength(),
                decoder.MtuLength(),
                decoder.SessionId(),
                decoder.StreamId(),
                decoder.StrippedChannel(),
                decoder.OriginalChannel(),
                decoder.SourceIdentity()
            );
        }

        private void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
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
                    HandleControlResponse(_controlResponseListener, buffer, offset);
                    break;

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    HandleRecordingDescriptor(_controlResponseListener, buffer, offset);
                    break;

                case RecordingSignalEventDecoder.TEMPLATE_ID:
                    HandleRecordingSignal(_recordingSignalConsumer, buffer, offset);
                    break;
            }
        }

        private void HandleControlResponse(IControlResponseListener listener, IDirectBuffer buffer, int offset)
        {
            _controlResponseDecoder.Wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                _messageHeaderDecoder.BlockLength(),
                _messageHeaderDecoder.Version()
            );

            listener.OnResponse(
                _controlResponseDecoder.ControlSessionId(),
                _controlResponseDecoder.CorrelationId(),
                _controlResponseDecoder.RelevantId(),
                _controlResponseDecoder.Code(),
                _controlResponseDecoder.ErrorMessage()
            );
        }

        private void HandleRecordingDescriptor(IControlResponseListener listener, IDirectBuffer buffer, int offset)
        {
            _recordingDescriptorDecoder.Wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                _messageHeaderDecoder.BlockLength(),
                _messageHeaderDecoder.Version()
            );

            DispatchDescriptor(_recordingDescriptorDecoder, listener);
        }

        private void HandleRecordingSignal(
            IRecordingSignalConsumer recordingSignalConsumer,
            IDirectBuffer buffer,
            int offset
        )
        {
            _recordingSignalEventDecoder.Wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                _messageHeaderDecoder.BlockLength(),
                _messageHeaderDecoder.Version()
            );

            recordingSignalConsumer.OnSignal(
                _recordingSignalEventDecoder.ControlSessionId(),
                _recordingSignalEventDecoder.CorrelationId(),
                _recordingSignalEventDecoder.RecordingId(),
                _recordingSignalEventDecoder.SubscriptionId(),
                _recordingSignalEventDecoder.Position(),
                _recordingSignalEventDecoder.Signal()
            );
        }
    }
}
