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
    /// Encapsulate the polling and decoding of recording events.
    /// </summary>
    public class RecordingEventsPoller : IControlledFragmentHandler
    {
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly RecordingStartedDecoder _recordingStartedDecoder = new RecordingStartedDecoder();
        private readonly RecordingProgressDecoder _recordingProgressDecoder = new RecordingProgressDecoder();
        private readonly RecordingStoppedDecoder _recordingStoppedDecoder = new RecordingStoppedDecoder();

        private readonly Subscription _subscription;
        private int _templateId;
        private bool _isPollComplete;

        private long _recordingId;
        private long _recordingStartPosition;
        private long _recordingPosition;
        private long _recordingStopPosition;

        /// <summary>
        /// Create a poller for a given subscription to an archive for recording events.
        /// </summary>
        /// <param name="subscription"> to poll for new events. </param>
        public RecordingEventsPoller(Subscription subscription)
        {
            this._subscription = subscription;
        }

        /// <summary>
        /// Poll for recording events.
        /// </summary>
        /// <returns> the number of fragments read during the operation. Zero if no events are available. </returns>
        public int Poll()
        {
            if (_isPollComplete)
            {
                _isPollComplete = false;
                _templateId = Aeron.Aeron.NULL_VALUE;
            }

            return _subscription.ControlledPoll(this, 1);
        }

        /// <summary>
        /// Has the last polling action received a complete message?
        /// </summary>
        /// <returns> true of the last polling action received a complete message? </returns>
        public bool IsPollComplete()
        {
            return _isPollComplete;
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
        /// Get the recording id of the last received event.
        /// </summary>
        /// <returns> the recording id of the last received event. </returns>
        public long RecordingId()
        {
            return _recordingId;
        }

        /// <summary>
        /// Get the position the recording started at.
        /// </summary>
        /// <returns> the position the recording started at. </returns>
        public long RecordingStartPosition()
        {
            return _recordingStartPosition;
        }

        /// <summary>
        /// Get the current recording position.
        /// </summary>
        /// <returns> the current recording position. </returns>
        public long RecordingPosition()
        {
            return _recordingPosition;
        }

        /// <summary>
        /// Get the position the recording stopped at.
        /// </summary>
        /// <returns> the position the recording stopped at. </returns>
        public long RecordingStopPosition()
        {
            return _recordingStopPosition;
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

            _templateId = _messageHeaderDecoder.TemplateId();
            switch (_templateId)
            {
                case RecordingStartedDecoder.TEMPLATE_ID:
                    _recordingStartedDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _recordingId = _recordingStartedDecoder.RecordingId();
                    _recordingStartPosition = _recordingStartedDecoder.StartPosition();
                    _recordingPosition = _recordingStartPosition;
                    _recordingStopPosition = Aeron.Aeron.NULL_VALUE;
                    _isPollComplete = true;
                    return ControlledFragmentHandlerAction.BREAK;

                case RecordingProgressDecoder.TEMPLATE_ID:
                    _recordingProgressDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _recordingId = _recordingProgressDecoder.RecordingId();
                    _recordingStartPosition = _recordingProgressDecoder.StartPosition();
                    _recordingPosition = _recordingProgressDecoder.Position();
                    _recordingStopPosition = Aeron.Aeron.NULL_VALUE;
                    _isPollComplete = true;
                    return ControlledFragmentHandlerAction.BREAK;

                case RecordingStoppedDecoder.TEMPLATE_ID:
                    _recordingStoppedDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version()
                    );

                    _recordingId = _recordingStoppedDecoder.RecordingId();
                    _recordingStartPosition = _recordingStoppedDecoder.StartPosition();
                    _recordingStopPosition = _recordingStoppedDecoder.StopPosition();
                    _recordingPosition = _recordingStopPosition;
                    _isPollComplete = true;
                    return ControlledFragmentHandlerAction.BREAK;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }
    }
}
