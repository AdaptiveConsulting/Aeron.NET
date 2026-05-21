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
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Internal wrapper around an <see cref="AeronArchive.AsyncConnect"/> and the resulting
    /// <see cref="AeronArchive"/> that exposes a non-blocking state machine to a
    /// <see cref="PersistentSubscription"/>. Control responses and recording descriptors are dispatched
    /// to an <see cref="IAsyncAeronArchiveListener"/>.
    /// </summary>
    /// <remarks>Since 1.51.0</remarks>
    internal sealed class AsyncAeronArchive : IDisposable
    {
        private readonly ControlledFragmentAssembler _fragmentAssembler;
        private readonly MessageHeaderDecoder _messageHeaderDecoder = new MessageHeaderDecoder();
        private readonly ControlResponseDecoder _controlResponseDecoder = new ControlResponseDecoder();
        private readonly RecordingDescriptorDecoder _recordingDescriptorDecoder = new RecordingDescriptorDecoder();

        private readonly AeronArchive.Context _context;
        private readonly IAsyncAeronArchiveListener _listener;
        private State _state = State.CONNECTING;
        private AeronArchive.AsyncConnect _asyncConnect;
        private AeronArchive _aeronArchive;
        private ArchiveProxy _archiveProxy;
        private Subscription _subscription;
        private long _controlSessionId;

        internal AsyncAeronArchive(AeronArchive.Context context, IAsyncAeronArchiveListener listener)
        {
            _context = context;
            _listener = listener;
            _fragmentAssembler = new ControlledFragmentAssembler(new ControlledFragmentHandlerDelegate(OnFragment));
        }

        internal bool TrySendListRecordingRequest(long correlationId, long recordingId)
        {
            if (_state == State.CONNECTED)
            {
                try
                {
                    return _archiveProxy.ListRecording(recordingId, correlationId, _controlSessionId);
                }
                catch (ArchiveException)
                {
                    _state = State.DISCONNECTED;
                }
            }

            return false;
        }

        internal bool TrySendMaxRecordedPositionRequest(long correlationId, long recordingId)
        {
            if (_state == State.CONNECTED)
            {
                try
                {
                    return _archiveProxy.GetMaxRecordedPosition(recordingId, correlationId, _controlSessionId);
                }
                catch (ArchiveException)
                {
                    _state = State.DISCONNECTED;
                }
            }

            return false;
        }

        internal bool TrySendReplayTokenRequest(long correlationId, long recordingId)
        {
            if (_state == State.CONNECTED)
            {
                try
                {
                    return _archiveProxy.RequestReplayToken(correlationId, _controlSessionId, recordingId);
                }
                catch (ArchiveException)
                {
                    _state = State.DISCONNECTED;
                }
            }

            return false;
        }

        internal bool TrySendReplayRequest(
            long correlationId,
            long recordingId,
            int replayStreamId,
            string replayChannel,
            ReplayParams replayParams)
        {
            return TrySendReplayRequest(
                _archiveProxy, correlationId, recordingId, replayStreamId, replayChannel, replayParams);
        }

        internal bool TrySendReplayRequest(
            ArchiveProxy archiveProxy,
            long correlationId,
            long recordingId,
            int replayStreamId,
            string replayChannel,
            ReplayParams replayParams)
        {
            if (_state == State.CONNECTED)
            {
                try
                {
                    return archiveProxy.Replay(
                        recordingId,
                        replayChannel,
                        replayStreamId,
                        replayParams,
                        correlationId,
                        _controlSessionId);
                }
                catch (ArchiveException)
                {
                    _state = State.DISCONNECTED;
                }
            }

            return false;
        }

        internal bool TrySendStopReplayRequest(long correlationId, long replaySessionId)
        {
            if (_state == State.CONNECTED)
            {
                try
                {
                    return _archiveProxy.StopReplay(replaySessionId, correlationId, _controlSessionId);
                }
                catch (ArchiveException)
                {
                    _state = State.DISCONNECTED;
                }
            }

            return false;
        }

        internal int Poll()
        {
            switch (_state)
            {
                case State.CONNECTING:
                    return Connecting();
                case State.CONNECTED:
                    return Connected();
                case State.DISCONNECTED:
                    return Disconnected();
                case State.CLOSED:
                default:
                    return 0;
            }
        }

        private int Connecting()
        {
            int workCount = 0;

            AeronArchive.AsyncConnect asyncConnect = _asyncConnect;

            if (asyncConnect == null)
            {
                try
                {
                    asyncConnect = AeronArchive.ConnectAsync(_context.Clone());
                }
                catch (Exception e)
                {
                    _state = State.CLOSED;
                    _listener.OnError(e);
                    return 1;
                }

                _asyncConnect = asyncConnect;
                workCount++;
            }

            AeronArchive aeronArchive = null;
            try
            {
                int stepBefore = asyncConnect.Step();
                aeronArchive = asyncConnect.Poll();
                workCount += asyncConnect.Step() - stepBefore;
            }
            catch (Exception e)
            {
                _asyncConnect = null;
                CloseHelper.QuietDispose(asyncConnect);
                _listener.OnError(e);
                workCount++;
            }

            if (aeronArchive != null)
            {
                _asyncConnect = null;
                _aeronArchive = aeronArchive;
                _archiveProxy = aeronArchive.Proxy();
                _subscription = aeronArchive.ControlResponsePoller().Subscription();
                _controlSessionId = aeronArchive.ControlSessionId();
                _state = State.CONNECTED;
                _listener.OnConnected();
            }

            return workCount;
        }

        private int Connected()
        {
            if (!_subscription.IsConnected)
            {
                _state = State.DISCONNECTED;
                return 1;
            }

            return _subscription.ControlledPoll(_fragmentAssembler, ControlResponsePoller.FRAGMENT_LIMIT);
        }

        private int Disconnected()
        {
            CloseHelper.QuietDispose(_aeronArchive);

            _subscription = null;
            _archiveProxy = null;
            _aeronArchive = null;

            try
            {
                _listener.OnDisconnected();
            }
            finally
            {
                _state = State.CONNECTING;
            }

            return 1;
        }

        internal bool IsConnected => _state == State.CONNECTED;

        internal bool IsClosed => _state == State.CLOSED;

        public void Dispose()
        {
            if (_state != State.CLOSED)
            {
                _state = State.CLOSED;

                CloseHelper.QuietDispose(_asyncConnect);
                CloseHelper.QuietDispose(_aeronArchive);
            }
        }

        private ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            _messageHeaderDecoder.Wrap(buffer, offset);

            int schemaId = _messageHeaderDecoder.SchemaId();
            if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
            {
                throw new ArchiveException(
                    "expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
            }

            int templateId = _messageHeaderDecoder.TemplateId();
            switch (templateId)
            {
                case ControlResponseDecoder.TEMPLATE_ID:
                    _controlResponseDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    if (_controlResponseDecoder.ControlSessionId() == _controlSessionId)
                    {
                        _listener.OnControlResponse(
                            _controlResponseDecoder.CorrelationId(),
                            _controlResponseDecoder.RelevantId(),
                            _controlResponseDecoder.Code(),
                            _controlResponseDecoder.ErrorMessage());
                    }

                    break;

                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    _recordingDescriptorDecoder.Wrap(
                        buffer,
                        offset + MessageHeaderEncoder.ENCODED_LENGTH,
                        _messageHeaderDecoder.BlockLength(),
                        _messageHeaderDecoder.Version());

                    if (_recordingDescriptorDecoder.ControlSessionId() == _controlSessionId)
                    {
                        _listener.OnRecordingDescriptor(
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
                            _recordingDescriptorDecoder.SourceIdentity());
                    }

                    break;
            }

            return ControlledFragmentHandlerAction.CONTINUE;
        }

        private sealed class ControlledFragmentHandlerDelegate : IControlledFragmentHandler
        {
            private readonly Func<IDirectBuffer, int, int, Header, ControlledFragmentHandlerAction> _handler;

            internal ControlledFragmentHandlerDelegate(
                Func<IDirectBuffer, int, int, Header, ControlledFragmentHandlerAction> handler)
            {
                _handler = handler;
            }

            public ControlledFragmentHandlerAction OnFragment(
                IDirectBuffer buffer, int offset, int length, Header header)
            {
                return _handler(buffer, offset, length, header);
            }
        }

        private enum State
        {
            CONNECTING,
            CONNECTED,
            DISCONNECTED,
            CLOSED
        }
    }
}
