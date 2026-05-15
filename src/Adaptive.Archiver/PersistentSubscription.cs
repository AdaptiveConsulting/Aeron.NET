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
using System.Diagnostics;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;
using AeronClient = Adaptive.Aeron.Aeron;

namespace Adaptive.Archiver
{
    /// <summary>
    /// A <c>PersistentSubscription</c> allows the consumption of messages from a live publication that is also being
    /// recorded to an Archive, in order and without gaps, regardless of when the messages were published.
    /// It tries to read messages from the live subscription as much as possible, falling back to an Archive replay when
    /// necessary, making any source switches transparent to the application.
    /// <para/>
    /// It offers:
    /// <list type="bullet">
    /// <item>late join - if any messages have been published after the provided start position, they will be replayed
    /// from the recording before switching to the live subscription,</item>
    /// <item>seamless recovery - if a subscription gets disconnected due to flow control or a network issue, it will
    /// automatically recover and replay any missed messages.</item>
    /// </list>
    /// <para/>
    /// Not thread-safe. Must be polled in a duty cycle. Performs message reassembly.
    /// </summary>
    /// <remarks>Since 1.51.0</remarks>
    public sealed class PersistentSubscription : IDisposable
    {
        /// <summary>
        /// Special value for <see cref="Context.StartPosition(long)"/> which will make a <c>PersistentSubscription</c>
        /// start by replaying the recording from its start position. Used when an application needs to process all
        /// historical and future messages.
        /// </summary>
        public const long FROM_START = AeronArchive.NULL_POSITION;

        /// <summary>
        /// Special value for <see cref="Context.StartPosition(long)"/> which will make a <c>PersistentSubscription</c>
        /// start by joining the live subscription. Used when an application needs to process all future messages from
        /// an unspecified join position, but does not need any historical ones.
        /// </summary>
        public const long FROM_LIVE = -2;

        private readonly ImageControlledFragmentAssembler _controlledFragmentAssembler;
        private readonly ImageFragmentAssembler _uncontrolledFragmentAssembler;
        private readonly IControlledFragmentHandler _liveCatchupFragmentHandler;
        private readonly IControlledFragmentHandler _replayCatchupControlledFragmentHandler;
        private readonly IControlledFragmentHandler _replayCatchupUncontrolledFragmentHandler;
        private readonly ListRecordingRequest _listRecordingRequest = new ListRecordingRequest();
        private readonly MaxRecordedPosition _maxRecordedPosition;
        private readonly AsyncArchiveOp _replayRequest = new AsyncArchiveOp();
        private readonly AsyncArchiveOp _replayTokenRequest = new AsyncArchiveOp();
        private readonly ReplayParams _replayParams = new ReplayParams();
        private readonly Context _ctx;
        private readonly long _recordingId;
        private readonly IPersistentSubscriptionListener _listener;
        private readonly string _liveChannel;
        private readonly int _liveStreamId;
        private readonly string _replayChannel;
        private readonly ChannelUri _replayChannelUri;
        private readonly ReplayChannelType _replayChannelType;
        private readonly int _replayStreamId;
        private readonly AeronClient _aeron;
        private readonly INanoClock _nanoClock;
        private readonly AsyncAeronArchive _asyncAeronArchive;
        private readonly long _messageTimeoutNs;
        private readonly Counter _stateCounter;
        private readonly Counter _joinDifferenceCounter;
        private readonly Counter _liveLeftCounter;
        private readonly Counter _liveJoinedCounter;

        private State _state;
        private long _replaySessionId = AeronClient.NULL_VALUE;
        private long _replaySubscriptionId = AeronClient.NULL_VALUE;
        private Subscription _replaySubscription;
        private long _replayImageDeadline;
        private Image _replayImage;
        private long _requestPublicationId;
        private ExclusivePublication _requestPublication;
        private ArchiveProxy _responseChannelArchiveProxy;
        private long _replayToken = AeronClient.NULL_VALUE;
        private long _liveSubscriptionId = AeronClient.NULL_VALUE;
        private Subscription _liveSubscription;
        private long _liveImageDeadline;
        private bool _liveImageDeadlineBreached;
        private Image _liveImage;
        private IControlledFragmentHandler _controlledFragmentHandler;
        private IFragmentHandler _uncontrolledFragmentHandler;
        private long _joinDifference;
        private long _nextLivePosition = AeronClient.NULL_VALUE;
        private long _position;
        private long _lastObservedLivePosition = AeronClient.NULL_VALUE;
        private Exception _failureReason;

        private PersistentSubscription(Context ctx)
        {
            ctx.Conclude();

            _ctx = ctx;
            _recordingId = ctx.RecordingId();
            _liveChannel = ctx.LiveChannel();
            _liveStreamId = ctx.LiveStreamId();
            _replayChannel = ctx.ReplayChannel();
            _replayChannelUri = ChannelUri.Parse(_replayChannel);
            _replayChannelType = ReplayChannelTypeHelper.Of(_replayChannelUri);
            _replayStreamId = ctx.ReplayStreamId();
            _listener = ctx.Listener();
            _aeron = ctx.Aeron();
            _nanoClock = _aeron.Ctx.NanoClock();
            _asyncAeronArchive = new AsyncAeronArchive(ctx.AeronArchiveContext(), new ArchiveListener(this));
            _messageTimeoutNs = ctx.AeronArchiveContext().MessageTimeoutNs();
            _stateCounter = ctx.StateCounter();
            _joinDifferenceCounter = ctx.JoinDifferenceCounter();
            _liveLeftCounter = ctx.LiveLeftCounter();
            _liveJoinedCounter = ctx.LiveJoinedCounter();
            _position = ctx.StartPosition();

            _controlledFragmentAssembler = new ImageControlledFragmentAssembler(
                new InlineControlledFragmentHandler(OnFragmentControlled));
            _uncontrolledFragmentAssembler = new ImageFragmentAssembler(
                new InlineFragmentHandler(OnFragmentUncontrolled));
            _liveCatchupFragmentHandler = new InlineControlledFragmentHandler(OnLiveCatchupFragment);
            _replayCatchupControlledFragmentHandler =
                new InlineControlledFragmentHandler(OnReplayCatchupFragmentControlled);
            _replayCatchupUncontrolledFragmentHandler =
                new InlineControlledFragmentHandler(OnReplayCatchupFragmentUncontrolled);
            _maxRecordedPosition = new MaxRecordedPosition(this);

            UpdateJoinDifference(long.MinValue);

            _state = State.AWAIT_ARCHIVE_CONNECTION;

            if (!_stateCounter.IsClosed)
            {
                _stateCounter.SetRelease((int)_state);
            }
        }

        /// <summary>
        /// Creates a new <c>PersistentSubscription</c> using the given configuration. The returned instance must be
        /// polled to perform any work.
        /// </summary>
        /// <param name="ctx"> the configuration to use for the new <c>PersistentSubscription</c>. </param>
        /// <returns> a new PersistentSubscription. </returns>
        /// <seealso cref="Poll(IFragmentHandler, int)"/>
        /// <seealso cref="ControlledPoll(IControlledFragmentHandler, int)"/>
        public static PersistentSubscription Create(Context ctx)
        {
            return new PersistentSubscription(ctx);
        }

        /// <summary>
        /// Poll for the next available message(s). The handler receives fully assembled messages.
        /// <para/>
        /// Either this method or <see cref="ControlledPoll(IControlledFragmentHandler, int)"/> must be called in a duty
        /// cycle for the <c>PersistentSubscription</c> to perform its work.
        /// </summary>
        /// <param name="fragmentHandler"> the handler to receive assembled messages if any are available. </param>
        /// <param name="fragmentLimit"> the max number of fragments to be processed during the poll operation. </param>
        /// <returns> positive number if work has been done, 0 otherwise. </returns>
        public int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
        {
            try
            {
                _uncontrolledFragmentHandler = fragmentHandler;
                return DoWork(fragmentLimit, false);
            }
            finally
            {
                _uncontrolledFragmentHandler = null;
            }
        }

        /// <summary>
        /// Poll for the next available message(s), allowing the <see cref="IControlledFragmentHandler"/> to control
        /// whether polling continues.
        /// </summary>
        /// <param name="fragmentHandler"> the handler to receive assembled messages if any are available. </param>
        /// <param name="fragmentLimit"> the max number of fragments to be processed during the poll operation. </param>
        /// <returns> positive number if work has been done, 0 otherwise. </returns>
        public int ControlledPoll(IControlledFragmentHandler fragmentHandler, int fragmentLimit)
        {
            try
            {
                _controlledFragmentHandler = fragmentHandler;
                return DoWork(fragmentLimit, true);
            }
            finally
            {
                _controlledFragmentHandler = null;
            }
        }

        private int DoWork(int fragmentLimit, bool isControlled)
        {
            int workCount = 0;
            AgentInvoker agentInvoker = _aeron.ConductorAgentInvoker;
            if (null != agentInvoker)
            {
                workCount += agentInvoker.Invoke();
            }
            workCount += _asyncAeronArchive.Poll();

            switch (_state)
            {
                case State.AWAIT_ARCHIVE_CONNECTION:
                    workCount += AwaitArchiveConnection();
                    break;
                case State.SEND_LIST_RECORDING_REQUEST:
                    workCount += SendListRecordingRequest();
                    break;
                case State.AWAIT_LIST_RECORDING_RESPONSE:
                    workCount += AwaitListRecordingResponse();
                    break;
                case State.SEND_REPLAY_REQUEST:
                    workCount += SendReplayRequest();
                    break;
                case State.AWAIT_REPLAY_RESPONSE:
                    workCount += AwaitReplayResponse();
                    break;
                case State.ADD_REPLAY_SUBSCRIPTION:
                    workCount += AddReplaySubscription();
                    break;
                case State.AWAIT_REPLAY_SUBSCRIPTION:
                    workCount += AwaitReplaySubscription();
                    break;
                case State.AWAIT_REPLAY_CHANNEL_ENDPOINT:
                    workCount += AwaitReplayChannelEndpoint();
                    break;
                case State.ADD_REQUEST_PUBLICATION:
                    workCount += AddRequestPublication();
                    break;
                case State.AWAIT_REQUEST_PUBLICATION:
                    workCount += AwaitRequestPublication();
                    break;
                case State.SEND_REPLAY_TOKEN_REQUEST:
                    workCount += SendReplayTokenRequest();
                    break;
                case State.AWAIT_REPLAY_TOKEN:
                    workCount += AwaitReplayToken();
                    break;
                case State.REPLAY:
                    workCount += Replay(fragmentLimit, isControlled);
                    break;
                case State.ATTEMPT_SWITCH:
                    workCount += AttemptSwitch(fragmentLimit, isControlled);
                    break;
                case State.ADD_LIVE_SUBSCRIPTION:
                    workCount += AddLiveSubscription();
                    break;
                case State.AWAIT_LIVE:
                    workCount += AwaitLive();
                    break;
                case State.LIVE:
                    workCount += Live(fragmentLimit, isControlled);
                    break;
                case State.FAILED:
                    break;
            }

            return workCount;
        }

        /// <summary>
        /// Indicates if the persistent subscription is reading from the live stream.
        /// </summary>
        /// <returns> true if persistent subscription is reading from the live stream. </returns>
        public bool IsLive()
        {
            return State.LIVE == _state;
        }

        /// <summary>
        /// Indicates if the persistent subscription is replaying from a recording.
        /// </summary>
        /// <returns> true if persistent subscription is replaying from a recording. </returns>
        public bool IsReplaying()
        {
            return State.REPLAY == _state || State.ATTEMPT_SWITCH == _state;
        }

        /// <summary>
        /// Indicates if the persistent subscription failed.
        /// </summary>
        /// <returns> true if persistent subscription has failed. </returns>
        public bool HasFailed()
        {
            return State.FAILED == _state;
        }

        /// <summary>
        /// The terminal error that caused the persistent subscription to fail.
        /// Only meaningful when <see cref="HasFailed"/> returns true.
        /// </summary>
        /// <returns> exception indicating the failure reason, or null if not in the failed state. </returns>
        public Exception FailureReason()
        {
            return _failureReason;
        }

        /// <summary>
        /// Close this PersistentSubscription and release any resources owned by it.
        /// </summary>
        public void Dispose()
        {
            CloseHelper.QuietDispose(CloseLive);
            CloseHelper.QuietDispose(CloseReplay);
            CloseHelper.QuietDispose(_asyncAeronArchive);
            CloseHelper.QuietDispose(_ctx.Dispose);
        }

        private void CloseLive()
        {
            if (!_ctx.OwnsAeronClient())
            {
                if (AeronClient.NULL_VALUE != _liveSubscriptionId)
                {
                    _aeron.AsyncRemoveSubscription(_liveSubscriptionId);
                }

                if (null != _liveSubscription)
                {
                    _liveSubscription.Dispose();
                }
            }
        }

        private void CloseReplay()
        {
            CleanUpReplay();

            if (!_ctx.OwnsAeronClient())
            {
                if (AeronClient.NULL_VALUE != _requestPublicationId)
                {
                    _aeron.AsyncRemovePublication(_requestPublicationId);
                }

                if (null != _requestPublication)
                {
                    _requestPublication.Dispose();
                }

                if (AeronClient.NULL_VALUE != _replaySubscriptionId)
                {
                    _aeron.AsyncRemoveSubscription(_replaySubscriptionId);
                }

                if (null != _replaySubscription)
                {
                    _replaySubscription.Dispose();
                }
            }
        }

        internal Context ContextInternal()
        {
            return _ctx;
        }

        internal long JoinDifference()
        {
            return _joinDifference;
        }

        private void UpdateJoinDifference(long joinDifference)
        {
            _joinDifference = joinDifference;

            if (!_joinDifferenceCounter.IsClosed)
            {
                _joinDifferenceCounter.SetRelease(joinDifference);
            }
        }

        private int AwaitArchiveConnection()
        {
            if (!_asyncAeronArchive.IsConnected)
            {
                return 0;
            }

            SetState(State.SEND_LIST_RECORDING_REQUEST);

            return 1;
        }

        private int SendListRecordingRequest()
        {
            long correlationId = _aeron.NextCorrelationId();

            if (!_asyncAeronArchive.TrySendListRecordingRequest(correlationId, _recordingId))
            {
                if (_asyncAeronArchive.IsConnected)
                {
                    return 0;
                }
                else
                {
                    SetState(State.AWAIT_ARCHIVE_CONNECTION);

                    return 1;
                }
            }

            _listRecordingRequest.Init(correlationId, _nanoClock.NanoTime() + _messageTimeoutNs);
            _listRecordingRequest.Remaining = 1;

            SetState(State.AWAIT_LIST_RECORDING_RESPONSE);

            return 1;
        }

        private int AwaitListRecordingResponse()
        {
            if (!_listRecordingRequest.ResponseReceived)
            {
                if (_nanoClock.NanoTime() - _listRecordingRequest.DeadlineNs >= 0)
                {
                    SetState(_asyncAeronArchive.IsConnected
                        ? State.SEND_LIST_RECORDING_REQUEST
                        : State.AWAIT_ARCHIVE_CONNECTION);

                    return 1;
                }

                return 0;
            }

            PersistentSubscriptionException error = ValidateDescriptor();

            if (null != error)
            {
                SetState(State.FAILED);
                OnTerminalError(error);
            }
            else
            {
                if (FROM_LIVE == _position ||
                    (AeronArchive.NULL_POSITION != _listRecordingRequest.StopPosition &&
                        _position == _listRecordingRequest.StopPosition))
                {
                    SetState(State.ADD_LIVE_SUBSCRIPTION);
                }
                else
                {
                    SetUpReplay();
                }
            }

            return 1;
        }

        private PersistentSubscriptionException ValidateDescriptor()
        {
            if (0 == _listRecordingRequest.Remaining)
            {
                Debug.Assert(
                    _listRecordingRequest.RecordingId == _recordingId,
                    _listRecordingRequest.ToString());

                if (_liveStreamId != _listRecordingRequest.StreamId)
                {
                    return new PersistentSubscriptionException(
                        PersistentSubscriptionException.Reason.STREAM_ID_MISMATCH,
                        "Requested live stream with ID: " + _liveStreamId + " does not match stream ID: " +
                        _listRecordingRequest.StreamId + " for recording: " + _recordingId);
                }

                if (AeronArchive.NULL_POSITION != _listRecordingRequest.StopPosition &&
                    _lastObservedLivePosition > _listRecordingRequest.StopPosition)
                {
                    return new PersistentSubscriptionException(
                        PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                        "recording " + _recordingId + " stopped at position=" +
                            _listRecordingRequest.StopPosition +
                            " which is earlier than last observed live position=" +
                            _lastObservedLivePosition);
                }

                if (_position >= 0)
                {
                    if (_position < _listRecordingRequest.StartPosition)
                    {
                        return new PersistentSubscriptionException(
                            PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                            ArchiveException.BuildReplayBeforeStartErrorMsg(
                                _recordingId, _position, _listRecordingRequest.StartPosition));
                    }

                    if (AeronArchive.NULL_POSITION != _listRecordingRequest.StopPosition &&
                        _position > _listRecordingRequest.StopPosition)
                    {
                        return new PersistentSubscriptionException(
                            PersistentSubscriptionException.Reason.INVALID_START_POSITION,
                            ArchiveException.BuildReplayExceedsLimitErrorMsg(
                                _recordingId, _position, _listRecordingRequest.StopPosition));
                    }
                }
                else if (FROM_START == _position)
                {
                    _position = _listRecordingRequest.StartPosition;
                }
            }
            else
            {
                Debug.Assert(
                    1 == _listRecordingRequest.Remaining &&
                        ControlResponseCode.RECORDING_UNKNOWN == _listRecordingRequest.Code &&
                        _listRecordingRequest.RelevantId == _recordingId,
                    _listRecordingRequest.ToString());

                return new PersistentSubscriptionException(
                    PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND,
                    ArchiveException.BuildUnknownRecordingErrorMsg(_recordingId));
            }

            return null;
        }

        private void SetUpReplay()
        {
            ResetReplayCatchupState();

            switch (_replayChannelType)
            {
                case ReplayChannelType.SESSION_SPECIFIC:
                    SetState(State.SEND_REPLAY_REQUEST);
                    break;
                case ReplayChannelType.DYNAMIC_PORT:
                case ReplayChannelType.RESPONSE_CHANNEL:
                    SetState(State.ADD_REPLAY_SUBSCRIPTION);
                    break;
            }
        }

        private void RefreshRecordingDescriptor()
        {
            ResetReplayCatchupState();

            SetState(State.SEND_LIST_RECORDING_REQUEST);
        }

        private void ResetReplayCatchupState()
        {
            UpdateJoinDifference(long.MinValue);
            _maxRecordedPosition.Reset(_listRecordingRequest.TermBufferLength >> 2);
            _nextLivePosition = AeronClient.NULL_VALUE;
        }

        private void CleanUpReplay()
        {
            if (AeronClient.NULL_VALUE != _replaySessionId)
            {
                _asyncAeronArchive.TrySendStopReplayRequest(_aeron.NextCorrelationId(), _replaySessionId);

                _replaySessionId = AeronClient.NULL_VALUE;
            }
        }

        private void CleanUpReplaySubscription()
        {
            if (AeronClient.NULL_VALUE != _replaySubscriptionId)
            {
                _aeron.AsyncRemoveSubscription(_replaySubscriptionId);
            }

            if (null != _replaySubscription)
            {
                _aeron.AsyncRemoveSubscription(_replaySubscription.RegistrationId);
            }

            _replaySubscriptionId = AeronClient.NULL_VALUE;
            _replaySubscription = null;
            _replayImage = null;
        }

        private void CleanUpRequestPublication()
        {
            if (AeronClient.NULL_VALUE != _requestPublicationId)
            {
                _aeron.AsyncRemovePublication(_requestPublicationId);
            }

            if (null != _requestPublication)
            {
                _aeron.AsyncRemovePublication(_requestPublication.RegistrationId);
            }

            _requestPublicationId = AeronClient.NULL_VALUE;
            _requestPublication = null;
            _responseChannelArchiveProxy = null;
        }

        private void CleanUpLiveSubscription()
        {
            if (AeronClient.NULL_VALUE != _liveSubscriptionId)
            {
                _aeron.AsyncRemoveSubscription(_liveSubscriptionId);
            }

            if (null != _liveSubscription)
            {
                _aeron.AsyncRemoveSubscription(_liveSubscription.RegistrationId);
            }

            _liveSubscriptionId = AeronClient.NULL_VALUE;
            _liveSubscription = null;
            _liveImage = null;
        }

        private int SendReplayRequest()
        {
            long correlationId = _aeron.NextCorrelationId();

            string channel;
            switch (_replayChannelType)
            {
                case ReplayChannelType.SESSION_SPECIFIC:
                    channel = _replayChannel;
                    break;
                case ReplayChannelType.DYNAMIC_PORT:
                case ReplayChannelType.RESPONSE_CHANNEL:
                    channel = _replayChannelUri.ToString();
                    break;
                default:
                    channel = _replayChannel;
                    break;
            }

            _replayParams.Reset();
            _replayParams.Position(_position).Length(AeronArchive.REPLAY_ALL_AND_FOLLOW);
            bool result;
            if (ReplayChannelType.RESPONSE_CHANNEL == _replayChannelType)
            {
                _replayParams.ReplayToken(_replayToken);

                result = _asyncAeronArchive.TrySendReplayRequest(
                    _responseChannelArchiveProxy,
                    correlationId,
                    _recordingId,
                    _replayStreamId,
                    channel,
                    _replayParams);
            }
            else
            {
                result = _asyncAeronArchive.TrySendReplayRequest(
                    correlationId,
                    _recordingId,
                    _replayStreamId,
                    channel,
                    _replayParams);
            }

            if (!result)
            {
                if (_asyncAeronArchive.IsConnected)
                {
                    return 0;
                }
                else
                {
                    CleanUpRequestPublication();
                    CleanUpReplaySubscription();

                    SetState(State.AWAIT_ARCHIVE_CONNECTION);

                    return 1;
                }
            }

            _replayRequest.Init(correlationId, _nanoClock.NanoTime() + _messageTimeoutNs);

            SetState(State.AWAIT_REPLAY_RESPONSE);

            return 1;
        }

        private int AwaitReplayResponse()
        {
            if (!_replayRequest.ResponseReceived)
            {
                if (_nanoClock.NanoTime() - _replayRequest.DeadlineNs >= 0)
                {
                    CleanUpRequestPublication();
                    CleanUpReplaySubscription();
                    if (_asyncAeronArchive.IsConnected)
                    {
                        SetUpReplay();
                    }
                    else
                    {
                        SetState(State.AWAIT_ARCHIVE_CONNECTION);
                    }

                    return 1;
                }

                return 0;
            }

            if (ControlResponseCode.OK != _replayRequest.Code)
            {
                SetState(State.FAILED);

                CleanUpRequestPublication();
                CleanUpReplaySubscription();

                int errorCode = (int)_replayRequest.RelevantId;

                PersistentSubscriptionException.Reason reason;
                if (errorCode == ArchiveException.INVALID_POSITION)
                {
                    reason = PersistentSubscriptionException.Reason.INVALID_START_POSITION;
                }
                else if (errorCode == ArchiveException.UNKNOWN_RECORDING)
                {
                    reason = PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND;
                }
                else
                {
                    reason = PersistentSubscriptionException.Reason.GENERIC;
                }

                OnTerminalError(new PersistentSubscriptionException(
                    reason, "replay request failed: " + _replayRequest.ErrorMessage));

                return 1;
            }

            _replaySessionId = _replayRequest.RelevantId;

            switch (_replayChannelType)
            {
                case ReplayChannelType.SESSION_SPECIFIC:
                    _replayChannelUri.Put(
                        AeronClient.Context.SESSION_ID_PARAM_NAME,
                        ((int)_replaySessionId).ToString());
                    SetState(State.ADD_REPLAY_SUBSCRIPTION);
                    return 1;
                case ReplayChannelType.DYNAMIC_PORT:
                    _replayImageDeadline = _nanoClock.NanoTime() + _messageTimeoutNs;
                    SetState(State.REPLAY);
                    return 1;
                case ReplayChannelType.RESPONSE_CHANNEL:
                    CleanUpRequestPublication();
                    _replayImageDeadline = _nanoClock.NanoTime() + _messageTimeoutNs;
                    SetState(State.REPLAY);
                    return 1;
                default:
                    return 1;
            }
        }

        private int AddReplaySubscription()
        {
            string channel;
            switch (_replayChannelType)
            {
                case ReplayChannelType.SESSION_SPECIFIC:
                    channel = _replayChannelUri.ToString();
                    break;
                case ReplayChannelType.DYNAMIC_PORT:
                case ReplayChannelType.RESPONSE_CHANNEL:
                    channel = _replayChannel;
                    break;
                default:
                    channel = _replayChannel;
                    break;
            }

            _replaySubscriptionId = _aeron.AsyncAddSubscription(channel, _replayStreamId);

            SetState(State.AWAIT_REPLAY_SUBSCRIPTION);

            return 1;
        }

        private int AwaitReplaySubscription()
        {
            Subscription subscription;
            try
            {
                subscription = _aeron.GetSubscription(_replaySubscriptionId);
            }
            catch (RegistrationException e)
            {
                _replaySubscriptionId = AeronClient.NULL_VALUE;

                CleanUpReplay();

                if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == e.ErrorCode())
                {
                    SetUpReplay();
                    _listener.OnError(e);
                }
                else
                {
                    SetState(State.FAILED);
                    OnTerminalError(e);
                }

                return 1;
            }

            if (null == subscription)
            {
                return 0;
            }

            _replaySubscriptionId = AeronClient.NULL_VALUE;
            _replaySubscription = subscription;

            if (ReplayChannelType.SESSION_SPECIFIC == _replayChannelType)
            {
                _replayImageDeadline = _nanoClock.NanoTime() + _messageTimeoutNs;
            }

            switch (_replayChannelType)
            {
                case ReplayChannelType.SESSION_SPECIFIC:
                    SetState(State.REPLAY);
                    break;
                case ReplayChannelType.DYNAMIC_PORT:
                    SetState(State.AWAIT_REPLAY_CHANNEL_ENDPOINT);
                    break;
                case ReplayChannelType.RESPONSE_CHANNEL:
                    SetState(State.ADD_REQUEST_PUBLICATION);
                    break;
            }

            return 1;
        }

        private int AwaitReplayChannelEndpoint()
        {
            string resolvedChannel = _replaySubscription.TryResolveChannelEndpointPort();
            if (null == resolvedChannel)
            {
                return 0;
            }

            string resolvedEndpoint = ChannelUri.Parse(resolvedChannel).Get(AeronClient.Context.ENDPOINT_PARAM_NAME);
            if (null != resolvedEndpoint)
            {
                _replayChannelUri.Put(AeronClient.Context.ENDPOINT_PARAM_NAME, resolvedEndpoint);
            }

            SetState(State.SEND_REPLAY_REQUEST);

            return 1;
        }

        private int AddRequestPublication()
        {
            string controlRequestChannel = _ctx.AeronArchiveContext().ControlRequestChannel();
            int controlRequestStreamId = _ctx.AeronArchiveContext().ControlRequestStreamId();
            int controlTermBufferLength = _ctx.AeronArchiveContext().ControlTermBufferLength();
            ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder(controlRequestChannel)
                .SessionId((int?)null)
                .ResponseCorrelationId(_replaySubscription.RegistrationId)
                .TermId((int?)null)
                .InitialTermId((int?)null)
                .TermOffset((int?)null)
                .TermLength(controlTermBufferLength)
                .SpiesSimulateConnection(false);
            string requestPublicationChannel = uriBuilder.Build();

            _requestPublicationId = _aeron.AsyncAddExclusivePublication(
                requestPublicationChannel, controlRequestStreamId);
            SetState(State.AWAIT_REQUEST_PUBLICATION);
            return 1;
        }

        private int AwaitRequestPublication()
        {
            ExclusivePublication publication;
            try
            {
                publication = _aeron.GetExclusivePublication(_requestPublicationId);
            }
            catch (RegistrationException e)
            {
                CleanUpRequestPublication();
                CleanUpReplaySubscription();

                if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == e.ErrorCode())
                {
                    SetUpReplay();
                    _listener.OnError(e);
                }
                else
                {
                    SetState(State.FAILED);
                    OnTerminalError(e);
                }

                return 1;
            }

            if (null == publication)
            {
                return 0;
            }

            _requestPublicationId = AeronClient.NULL_VALUE;
            _requestPublication = publication;
            _responseChannelArchiveProxy = new ArchiveProxy(publication);

            SetState(State.SEND_REPLAY_TOKEN_REQUEST);

            return 1;
        }

        private int SendReplayTokenRequest()
        {
            long correlationId = _aeron.NextCorrelationId();

            if (!_asyncAeronArchive.TrySendReplayTokenRequest(correlationId, _recordingId))
            {
                if (_asyncAeronArchive.IsConnected)
                {
                    return 0;
                }
                else
                {
                    CleanUpRequestPublication();
                    CleanUpReplaySubscription();

                    SetState(State.AWAIT_ARCHIVE_CONNECTION);

                    return 1;
                }
            }

            _replayTokenRequest.Init(correlationId, _nanoClock.NanoTime() + _messageTimeoutNs);

            SetState(State.AWAIT_REPLAY_TOKEN);

            return 1;
        }

        private int AwaitReplayToken()
        {
            if (!_replayTokenRequest.ResponseReceived)
            {
                if (_nanoClock.NanoTime() - _replayTokenRequest.DeadlineNs >= 0)
                {
                    CleanUpRequestPublication();
                    CleanUpReplaySubscription();

                    if (_asyncAeronArchive.IsConnected)
                    {
                        SetUpReplay();
                    }
                    else
                    {
                        SetState(State.AWAIT_ARCHIVE_CONNECTION);
                    }

                    return 1;
                }

                return 0;
            }

            if (ControlResponseCode.OK != _replayTokenRequest.Code)
            {
                SetState(State.FAILED);

                CleanUpRequestPublication();
                CleanUpReplaySubscription();

                OnTerminalError(new ArchiveException(
                    "replay token request failed: " + _replayTokenRequest.ErrorMessage,
                    (int)_replayTokenRequest.RelevantId,
                    _replayTokenRequest.CorrelationId));

                return 1;
            }

            _replayToken = _replayTokenRequest.RelevantId;
            if (_replayChannelUri.IsIpc)
            {
                SetState(State.SEND_REPLAY_REQUEST);
            }
            else
            {
                SetState(State.AWAIT_REPLAY_CHANNEL_ENDPOINT);
            }
            return 1;
        }

        private int Replay(int fragmentLimit, bool isControlled)
        {
            Image replayImage = _replayImage;

            if (null == replayImage)
            {
                replayImage = _replaySubscription.ImageBySessionId((int)_replaySessionId);

                if (null == replayImage)
                {
                    if (_nanoClock.NanoTime() - _replayImageDeadline >= 0)
                    {
                        CleanUpReplay();
                        CleanUpReplaySubscription();
                        SetUpReplay();

                        return 1;
                    }

                    return 0;
                }

                _replayImage = replayImage;
            }

            if (replayImage.Closed)
            {
                _position = replayImage.Position;
                CleanUpLiveSubscription();
                CleanUpReplay();
                CleanUpReplaySubscription();
                RefreshRecordingDescriptor();

                return 1;
            }

            if (null == _liveSubscription && AeronClient.NULL_VALUE != _liveSubscriptionId)
            {
                try
                {
                    _liveSubscription = _aeron.GetSubscription(_liveSubscriptionId);

                    if (null != _liveSubscription)
                    {
                        _liveSubscriptionId = AeronClient.NULL_VALUE;
                        SetLiveImageDeadline();
                    }
                }
                catch (RegistrationException e)
                {
                    _liveSubscriptionId = AeronClient.NULL_VALUE;

                    if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE != e.ErrorCode())
                    {
                        CleanUpReplay();
                        CleanUpReplaySubscription();
                        SetState(State.FAILED);
                        OnTerminalError(e);
                    }
                    else
                    {
                        _listener.OnError(e);
                    }
                    return 1;
                }
            }

            if (null != _liveSubscription)
            {
                if (_liveSubscription.ImageCount > 0)
                {
                    _liveImage = _liveSubscription.ImageAtIndex(0);

                    long livePosition = _liveImage.Position;
                    long replayPosition = replayImage.Position;
                    UpdateJoinDifference(livePosition - replayPosition);

                    SetState(State.ATTEMPT_SWITCH);

                    return 1;
                }
                else if (!_liveImageDeadlineBreached && _nanoClock.NanoTime() - _liveImageDeadline >= 0)
                {
                    OnLiveImageDeadlineBreached();
                }
            }

            int fragments = DoPoll(replayImage, fragmentLimit, isControlled);

            _position = replayImage.Position;

            if (AeronClient.NULL_VALUE == _liveSubscriptionId &&
                null == _liveSubscription &&
                _maxRecordedPosition.IsCaughtUp(_position))
            {
                DoAddLiveSubscription();
            }

            return fragments;
        }

        private void DoAddLiveSubscription()
        {
            _liveImage = null;
            _liveSubscription = null;
            _liveSubscriptionId = _aeron.AsyncAddSubscription(_liveChannel, _liveStreamId);
        }

        private void SetLiveImageDeadline()
        {
            _liveImageDeadline = _nanoClock.NanoTime() + _messageTimeoutNs;
            _liveImageDeadlineBreached = false;
        }

        private void OnLiveImageDeadlineBreached()
        {
            _liveImageDeadlineBreached = true;
            _listener.OnError(new AeronEvent("No image became available on the live subscription within " +
                                             SystemUtil.FormatDuration(_messageTimeoutNs) + ". This could be " +
                                             "caused by the publisher being down, or by a misconfiguration of the " +
                                             "subscriber or a firewall between them."));
        }

        private int AttemptSwitch(int fragmentLimit, bool isControlled)
        {
            int fragments = 0;

            long livePosition = _liveImage.Position;
            long replayPosition = _replayImage.Position;

            if (replayPosition == livePosition)
            {
                SetState(State.LIVE);
            }
            else
            {
                if (_replayImage.Closed)
                {
                    _position = replayPosition;
                    AdvanceLastObservedLivePosition(livePosition);
                    CleanUpLiveSubscription();
                    CleanUpReplay();
                    CleanUpReplaySubscription();
                    RefreshRecordingDescriptor();

                    return 1;
                }

                if (_liveImage.Closed)
                {
                    CleanUpLiveSubscription();
                    ResetReplayCatchupState();
                    SetState(State.REPLAY);

                    return 1;
                }

                fragments += _liveImage.ControlledPoll(_liveCatchupFragmentHandler, fragmentLimit);

                if (isControlled)
                {
                    fragments += _replayImage.ControlledPoll(_replayCatchupControlledFragmentHandler, fragmentLimit);
                }
                else
                {
                    fragments += _replayImage.ControlledPoll(_replayCatchupUncontrolledFragmentHandler, fragmentLimit);
                }
            }

            if (IsLive())
            {
                CleanUpReplay();
                CleanUpReplaySubscription();
                OnLiveJoined();
            }

            return fragments;
        }

        private void OnTerminalError(Exception error)
        {
            _failureReason = error;
            _listener.OnError(error);
        }

        private ControlledFragmentHandlerAction OnLiveCatchupFragment(
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header)
        {
            long currentLivePosition = header.Position;
            long lastReplayPosition = _replayImage.Position;
            if (currentLivePosition <= lastReplayPosition)
            {
                return ControlledFragmentHandlerAction.CONTINUE;
            }
            _nextLivePosition = currentLivePosition;
            return ControlledFragmentHandlerAction.ABORT;
        }

        private ControlledFragmentHandlerAction OnReplayCatchupFragmentControlled(
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header)
        {
            long currentReplayPosition = header.Position;
            if (currentReplayPosition == _nextLivePosition)
            {
                SetState(State.LIVE);
                return ControlledFragmentHandlerAction.ABORT;
            }
            return _controlledFragmentAssembler.OnFragment(buffer, offset, length, header);
        }

        private ControlledFragmentHandlerAction OnReplayCatchupFragmentUncontrolled(
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header)
        {
            long currentReplayPosition = header.Position;
            if (currentReplayPosition == _nextLivePosition)
            {
                SetState(State.LIVE);
                return ControlledFragmentHandlerAction.ABORT;
            }
            _uncontrolledFragmentAssembler.OnFragment(buffer, offset, length, header);
            return ControlledFragmentHandlerAction.CONTINUE;
        }

        private int AddLiveSubscription()
        {
            DoAddLiveSubscription();

            SetState(State.AWAIT_LIVE);

            return 1;
        }

        private int AwaitLive()
        {
            if (null == _liveSubscription)
            {
                try
                {
                    _liveSubscription = _aeron.GetSubscription(_liveSubscriptionId);

                    if (null != _liveSubscription)
                    {
                        _liveSubscriptionId = AeronClient.NULL_VALUE;
                        SetLiveImageDeadline();
                    }
                }
                catch (RegistrationException e)
                {
                    _liveSubscriptionId = AeronClient.NULL_VALUE;

                    if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == e.ErrorCode())
                    {
                        SetState(State.ADD_LIVE_SUBSCRIPTION);
                        _listener.OnError(e);
                    }
                    else
                    {
                        SetState(State.FAILED);
                        OnTerminalError(e);
                    }

                    return 1;
                }
            }

            if (null != _liveSubscription)
            {
                if (0 < _liveSubscription.ImageCount)
                {
                    Image image = _liveSubscription.ImageAtIndex(0);
                    long livePosition = image.Position;
                    AdvanceLastObservedLivePosition(livePosition);

                    if (_position >= 0)
                    {
                        if (livePosition < _position)
                        {
                            CleanUpLiveSubscription();
                            SetState(State.FAILED);
                            OnTerminalError(new PersistentSubscriptionException(
                                PersistentSubscriptionException.Reason.GENERIC,
                                "live stream joined at position " + livePosition +
                                " which is earlier than last seen position " + _position));

                            return 1;
                        }
                        else if (livePosition > _position)
                        {
                            CleanUpLiveSubscription();
                            RefreshRecordingDescriptor();

                            return 1;
                        }
                    }

                    _liveImage = image;
                    _position = livePosition;
                    UpdateJoinDifference(0);
                    SetState(State.LIVE);
                    OnLiveJoined();

                    return 1;
                }
                else if (!_liveImageDeadlineBreached && _nanoClock.NanoTime() - _liveImageDeadline >= 0)
                {
                    OnLiveImageDeadlineBreached();
                }
            }

            return 0;
        }

        private int Live(int fragmentLimit, bool isControlled)
        {
            Image image = _liveImage;
            int fragments = DoPoll(image, fragmentLimit, isControlled);
            if (0 == fragments && image.Closed)
            {
                long finalPosition = image.Position;
                AdvanceLastObservedLivePosition(finalPosition);
                _position = finalPosition;
                CleanUpLiveSubscription();
                RefreshRecordingDescriptor();
                OnLiveLeft();

                return 1;
            }
            return fragments;
        }

        private void AdvanceLastObservedLivePosition(long livePosition)
        {
            if (livePosition > _lastObservedLivePosition)
            {
                _lastObservedLivePosition = livePosition;
            }
        }

        private void SetState(State newState)
        {
            if (newState != _state)
            {
                _state = newState;
                if (!_stateCounter.IsClosed)
                {
                    _stateCounter.SetRelease((int)_state);
                }
                if (State.FAILED == newState)
                {
                    _asyncAeronArchive.Dispose();
                }
            }
        }

        private void OnLiveJoined()
        {
            if (!_liveJoinedCounter.IsClosed)
            {
                _liveJoinedCounter.IncrementRelease();
            }

            _listener.OnLiveJoined();
        }

        private void OnLiveLeft()
        {
            if (!_liveLeftCounter.IsClosed)
            {
                _liveLeftCounter.IncrementRelease();
            }

            _listener.OnLiveLeft();
        }

        private int DoPoll(Image image, int fragmentLimit, bool isControlled)
        {
            if (isControlled)
            {
                return image.ControlledPoll(_controlledFragmentAssembler, fragmentLimit);
            }
            return image.Poll(_uncontrolledFragmentAssembler, fragmentLimit);
        }

        private ControlledFragmentHandlerAction OnFragmentControlled(
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header)
        {
            return _controlledFragmentHandler.OnFragment(buffer, offset, length, header);
        }

        private void OnFragmentUncontrolled(
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header)
        {
            _uncontrolledFragmentHandler.OnFragment(buffer, offset, length, header);
        }

        private enum ReplayChannelType
        {
            SESSION_SPECIFIC,
            DYNAMIC_PORT,
            RESPONSE_CHANNEL
        }

        private static class ReplayChannelTypeHelper
        {
            internal static ReplayChannelType Of(ChannelUri channelUri)
            {
                if (channelUri.HasControlModeResponse())
                {
                    return ReplayChannelType.RESPONSE_CHANNEL;
                }
                if (channelUri.IsUdp)
                {
                    string endpoint = channelUri.Get(AeronClient.Context.ENDPOINT_PARAM_NAME);
                    if (null != endpoint && endpoint.EndsWith(":0", StringComparison.Ordinal))
                    {
                        return ReplayChannelType.DYNAMIC_PORT;
                    }
                }
                return ReplayChannelType.SESSION_SPECIFIC;
            }
        }

        private enum State
        {
            AWAIT_ARCHIVE_CONNECTION = 0,
            SEND_LIST_RECORDING_REQUEST = 1,
            AWAIT_LIST_RECORDING_RESPONSE = 2,
            SEND_REPLAY_REQUEST = 3,
            AWAIT_REPLAY_RESPONSE = 4,
            ADD_REPLAY_SUBSCRIPTION = 5,
            AWAIT_REPLAY_SUBSCRIPTION = 6,
            AWAIT_REPLAY_CHANNEL_ENDPOINT = 7,
            ADD_REQUEST_PUBLICATION = 8,
            AWAIT_REQUEST_PUBLICATION = 9,
            SEND_REPLAY_TOKEN_REQUEST = 10,
            AWAIT_REPLAY_TOKEN = 11,
            REPLAY = 12,
            ATTEMPT_SWITCH = 13,
            ADD_LIVE_SUBSCRIPTION = 14,
            AWAIT_LIVE = 15,
            LIVE = 16,
            FAILED = 17
        }

        private class AsyncArchiveOp
        {
            internal long CorrelationId { get; set; }
            internal long DeadlineNs { get; set; }

            internal long RelevantId { get; set; }
            internal ControlResponseCode Code { get; set; }
            internal string ErrorMessage { get; set; }

            internal bool ResponseReceived { get; set; }

            internal void Init(long correlationId, long deadlineNs)
            {
                CorrelationId = correlationId;
                DeadlineNs = deadlineNs;
                ResponseReceived = false;
            }

            internal void OnControlResponse(long relevantId, ControlResponseCode code, string errorMessage)
            {
                RelevantId = relevantId;
                Code = code;
                ErrorMessage = errorMessage;
                ResponseReceived = true;
            }
        }

        private sealed class ListRecordingRequest : AsyncArchiveOp, IRecordingDescriptorConsumer
        {
            internal int Remaining { get; set; }

            internal long RecordingId { get; set; }
            internal long StartPosition { get; set; }
            internal long StopPosition { get; set; }
            internal int TermBufferLength { get; set; }
            internal int StreamId { get; set; }

            public void OnRecordingDescriptor(
                long controlSessionId,
                long correlationId,
                long recordingId,
                long startTimestamp,
                long stopTimestamp,
                long startPosition,
                long stopPosition,
                int initialTermId,
                int segmentFileLength,
                int termBufferLength,
                int mtuLength,
                int sessionId,
                int streamId,
                string strippedChannel,
                string originalChannel,
                string sourceIdentity)
            {
                RecordingId = recordingId;
                StartPosition = startPosition;
                StopPosition = stopPosition;
                TermBufferLength = termBufferLength;
                StreamId = streamId;

                if (0 == --Remaining)
                {
                    ResponseReceived = true;
                }
            }
        }

        /// <summary>
        /// Configuration of a <c>PersistentSubscription</c> to be created.
        /// </summary>
        public sealed class Context
        {
            private int _isConcluded;
            private AeronClient _aeron;
            private bool _ownsAeronClient;
            private string _aeronDirectoryName;
            private long _recordingId = AeronClient.NULL_VALUE;
            private long _startPosition = FROM_LIVE;
            private string _liveChannel;
            private int _liveStreamId = AeronClient.NULL_VALUE;
            private string _replayChannel;
            private int _replayStreamId = AeronClient.NULL_VALUE;
            private IPersistentSubscriptionListener _listener;
            private AeronArchive.Context _aeronArchiveContext;
            private Counter _stateCounter;
            private Counter _joinDifferenceCounter;
            private Counter _liveLeftCounter;
            private Counter _liveJoinedCounter;

            /// <summary>
            /// Construct a Context using default values.
            /// </summary>
            public Context()
            {
            }

            /// <summary>
            /// Perform a shallow copy of the object.
            /// </summary>
            /// <returns> a shallow copy of the object. </returns>
            public Context Clone()
            {
                return (Context)MemberwiseClone();
            }

            /// <summary>
            /// Conclude configuration by setting up defaults when specifics are not provided.
            /// </summary>
            public void Conclude()
            {
                if (0 != Interlocked.Exchange(ref _isConcluded, 1))
                {
                    throw new ConcurrentConcludeException();
                }

                if (AeronClient.NULL_VALUE == _recordingId)
                {
                    throw new ConfigurationException("recordingId must be set");
                }

                if (AeronClient.NULL_VALUE == _liveStreamId)
                {
                    throw new ConfigurationException("liveStreamId must be set");
                }

                if (string.IsNullOrEmpty(_liveChannel))
                {
                    throw new ConfigurationException("liveChannel must be set");
                }

                if (string.IsNullOrEmpty(_replayChannel))
                {
                    throw new ConfigurationException("replayChannel must be set");
                }

                if (AeronClient.NULL_VALUE == _replayStreamId)
                {
                    throw new ConfigurationException("replayStreamId must be set");
                }

                if (null == _aeronArchiveContext)
                {
                    throw new ConfigurationException("aeronArchiveContext must be set");
                }

                if (null == _listener)
                {
                    _listener = new NoOpPersistentSubscriptionListener();
                }

                if (0 > _recordingId)
                {
                    throw new ConfigurationException("invalid recordingId " + _recordingId);
                }

                if (FROM_LIVE > _startPosition)
                {
                    throw new ConfigurationException("invalid startPosition " + _startPosition);
                }

                ChannelUri replayChannelUri = ChannelUri.Parse(_replayChannel);

                if (replayChannelUri.HasControlModeResponse())
                {
                    string controlRequestChannel = _aeronArchiveContext.ControlRequestChannel();
                    if (null != controlRequestChannel &&
                        !replayChannelUri.IsIpc == ChannelUri.Parse(controlRequestChannel).IsIpc)
                    {
                        throw new ConfigurationException(
                            "Channel media type mismatch. " +
                            "When using `control-mode=response`, the `replayChannel` media type must match" +
                            " the media type for the archive control channel.");
                    }
                }

                replayChannelUri.Put(AeronClient.Context.REJOIN_PARAM_NAME, "false");

                _replayChannel = replayChannelUri.ToString();

                if (null == _aeron)
                {
                    AeronClient.Context aeronCtx = new AeronClient.Context()
                        .ClientName("PersistentSubscription")
                        .SubscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                        .UseConductorAgentInvoker(true);
                    if (null != _aeronDirectoryName)
                    {
                        aeronCtx.AeronDirectoryName(_aeronDirectoryName);
                    }
                    _aeron = AeronClient.Connect(aeronCtx);
                    _ownsAeronClient = true;
                }

                if (null == _aeronArchiveContext.AeronClient())
                {
                    _aeronArchiveContext.AeronClient(_aeron);
                }

                AllocateMissingCounters();
            }

            private void AllocateMissingCounters()
            {
                if (null == _stateCounter)
                {
                    _stateCounter = AllocatePersistentSubscriptionCounter(
                        _aeron,
                        "Persistent Subscription State",
                        AeronCounters.PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID,
                        _replayStreamId,
                        _liveStreamId,
                        _replayChannel,
                        _liveChannel);
                }

                if (null == _joinDifferenceCounter)
                {
                    _joinDifferenceCounter = AllocatePersistentSubscriptionCounter(
                        _aeron,
                        "Persistent Subscription Join Difference",
                        AeronCounters.PERSISTENT_SUBSCRIPTION_JOIN_DIFFERENCE_TYPE_ID,
                        _replayStreamId,
                        _liveStreamId,
                        _replayChannel,
                        _liveChannel);
                }

                if (null == _liveLeftCounter)
                {
                    _liveLeftCounter = AllocatePersistentSubscriptionCounter(
                        _aeron,
                        "Persistent Subscription Live Left Count",
                        AeronCounters.PERSISTENT_SUBSCRIPTION_LIVE_LEFT_COUNT_TYPE_ID,
                        _replayStreamId,
                        _liveStreamId,
                        _replayChannel,
                        _liveChannel);
                }

                if (null == _liveJoinedCounter)
                {
                    _liveJoinedCounter = AllocatePersistentSubscriptionCounter(
                        _aeron,
                        "Persistent Subscription Live Joined Count",
                        AeronCounters.PERSISTENT_SUBSCRIPTION_LIVE_JOINED_COUNT_TYPE_ID,
                        _replayStreamId,
                        _liveStreamId,
                        _replayChannel,
                        _liveChannel);
                }
            }

            /// <summary>
            /// Has the context had the <see cref="Conclude()"/> method called.
            /// </summary>
            /// <returns> true if the <see cref="Conclude()"/> method has been called. </returns>
            public bool IsConcluded()
            {
                return 0 != Volatile.Read(ref _isConcluded);
            }

            public Context Aeron(AeronClient aeron)
            {
                _aeron = aeron;
                return this;
            }

            public AeronClient Aeron()
            {
                return _aeron;
            }

            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                _ownsAeronClient = ownsAeronClient;
                return this;
            }

            public bool OwnsAeronClient()
            {
                return _ownsAeronClient;
            }

            public Context AeronDirectoryName(string aeronDirectoryName)
            {
                _aeronDirectoryName = aeronDirectoryName;
                return this;
            }

            public string AeronDirectoryName()
            {
                return _aeronDirectoryName;
            }

            public Context RecordingId(long recordingId)
            {
                _recordingId = recordingId;
                return this;
            }

            public long RecordingId()
            {
                return _recordingId;
            }

            public Context StartPosition(long startPosition)
            {
                _startPosition = startPosition;
                return this;
            }

            public long StartPosition()
            {
                return _startPosition;
            }

            public Context LiveChannel(string liveChannel)
            {
                _liveChannel = liveChannel;
                return this;
            }

            public string LiveChannel()
            {
                return _liveChannel;
            }

            public Context LiveStreamId(int liveStreamId)
            {
                _liveStreamId = liveStreamId;
                return this;
            }

            public int LiveStreamId()
            {
                return _liveStreamId;
            }

            public Context ReplayChannel(string replayChannel)
            {
                _replayChannel = replayChannel;
                return this;
            }

            public string ReplayChannel()
            {
                return _replayChannel;
            }

            public Context ReplayStreamId(int replayStreamId)
            {
                _replayStreamId = replayStreamId;
                return this;
            }

            public int ReplayStreamId()
            {
                return _replayStreamId;
            }

            public Context Listener(IPersistentSubscriptionListener listener)
            {
                _listener = listener;
                return this;
            }

            public IPersistentSubscriptionListener Listener()
            {
                return _listener;
            }

            public Context AeronArchiveContext(AeronArchive.Context aeronArchiveContext)
            {
                _aeronArchiveContext = aeronArchiveContext;
                return this;
            }

            public AeronArchive.Context AeronArchiveContext()
            {
                return _aeronArchiveContext;
            }

            public Context StateCounter(Counter stateCounter)
            {
                _stateCounter = stateCounter;
                return this;
            }

            public Counter StateCounter()
            {
                return _stateCounter;
            }

            public Context JoinDifferenceCounter(Counter joinDifferenceCounter)
            {
                _joinDifferenceCounter = joinDifferenceCounter;
                return this;
            }

            public Counter JoinDifferenceCounter()
            {
                return _joinDifferenceCounter;
            }

            public Context LiveLeftCounter(Counter liveLeftCounter)
            {
                _liveLeftCounter = liveLeftCounter;
                return this;
            }

            public Counter LiveLeftCounter()
            {
                return _liveLeftCounter;
            }

            public Context LiveJoinedCounter(Counter liveJoinedCounter)
            {
                _liveJoinedCounter = liveJoinedCounter;
                return this;
            }

            public Counter LiveJoinedCounter()
            {
                return _liveJoinedCounter;
            }

            /// <summary>
            /// Close the context and free applicable resources. If <see cref="OwnsAeronClient()"/> is true the
            /// <see cref="Aeron()"/> client will be closed.
            /// </summary>
            public void Dispose()
            {
                if (_ownsAeronClient)
                {
                    CloseHelper.QuietDispose(_aeron);
                }
                else if (null != _aeron && !_aeron.IsClosed)
                {
                    CloseHelper.QuietDispose(_stateCounter);
                    CloseHelper.QuietDispose(_joinDifferenceCounter);
                    CloseHelper.QuietDispose(_liveLeftCounter);
                    CloseHelper.QuietDispose(_liveJoinedCounter);
                }
            }
        }

        private sealed class NoOpPersistentSubscriptionListener : IPersistentSubscriptionListener
        {
            public void OnLiveJoined()
            {
            }

            public void OnLiveLeft()
            {
            }

            public void OnError(Exception e)
            {
            }
        }

        private sealed class MaxRecordedPosition : AsyncArchiveOp
        {
            private enum MaxRecordedPositionState
            {
                REQUEST_MAX_POSITION,
                AWAIT_MAX_POSITION,
                RECHECK_REQUIRED
            }

            private readonly PersistentSubscription _parent;
            private MaxRecordedPositionState _state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
            private long _maxRecordedPosition;
            private int _closeEnoughThreshold;

            internal MaxRecordedPosition(PersistentSubscription parent)
            {
                _parent = parent;
            }

            internal void Reset(int closeEnoughThreshold)
            {
                _closeEnoughThreshold = closeEnoughThreshold;
                _state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
            }

            internal bool IsCaughtUp(long replayedPosition)
            {
                switch (_state)
                {
                    case MaxRecordedPositionState.REQUEST_MAX_POSITION:
                        return RequestMaxPosition();
                    case MaxRecordedPositionState.AWAIT_MAX_POSITION:
                        return AwaitMaxPosition(replayedPosition);
                    case MaxRecordedPositionState.RECHECK_REQUIRED:
                        return RecheckRequired(replayedPosition);
                    default:
                        return false;
                }
            }

            private bool RequestMaxPosition()
            {
                long correlationId = _parent._aeron.NextCorrelationId();
                if (_parent._asyncAeronArchive.TrySendMaxRecordedPositionRequest(correlationId, _parent._recordingId))
                {
                    Init(correlationId, _parent._nanoClock.NanoTime() + _parent._messageTimeoutNs);
                    _state = MaxRecordedPositionState.AWAIT_MAX_POSITION;
                }
                return false;
            }

            private bool AwaitMaxPosition(long replayedPosition)
            {
                if (ResponseReceived)
                {
                    if (ControlResponseCode.OK == Code)
                    {
                        _maxRecordedPosition = RelevantId;
                        if (CloseEnoughToSwitch(replayedPosition, _maxRecordedPosition))
                        {
                            return true;
                        }
                        else
                        {
                            _state = MaxRecordedPositionState.RECHECK_REQUIRED;
                            return false;
                        }
                    }
                    else
                    {
                        ArchiveException archiveException = new ArchiveException(
                            "get max position request failed code=" + Code + " relevantId=" + RelevantId +
                            " errorMessage='" + ErrorMessage + "'");
                        _parent.SetState(State.FAILED);
                        _parent.OnTerminalError(archiveException);
                    }
                }
                else
                {
                    if (DeadlineNs - _parent._nanoClock.NanoTime() < 0)
                    {
                        _state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
                    }
                }
                return false;
            }

            private bool RecheckRequired(long replayedPosition)
            {
                if (CloseEnoughToReCheck(replayedPosition))
                {
                    _state = MaxRecordedPositionState.REQUEST_MAX_POSITION;
                }
                return false;
            }

            private bool CloseEnoughToSwitch(long replayedPosition, long maxRecordedPosition)
            {
                return replayedPosition >= maxRecordedPosition - _closeEnoughThreshold;
            }

            private bool CloseEnoughToReCheck(long replayedPosition)
            {
                return replayedPosition >= _maxRecordedPosition;
            }
        }

        private sealed class ArchiveListener : IAsyncAeronArchiveListener
        {
            private readonly PersistentSubscription _parent;

            internal ArchiveListener(PersistentSubscription parent)
            {
                _parent = parent;
            }

            public void OnConnected()
            {
            }

            public void OnDisconnected()
            {
                if (State.AWAIT_ARCHIVE_CONNECTION == _parent._state ||
                    State.ATTEMPT_SWITCH == _parent._state ||
                    State.LIVE == _parent._state ||
                    State.FAILED == _parent._state)
                {
                    return;
                }

                Image replayImage = _parent._replayImage;
                if (null != replayImage)
                {
                    _parent._position = replayImage.Position;
                }

                _parent.CleanUpRequestPublication();
                _parent.CleanUpLiveSubscription();
                _parent.CleanUpReplay();
                _parent.CleanUpReplaySubscription();

                _parent.SetState(State.AWAIT_ARCHIVE_CONNECTION);
            }

            public void OnControlResponse(
                long correlationId,
                long relevantId,
                ControlResponseCode code,
                string errorMessage)
            {
                if (correlationId == _parent._maxRecordedPosition.CorrelationId)
                {
                    _parent._maxRecordedPosition.OnControlResponse(relevantId, code, errorMessage);
                }
                else if (correlationId == _parent._listRecordingRequest.CorrelationId)
                {
                    _parent._listRecordingRequest.OnControlResponse(relevantId, code, errorMessage);
                }
                else if (correlationId == _parent._replayRequest.CorrelationId)
                {
                    _parent._replayRequest.OnControlResponse(relevantId, code, errorMessage);
                }
                else if (correlationId == _parent._replayTokenRequest.CorrelationId)
                {
                    _parent._replayTokenRequest.OnControlResponse(relevantId, code, errorMessage);
                }
            }

            public void OnError(Exception error)
            {
                if (_parent._asyncAeronArchive.IsClosed)
                {
                    _parent.SetState(State.FAILED);
                    _parent.OnTerminalError(error);
                }
                else
                {
                    _parent._listener.OnError(error);
                }
            }

            public void OnRecordingDescriptor(
                long controlSessionId,
                long correlationId,
                long recordingId,
                long startTimestamp,
                long stopTimestamp,
                long startPosition,
                long stopPosition,
                int initialTermId,
                int segmentFileLength,
                int termBufferLength,
                int mtuLength,
                int sessionId,
                int streamId,
                string strippedChannel,
                string originalChannel,
                string sourceIdentity)
            {
                if (correlationId == _parent._listRecordingRequest.CorrelationId)
                {
                    _parent._listRecordingRequest.OnRecordingDescriptor(
                        controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity);
                }
            }
        }

        private sealed class InlineControlledFragmentHandler : IControlledFragmentHandler
        {
            private readonly Func<IDirectBuffer, int, int, Header, ControlledFragmentHandlerAction> _handler;

            internal InlineControlledFragmentHandler(
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

        private sealed class InlineFragmentHandler : IFragmentHandler
        {
            private readonly Action<IDirectBuffer, int, int, Header> _handler;

            internal InlineFragmentHandler(Action<IDirectBuffer, int, int, Header> handler)
            {
                _handler = handler;
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _handler(buffer, offset, length, header);
            }
        }

        private static Counter AllocatePersistentSubscriptionCounter(
            AeronClient aeron,
            string name,
            int typeId,
            int replayStreamId,
            int liveStreamId,
            string replayChannel,
            string liveChannel)
        {
            string label =
                name + ": " + replayStreamId + " " + replayChannel + " " + liveStreamId + " " + liveChannel;

            return aeron.AddCounter(typeId, label);
        }
    }
}
