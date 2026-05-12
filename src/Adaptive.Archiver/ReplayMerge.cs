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
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.Aeron;
using static Adaptive.Aeron.Aeron.Context;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Replay a recorded stream from a starting position and merge with live stream for a full history of a stream.
    /// <para>
    /// Once constructed either of <seealso cref="Poll(FragmentHandler, int)"/> or <seealso cref="DoWork()"/>,
    /// interleaved with consumption of the <seealso cref="Image()"/>, should be called in a duty cycle loop until
    /// <seealso cref="Merged"/> is {@code true}. After which the <seealso cref="ReplayMerge"/> can be closed and
    /// continued usage can be made of the
    /// <seealso cref="Image"/> or its
    /// parent <seealso cref="Subscription"/>. If an exception occurs or progress stops, the merge will fail and
    /// <seealso cref="HasFailed()"/> will be {@code true}.
    /// </para>
    /// <para>
    /// If the endpoint on the replay destination uses a port of 0, then the OS will assign a port from the ephemeral
    /// range and this will be added to the replay channel for instructing the archive.
    /// </para>
    /// <para>
    /// NOTE: Merging is only supported with UDP streams.
    ///</para>
    /// <para>
    /// NOTE: ReplayMerge is not threadsafe and should <b>not</b> be used with a shared {@link AeronArchive} client.
    /// </para>
    /// </summary>
    public class ReplayMerge : IDisposable
    {
        /// <summary>
        /// The maximum window at which a live destination should be added when trying to merge.
        /// </summary>
        public const int LIVE_ADD_MAX_WINDOW = 32 * 1024 * 1024;

        private const int ReplayRemoveThreshold = 0;
        private static readonly long MergeProgressTimeoutDefaultMs = 5_000;
        private static readonly long InitialGetMaxRecordedPositionBackoffMs = 8;
        private static readonly long GetMaxRecordedPositionBackoffMaxMs = 500;
        private static readonly long ArchivePollIntervalMs = 100;

        internal enum State
        {
            RESOLVE_REPLAY_PORT,
            GET_RECORDING_POSITION,
            REPLAY,
            CATCHUP,
            ATTEMPT_LIVE_JOIN,
            MERGED,
            FAILED,
            CLOSED,
        }

        private readonly long _recordingId;
        private readonly long _startPosition;
        private readonly long _mergeProgressTimeoutMs;
        private long _replaySessionId = NULL_VALUE;
        private long _activeCorrelationId = NULL_VALUE;
        private long _nextTargetPosition = NULL_VALUE;
        private long _positionOfLastProgress = NULL_VALUE;
        private long _timeOfLastProgressMs;
        private long _timeOfNextGetMaxRecordedPositionMs;
        private long _getMaxRecordedPositionBackoffMs = InitialGetMaxRecordedPositionBackoffMs;
        private long _timeOfLastScheduledArchivePollMs;
        private bool _isLiveAdded = false;
        private bool _isReplayActive = false;
        private State _state;
        private Image _image;

        private readonly AeronArchive _archive;
        private readonly Subscription _subscription;
        private readonly IEpochClock _epochClock;
        private readonly string _replayDestination;
        private readonly string _liveDestination;
        private readonly ChannelUri _replayChannelUri;

        /// <summary>
        /// Create a <seealso cref="ReplayMerge"/> to manage the merging of a replayed stream and switching over to live
        /// stream as appropriate.
        /// </summary>
        /// <param name="subscription"> to use for the replay and live stream. Must be a multi-destination subscription.
        /// </param>
        /// <param name="archive">                to use for the replay. </param>
        /// <param name="replayChannel">          to as a template for what the archive will use. </param>
        /// <param name="replayDestination"> to send the replay to and the destination added by the
        /// <seealso cref="Subscription"/>. </param>
        /// <param name="liveDestination"> for the live stream and the destination added by the
        /// <seealso cref="Subscription"/>. </param>
        /// <param name="recordingId">            for the replay. </param>
        /// <param name="startPosition">          for the replay. </param>
        /// <param name="epochClock">             to use for progress checks. </param>
        /// <param name="mergeProgressTimeoutMs"> to use for progress checks. </param>
        public ReplayMerge(
            Subscription subscription,
            AeronArchive archive,
            string replayChannel,
            string replayDestination,
            string liveDestination,
            long recordingId,
            long startPosition,
            IEpochClock epochClock,
            long mergeProgressTimeoutMs
        )
        {
            if (
                subscription.Channel.StartsWith(IPC_CHANNEL)
                || replayChannel.StartsWith(IPC_CHANNEL, StringComparison.Ordinal)
                || replayDestination.StartsWith(IPC_CHANNEL, StringComparison.Ordinal)
                || liveDestination.StartsWith(IPC_CHANNEL, StringComparison.Ordinal)
            )
            {
                throw new ArgumentException("IPC merging is not supported");
            }

            if (!subscription.Channel.Contains("control-mode=manual"))
            {
                throw new ArgumentException(
                    "Subscription URI must have 'control-mode=manual' uri=" + subscription.Channel
                );
            }

            this._archive = archive;
            this._subscription = subscription;
            this._epochClock = epochClock;
            this._replayDestination = replayDestination;
            this._liveDestination = liveDestination;
            this._recordingId = recordingId;
            this._startPosition = startPosition;
            this._mergeProgressTimeoutMs = mergeProgressTimeoutMs;

            _replayChannelUri = ChannelUri.Parse(replayChannel);
            _replayChannelUri.Put(LINGER_PARAM_NAME, "0");
            _replayChannelUri.Put(EOS_PARAM_NAME, "false");

            var replayEndpoint = ChannelUri.Parse(replayDestination).Get(ENDPOINT_PARAM_NAME);
            if (replayEndpoint.EndsWith(":0", StringComparison.Ordinal))
            {
                _state = State.RESOLVE_REPLAY_PORT;
            }
            else
            {
                _replayChannelUri.Put(ENDPOINT_PARAM_NAME, replayEndpoint);
                _state = State.GET_RECORDING_POSITION;
            }

            subscription.AsyncAddDestination(replayDestination);
            _timeOfLastProgressMs = _timeOfNextGetMaxRecordedPositionMs = epochClock.Time();
        }

        /// <summary>
        /// Create a <seealso cref="ReplayMerge"/> to manage the merging of a replayed stream and switching over to live
        /// stream as appropriate.
        /// </summary>
        /// <param name="subscription"> to use for the replay and live stream. Must be a multi-destination subscription.
        /// </param>
        /// <param name="archive">           to use for the replay. </param>
        /// <param name="replayChannel">     to use as a template for what the archive will use. </param>
        /// <param name="replayDestination"> to send the replay to and the destination added by the
        /// <seealso cref="Subscription"/>. </param>
        /// <param name="liveDestination"> for the live stream and the destination added by the
        /// <seealso cref="Subscription"/>. </param>
        /// <param name="recordingId">       for the replay. </param>
        /// <param name="startPosition">     for the replay. </param>
        public ReplayMerge(
            Subscription subscription,
            AeronArchive archive,
            string replayChannel,
            string replayDestination,
            string liveDestination,
            long recordingId,
            long startPosition
        )
            : this(
                subscription,
                archive,
                replayChannel,
                replayDestination,
                liveDestination,
                recordingId,
                startPosition,
                archive.Ctx().AeronClient().Ctx.EpochClock(),
                MergeProgressTimeoutDefaultMs
            ) { }

        /// <summary>
        /// Close and stop any active replay. Will remove the replay destination from the subscription. This operation
        /// Will NOT remove the live destination if it has been added, so it can be used for live consumption.
        /// </summary>
        public void Dispose()
        {
            State state = this._state;
            if (State.CLOSED != state)
            {
                if (!_archive.Ctx().AeronClient().IsClosed)
                {
                    if (State.MERGED != state)
                    {
                        _subscription.AsyncRemoveDestination(_replayDestination);
                    }

                    if (_isReplayActive && _archive.Proxy().Pub().IsConnected)
                    {
                        StopReplay();
                    }
                }

                SetState(State.CLOSED);
            }
        }

        /// <summary>
        /// Get the <seealso cref="Subscription"/> used to consume the replayed and merged stream.
        /// </summary>
        /// <returns> the <seealso cref="Subscription"/> used to consume the replayed and merged stream. </returns>
        public Subscription Subscription()
        {
            return _subscription;
        }

        /// <summary>
        /// Perform the work of replaying and merging. Should only be used if polling the underlying
        /// <seealso cref="Image"/> directly,
        /// call <seealso cref="Poll(FragmentHandler, int)"/> on this class.
        /// </summary>
        /// <returns> indication of work done processing the merge. </returns>
        public int DoWork()
        {
            int workCount = 0;
            long nowMs = _epochClock.Time();

            try
            {
                switch (_state)
                {
                    case State.RESOLVE_REPLAY_PORT:
                        workCount += ResolveReplayPort(nowMs);
                        CheckProgress(nowMs);
                        break;

                    case State.GET_RECORDING_POSITION:
                        workCount += GetRecordingPosition(nowMs);
                        CheckProgress(nowMs);
                        break;

                    case State.REPLAY:
                        workCount += Replay(nowMs);
                        CheckProgress(nowMs);
                        break;

                    case State.CATCHUP:
                        workCount += Catchup(nowMs);
                        CheckProgress(nowMs);
                        break;

                    case State.ATTEMPT_LIVE_JOIN:
                        workCount += AttemptLiveJoin(nowMs);
                        CheckProgress(nowMs);
                        break;

                    case State.MERGED:
                    case State.CLOSED:
                    case State.FAILED:
                        break;
                }
            }
            catch (Exception)
            {
                SetState(State.FAILED);
                throw;
            }

            return workCount;
        }

        /// <summary>
        /// Poll the <seealso cref="Image"/> used for replay and merging and live stream. The
        /// <seealso cref="ReplayMerge.DoWork()"/> method
        /// will be called before the poll so that processing of the merge can be done.
        /// </summary>
        /// <param name="fragmentHandler"> to call for fragments. </param>
        /// <param name="fragmentLimit">   for poll call. </param>
        /// <returns> number of fragments processed. </returns>
        public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
        {
            DoWork();
            return null == _image ? 0 : _image.Poll(fragmentHandler, fragmentLimit);
        }

        /// <summary>
        /// Is the live stream merged and the replay stopped?
        /// </summary>
        /// <returns> true if live stream is merged and the replay stopped or false if not. </returns>
        public bool Merged
        {
            get { return _state == State.MERGED; }
        }

        /// <summary>
        /// Has the replay merge failed due to an error?
        /// </summary>
        /// <returns> true if replay merge has failed due to an error. </returns>
        public bool HasFailed()
        {
            return _state == State.FAILED;
        }

        /// <summary>
        /// The <seealso cref="Image"/> which is a merge of the replay and live stream.
        /// </summary>
        /// <returns> the <seealso cref="Image"/> which is a merge of the replay and live stream. </returns>
        public Image Image()
        {
            return _image;
        }

        /// <summary>
        /// Is the live destination added to the <seealso cref="Subscription()"/>?
        /// </summary>
        /// <returns> true if live destination added or false if not. </returns>
        public bool LiveAdded
        {
            get { return _isLiveAdded; }
        }

        private int ResolveReplayPort(long nowMs)
        {
            int workCount = 0;

            string resolvedEndpoint = _subscription.ResolvedEndpoint;
            if (null != resolvedEndpoint)
            {
                _replayChannelUri.ReplaceEndpointWildcardPort(resolvedEndpoint);

                _timeOfLastProgressMs = nowMs;
                SetState(State.GET_RECORDING_POSITION);
                workCount += 1;
            }

            return workCount;
        }

        private int GetRecordingPosition(long nowMs)
        {
            int workCount = 0;

            if (NULL_VALUE == _activeCorrelationId)
            {
                if (CallGetMaxRecordedPosition(nowMs))
                {
                    _timeOfLastProgressMs = nowMs;
                    workCount += 1;
                }
            }
            else if (PollForResponse(_archive, _activeCorrelationId))
            {
                _nextTargetPosition = PolledRelevantId(_archive);
                _activeCorrelationId = NULL_VALUE;

                if (AeronArchive.NULL_POSITION != _nextTargetPosition)
                {
                    _timeOfLastProgressMs = nowMs;
                    SetState(State.REPLAY);
                }

                workCount += 1;
            }

            return workCount;
        }

        private int Replay(long nowMs)
        {
            int workCount = 0;

            if (NULL_VALUE == _activeCorrelationId)
            {
                long correlationId = _archive.Ctx().AeronClient().NextCorrelationId();

                if (
                    _archive
                        .Proxy()
                        .Replay(
                            _recordingId,
                            _startPosition,
                            long.MaxValue,
                            _replayChannelUri.ToString(),
                            _subscription.StreamId,
                            correlationId,
                            _archive.ControlSessionId()
                        )
                )
                {
                    _activeCorrelationId = correlationId;
                    _timeOfLastProgressMs = nowMs;
                    workCount += 1;
                }
            }
            else if (PollForResponse(_archive, _activeCorrelationId))
            {
                _isReplayActive = true;
                _replaySessionId = PolledRelevantId(_archive);
                _timeOfLastProgressMs = nowMs;
                _activeCorrelationId = NULL_VALUE;

                // reset getRecordingPosition backoff when moving to CATCHUP state
                _getMaxRecordedPositionBackoffMs = InitialGetMaxRecordedPositionBackoffMs;
                _timeOfNextGetMaxRecordedPositionMs = nowMs;

                SetState(State.CATCHUP);
                workCount += 1;
            }

            return workCount;
        }

        private int Catchup(long nowMs)
        {
            int workCount = 0;

            if (null == _image && _subscription.IsConnected)
            {
                _timeOfLastProgressMs = nowMs;
                Image image = _subscription.ImageBySessionId((int)_replaySessionId);

                if (null == this._image && null != image)
                {
                    this._image = image;
                    _positionOfLastProgress = image.Position;
                }
                else
                {
                    _positionOfLastProgress = NULL_VALUE;
                }
            }

            if (null != _image)
            {
                long position = _image.Position;
                if (position >= _nextTargetPosition)
                {
                    _timeOfLastProgressMs = nowMs;
                    _positionOfLastProgress = position;
                    SetState(State.ATTEMPT_LIVE_JOIN);
                    workCount += 1;
                }
                else if (position > _positionOfLastProgress)
                {
                    _timeOfLastProgressMs = nowMs;
                    _positionOfLastProgress = position;
                }
                else if (_image.Closed)
                {
                    throw new InvalidOperationException("ReplayMerge Image closed unexpectedly.");
                }
            }

            return workCount;
        }

        private int AttemptLiveJoin(long nowMs)
        {
            int workCount = 0;

            if (NULL_VALUE == _activeCorrelationId)
            {
                if (CallGetMaxRecordedPosition(nowMs))
                {
                    _timeOfLastProgressMs = nowMs;
                    workCount += 1;
                }
            }
            else if (PollForResponse(_archive, _activeCorrelationId))
            {
                _nextTargetPosition = PolledRelevantId(_archive);
                _activeCorrelationId = NULL_VALUE;

                if (AeronArchive.NULL_POSITION != _nextTargetPosition)
                {
                    State nextState = State.CATCHUP;

                    if (null != _image)
                    {
                        long position = _image.Position;
                        if (ShouldAddLiveDestination(position))
                        {
                            _subscription.AsyncAddDestination(_liveDestination);
                            _timeOfLastProgressMs = nowMs;
                            _positionOfLastProgress = position;
                            _isLiveAdded = true;
                        }
                        else if (ShouldStopAndRemoveReplay(position))
                        {
                            _subscription.AsyncRemoveDestination(_replayDestination);
                            StopReplay();
                            _timeOfLastProgressMs = nowMs;
                            _positionOfLastProgress = position;
                            nextState = State.MERGED;
                        }
                    }

                    SetState(nextState);
                }

                workCount += 1;
            }

            return workCount;
        }

        private bool CallGetMaxRecordedPosition(long nowMs)
        {
            if (nowMs < _timeOfNextGetMaxRecordedPositionMs)
            {
                return false;
            }

            long correlationId = _archive.Ctx().AeronClient().NextCorrelationId();

            bool result = _archive
                .Proxy()
                .GetMaxRecordedPosition(_recordingId, correlationId, _archive.ControlSessionId());

            if (result)
            {
                _activeCorrelationId = correlationId;
            }

            // increase backoff regardless of result
            _getMaxRecordedPositionBackoffMs = Math.Min(
                _getMaxRecordedPositionBackoffMs * 2,
                GetMaxRecordedPositionBackoffMaxMs
            );
            _timeOfNextGetMaxRecordedPositionMs = nowMs + _getMaxRecordedPositionBackoffMs;

            return result;
        }

        private void StopReplay()
        {
            long correlationId = _archive.Ctx().AeronClient().NextCorrelationId();
            if (_archive.Proxy().StopReplay(_replaySessionId, correlationId, _archive.ControlSessionId()))
            {
                _isReplayActive = false;
            }
        }

        private void SetState(ReplayMerge.State newState)
        {
            //System.out.println(state + " -> " + newState);
            _state = newState;
            _activeCorrelationId = NULL_VALUE;
        }

        private bool ShouldAddLiveDestination(long position)
        {
            return !_isLiveAdded
                && (_nextTargetPosition - position) <= Math.Min(_image.TermBufferLength >> 2, LIVE_ADD_MAX_WINDOW);
        }

        private bool ShouldStopAndRemoveReplay(long position)
        {
            return _isLiveAdded
                && (_nextTargetPosition - position) <= ReplayRemoveThreshold
                && _image.ActiveTransportCount() >= 2;
        }

        private void CheckProgress(long nowMs)
        {
            if (nowMs > (_timeOfLastProgressMs + _mergeProgressTimeoutMs))
            {
                int transportCount = _image?.ActiveTransportCount() ?? 0;
                throw new TimeoutException(
                    "ReplayMerge no progress: state=" + _state + ", activeTransportCount=" + transportCount
                );
            }

            if (
                NULL_VALUE == _activeCorrelationId
                && (nowMs > (_timeOfLastScheduledArchivePollMs + ArchivePollIntervalMs))
            )
            {
                _timeOfLastScheduledArchivePollMs = nowMs;
                PollForResponse(_archive, NULL_VALUE);
            }
        }

        private static bool PollForResponse(AeronArchive archive, long correlationId)
        {
            ControlResponsePoller poller = archive.ControlResponsePoller();
            int pollCount = poller.Poll();
            if (poller.PollComplete)
            {
                if (poller.ControlSessionId() == archive.ControlSessionId())
                {
                    if (poller.Code() == ControlResponseCode.ERROR)
                    {
                        throw new ArchiveException(
                            "archive response for correlationId="
                                + poller.CorrelationId()
                                + ", error: "
                                + poller.ErrorMessage(),
                            (int)poller.RelevantId(),
                            poller.CorrelationId()
                        );
                    }

                    return poller.CorrelationId() == correlationId;
                }
            }
            else if (pollCount == 0 && !poller.Subscription().IsConnected)
            {
                throw new ArchiveException("archive is not connected");
            }

            return false;
        }

        private static long PolledRelevantId(AeronArchive archive)
        {
            return archive.ControlResponsePoller().RelevantId();
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "ReplayMerge{"
                + "state="
                + _state
                + ", nextTargetPosition="
                + _nextTargetPosition
                + ", timeOfLastProgressMs="
                + _timeOfLastProgressMs
                + ", positionOfLastProgress="
                + _positionOfLastProgress
                + ", isLiveAdded="
                + _isLiveAdded
                + ", isReplayActive="
                + _isReplayActive
                + ", replayChannelUri="
                + _replayChannelUri
                + ", image="
                + _image
                + '}';
        }
    }
}
