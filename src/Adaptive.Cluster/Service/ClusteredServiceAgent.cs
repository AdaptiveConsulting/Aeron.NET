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
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver;
using Adaptive.Archiver.Codecs;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using static Adaptive.Aeron.Aeron;
using static Adaptive.Archiver.AeronArchive;
using static Adaptive.Cluster.Client.AeronCluster;
using static Adaptive.Cluster.Service.ClusteredServiceContainer;
using MessageHeaderEncoder = Adaptive.Cluster.Codecs.MessageHeaderEncoder;

namespace Adaptive.Cluster.Service
{
    internal sealed class ClusteredServiceAgent : IAgent, ICluster, IIdleStrategy
    {
        static long s_oneMillisecondNs = Agrona.TimeUnit.MILLIS.ToNanos(1);

        static long s_markFileUpdateIntervalMs = Agrona.TimeUnit.NANOSECONDS.ToMillis(
            ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS
        );

        private static readonly int MaxUdpPayloadLength = 65504;

        const int LifecycleCallbackNone = 0;
        const int LifecycleCallbackOnStart = 1;
        const int LifecycleCallbackOnTerminate = 2;
        const int LifecycleCallbackOnRoleChange = 3;
        const int LifecycleCallbackDoBackgroundWork = 4;

        static String LifecycleName(int activeLifecycleCallback)
        {
            switch (activeLifecycleCallback)
            {
                case LifecycleCallbackNone:
                    return "none";
                case LifecycleCallbackOnStart:
                    return "onStart";
                case LifecycleCallbackOnTerminate:
                    return "onTerminate";
                case LifecycleCallbackOnRoleChange:
                    return "onRoleChange";
                case LifecycleCallbackDoBackgroundWork:
                    return "doBackgroundWork";
                default:
                    return "unknown";
            }
        }

        private int _activeLifecycleCallback;

        private volatile bool _isAbort;
        private bool _isServiceActive;
        private readonly int _serviceId;
        private int _memberId = NULL_VALUE;
        private long _closeHandlerRegistrationid;
        private long _ackId = 0;
        private long _terminationPosition = NULL_POSITION;
        private long _markFileUpdateDeadlineMs;
        private long _lastSlowTickNs;
        private long _clusterTime;
        private long _logPosition = NULL_POSITION;

        private readonly IIdleStrategy _idleStrategy;
        private readonly ClusterMarkFile _markFile;
        private readonly ClusteredServiceContainer.Context _ctx;
        private readonly Aeron.Aeron _aeron;
        private readonly AgentInvoker _aeronAgentInvoker;
        private readonly IClusteredService _service;
        private readonly ConsensusModuleProxy _consensusModuleProxy;
        private readonly ServiceAdapter _serviceAdapter;
        private readonly IEpochClock _epochClock;
        private readonly INanoClock _nanoClock;
        private readonly UnsafeBuffer _messageBuffer = new UnsafeBuffer(new byte[MaxUdpPayloadLength]);
        private readonly UnsafeBuffer _headerBuffer; // set in constructor
        private readonly DirectBufferVector _headerVector; // set in constructor
        private readonly SessionMessageHeaderEncoder _sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private readonly List<ContainerClientSession> _sessions = new List<ContainerClientSession>();
        private readonly Map<long, IClientSession> _sessionByIdMap = new Map<long, IClientSession>();
        private readonly BoundedLogAdapter _logAdapter;
        private readonly DutyCycleTracker _dutyCycleTracker;
        private readonly SnapshotDurationTracker _snapshotDurationTracker;
        private readonly String _subscriptionAlias;
        private readonly int _standbySnapshotFlags;

        private ReadableCounter _commitPosition;
        private ActiveLogEvent _activeLogEvent;
        private ClusterRole _role = ClusterRole.Follower;
        private ClusterTimeUnit _timeUnit = ClusterTimeUnit.NULL_VALUE;
        private long _requestedAckPosition = NULL_POSITION;

        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            _headerBuffer = new UnsafeBuffer(
                _messageBuffer,
                DataHeaderFlyweight.HEADER_LENGTH,
                MaxUdpPayloadLength - DataHeaderFlyweight.HEADER_LENGTH
            );
            _headerVector = new DirectBufferVector(_headerBuffer, 0, SESSION_HEADER_LENGTH);

            _logAdapter = new BoundedLogAdapter(this, ctx.LogFragmentLimit());
            this._ctx = ctx;

            _markFile = ctx.ClusterMarkFile();
            _aeron = ctx.AeronClient();
            _aeronAgentInvoker = ctx.AeronClient().ConductorAgentInvoker;
            _service = ctx.ClusteredService();
            _idleStrategy = ctx.IdleStrategy();
            _serviceId = ctx.ServiceId();
            _epochClock = ctx.EpochClock();
            _nanoClock = ctx.NanoClock();
            _dutyCycleTracker = ctx.DutyCycleTracker();
            _snapshotDurationTracker = ctx.SnapshotDurationTracker();
            _subscriptionAlias = "log-sc-" + ctx.ServiceId();

            var channel = ctx.ControlChannel();
            _consensusModuleProxy = new ConsensusModuleProxy(
                _aeron.AddPublication(channel, ctx.ConsensusModuleStreamId())
            );
            _serviceAdapter = new ServiceAdapter(_aeron.AddSubscription(channel, ctx.ServiceStreamId()), this);
            _sessionMessageHeaderEncoder.WrapAndApplyHeader(_headerBuffer, 0, new MessageHeaderEncoder());
            this._standbySnapshotFlags = ctx.StandbySnapshotEnabled()
                ? CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT
                : CLUSTER_ACTION_FLAGS_DEFAULT;
        }

        public void OnStart()
        {
            _closeHandlerRegistrationid = _aeron.AddCloseHandler(Abort);
            _aeron.AddUnavailableCounterHandler(CounterUnavailable);
            CountersReader counters = _aeron.CountersReader;
            _commitPosition = AwaitCommitPositionCounter(counters, _ctx.ClusterId());

            RecoverState(counters);
            _dutyCycleTracker.Update(_nanoClock.NanoTime());
            _isServiceActive = true;
        }

        public void OnClose()
        {
            _aeron.RemoveCloseHandler(_closeHandlerRegistrationid);

            if (_isAbort)
            {
                _ctx.AbortLatch().Signal();
            }
            else
            {
                ErrorHandler errorHandler = _ctx.CountedErrorHandler().OnError;

                if (_isServiceActive)
                {
                    _isServiceActive = false;
                    try
                    {
                        _service.OnTerminate(this);
                    }
                    catch (Exception ex)
                    {
                        errorHandler(ex);
                    }
                }

                CloseHelper.Dispose(errorHandler, _logAdapter);

                if (!_ctx.OwnsAeronClient() && !_aeron.IsClosed)
                {
                    CloseHelper.Dispose(errorHandler, _serviceAdapter);
                    CloseHelper.Dispose(errorHandler, _consensusModuleProxy);
                    DisconnectEgress(errorHandler);
                }
            }

            _markFile.UpdateActivityTimestamp(NULL_VALUE);
            _markFile.Force();
            _ctx.Dispose();
        }

        public int DoWork()
        {
            int workCount = 0;

            long nowNs = _nanoClock.NanoTime();
            _dutyCycleTracker.MeasureAndUpdate(_nanoClock.NanoTime());

            try
            {
                if (CheckForClockTick(nowNs))
                {
                    workCount += PollServiceAdapter();
                }

                if (null != _logAdapter.Image())
                {
                    int polled = _logAdapter.Poll(_commitPosition.Get());
                    workCount += polled;

                    if (0 == polled && _logAdapter.IsDone())
                    {
                        CloseLog();
                    }
                }

                workCount += InvokeBackgroundWork(nowNs);
            }
            catch (AgentTerminationException)
            {
                RunTerminationHook();
                throw;
            }

            return workCount;
        }

        public string RoleName() => _ctx.ServiceName();

        public ClusterRole Role
        {
            get => _role;
            private set
            {
                if (value != _role)
                {
                    _role = value;
                    _activeLifecycleCallback = LifecycleCallbackOnRoleChange;
                    try
                    {
                        _service.OnRoleChange(value);
                    }
                    finally
                    {
                        _activeLifecycleCallback = LifecycleCallbackNone;
                    }
                }
            }
        }

        public int MemberId => _memberId;

        public Aeron.Aeron Aeron => _aeron;

        public ClusteredServiceContainer.Context Context => _ctx;

        public IClientSession GetClientSession(long clusterSessionId)
        {
            return _sessionByIdMap.Get(clusterSessionId);
        }

        public ICollection<IClientSession> ClientSessions => _sessionByIdMap.Values;

        public void ForEachClientSession(Action<IClientSession> action)
        {
            foreach (var clientSession in _sessions)
            {
                action(clientSession);
            }
        }

        public bool CloseClientSession(long clusterSessionId)
        {
            CheckForValidInvocation();

            if (!_sessionByIdMap.ContainsKey(clusterSessionId))
            {
                throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
            }

            ContainerClientSession clientSession = (ContainerClientSession)_sessionByIdMap.Get(clusterSessionId);

            if (clientSession.IsClosing)
            {
                return true;
            }

            int attempts = 3;
            do
            {
                if (_consensusModuleProxy.CloseSession(clusterSessionId))
                {
                    clientSession.MarkClosing();
                    return true;
                }

                Idle();
            } while (--attempts > 0);

            return false;
        }

        public ClusterTimeUnit TimeUnit()
        {
            return _timeUnit;
        }

        public long Time => _clusterTime;

        public long LogPosition()
        {
            return _logPosition;
        }

        public bool ScheduleTimer(long correlationId, long deadline)
        {
            CheckForValidInvocation();

            return _consensusModuleProxy.ScheduleTimer(correlationId, deadline);
        }

        public bool CancelTimer(long correlationId)
        {
            CheckForValidInvocation();

            return _consensusModuleProxy.CancelTimer(correlationId);
        }

        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            CheckForValidInvocation();
            _sessionMessageHeaderEncoder.ClusterSessionId(Context.ServiceId());

            return _consensusModuleProxy.Offer(_headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
        }

        public long Offer(DirectBufferVector[] vectors)
        {
            CheckForValidInvocation();
            _sessionMessageHeaderEncoder.ClusterSessionId(Context.ServiceId());
            vectors[0] = _headerVector;

            return _consensusModuleProxy.Offer(vectors);
        }

        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            CheckForValidInvocation();
            _sessionMessageHeaderEncoder.ClusterSessionId(Context.ServiceId());

            return _consensusModuleProxy.TryClaim(length + SESSION_HEADER_LENGTH, bufferClaim, _headerBuffer);
        }

        public IIdleStrategy IdleStrategy()
        {
            return this;
        }

        public void Reset()
        {
            _idleStrategy.Reset();
        }

        public void Idle()
        {
            _idleStrategy.Idle();
            DoIdleWork();
        }

        public void Idle(int workCount)
        {
            _idleStrategy.Idle(workCount);

            if (workCount <= 0)
            {
                DoIdleWork();
            }
        }

        private void DoIdleWork()
        {
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException();
            }

            long nowNs = _nanoClock.NanoTime();

            CheckForClockTick(nowNs);

            if (_isServiceActive)
            {
                InvokeBackgroundWork(nowNs);
            }
        }

        public void OnJoinLog(
            long logPosition,
            long maxLogPosition,
            int memberId,
            int logSessionId,
            int logStreamId,
            bool isStartup,
            ClusterRole role,
            string logChannel
        )
        {
            _logAdapter.MaxLogPosition(logPosition);
            _activeLogEvent = new ActiveLogEvent(
                logPosition,
                maxLogPosition,
                memberId,
                logSessionId,
                logStreamId,
                isStartup,
                role,
                logChannel
            );
        }

        internal void OnServiceTerminationPosition(long logPosition)
        {
            _terminationPosition = logPosition;
        }

        internal void OnRequestServiceAck(long logPosition)
        {
            _requestedAckPosition = logPosition;
        }

        internal void OnSessionMessage(
            long logPosition,
            long clusterSessionId,
            long timestamp,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header
        )
        {
            this._logPosition = logPosition;
            _clusterTime = timestamp;

            var clientSession = _sessionByIdMap.Get(clusterSessionId);
            _service.OnSessionMessage(clientSession, timestamp, buffer, offset, length, header);
        }

        internal void OnTimerEvent(long logPosition, long correlationId, long timestampMs)
        {
            this._logPosition = logPosition;
            _clusterTime = timestampMs;

            _service.OnTimerEvent(correlationId, timestampMs);
        }

        internal void OnSessionOpen(
            long leadershipTermId,
            long logPosition,
            long clusterSessionId,
            long timestamp,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal
        )
        {
            this._logPosition = logPosition;
            _clusterTime = timestamp;

            if (_sessionByIdMap.ContainsKey(clusterSessionId))
            {
                throw new ClusterException(
                    "clashing open clusterSessionId="
                        + clusterSessionId
                        + " leadershipTermId="
                        + leadershipTermId
                        + " logPosition="
                        + logPosition
                );
            }

            ContainerClientSession session = new ContainerClientSession(
                clusterSessionId,
                responseStreamId,
                responseChannel,
                encodedPrincipal,
                this
            );

            if (ClusterRole.Leader == _role && _ctx.IsRespondingService())
            {
                session.Connect(_aeron);
            }

            AddSession(session);
            _service.OnSessionOpen(session, timestamp);
        }

        internal void OnSessionClose(
            long leadershipTermId,
            long logPosition,
            long clusterSessionId,
            long timestamp,
            CloseReason closeReason
        )
        {
            this._logPosition = logPosition;
            _clusterTime = timestamp;
            var session = _sessionByIdMap.Remove(clusterSessionId);

            if (null == session)
            {
                _ctx.CountedErrorHandler()
                    .OnError(
                        new ClusterEvent(
                            "unknown clusterSessionId="
                                + clusterSessionId
                                + " for close reason="
                                + closeReason
                                + " leadershipTermId="
                                + leadershipTermId
                                + " logPosition="
                                + logPosition
                        )
                    );
            }
            else
            {
                for (int i = 0, size = _sessions.Count; i < size; i++)
                {
                    if (_sessions[i].Id == clusterSessionId)
                    {
                        _sessions.RemoveAt(i);
                        break;
                    }
                }

                ((ContainerClientSession)session).Disconnect(_ctx.CountedErrorHandler().OnError);
                _service.OnSessionClose(session, timestamp, closeReason);
            }
        }

        internal void OnServiceAction(
            long leadershipTermId,
            long logPosition,
            long timestamp,
            ClusterAction action,
            int flags
        )
        {
            this._logPosition = logPosition;
            _clusterTime = timestamp;
            ExecuteAction(action, logPosition, leadershipTermId, flags);
        }

        internal void OnNewLeadershipTermEvent(
            long leadershipTermId,
            long logPosition,
            long timestamp,
            long termBaseLogPosition,
            int leaderMemberId,
            int logSessionId,
            ClusterTimeUnit timeUnit,
            int appVersion
        )
        {
            if (!_ctx.AppVersionValidator().IsVersionCompatible(_ctx.AppVersion(), appVersion))
            {
                _ctx.CountedErrorHandler()
                    .OnError(
                        new ClusterException(
                            "incompatible version: "
                                + SemanticVersion.ToString(_ctx.AppVersion())
                                + " log="
                                + SemanticVersion.ToString(appVersion)
                        )
                    );
                throw new AgentTerminationException();
            }

            _sessionMessageHeaderEncoder.LeadershipTermId(leadershipTermId);
            this._logPosition = logPosition;
            _clusterTime = timestamp;
            this._timeUnit = timeUnit;

            _service.OnNewLeadershipTermEvent(
                leadershipTermId,
                logPosition,
                timestamp,
                termBaseLogPosition,
                leaderMemberId,
                logSessionId,
                timeUnit,
                appVersion
            );
        }

        internal void AddSession(
            long clusterSessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal
        )
        {
            ContainerClientSession session = new ContainerClientSession(
                clusterSessionId,
                responseStreamId,
                responseChannel,
                encodedPrincipal,
                this
            );

            AddSession(session);
        }

        private void AddSession(ContainerClientSession session)
        {
            long clusterSessionId = session.Id;
            _sessionByIdMap.Put(clusterSessionId, session);

            int size = _sessions.Count;
            int addIndex = size;
            for (int i = size - 1; i >= 0; i--)
            {
                if (_sessions[i].Id < clusterSessionId)
                {
                    addIndex = i + 1;
                    break;
                }
            }

            if (size == addIndex)
            {
                _sessions.Add(session);
            }
            else
            {
                _sessions.Insert(addIndex, session);
            }
        }

        internal void HandleError(Exception ex)
        {
            _ctx.CountedErrorHandler().OnError(ex);
        }

        internal long Offer(
            long clusterSessionId,
            Publication publication,
            IDirectBuffer buffer,
            int offset,
            int length
        )
        {
            CheckForValidInvocation();

            if (ClusterRole.Leader != _role)
            {
                return ClientSessionConstants.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            _sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId).Timestamp(_clusterTime);

            return publication.Offer(_headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
        }

        internal long Offer(long clusterSessionId, Publication publication, DirectBufferVector[] vectors)
        {
            CheckForValidInvocation();

            if (ClusterRole.Leader != _role)
            {
                return ClientSessionConstants.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            _sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId).Timestamp(_clusterTime);

            vectors[0] = _headerVector;

            return publication.Offer(vectors, null);
        }

        internal long TryClaim(long clusterSessionId, Publication publication, int length, BufferClaim bufferClaim)
        {
            CheckForValidInvocation();

            if (ClusterRole.Leader != _role)
            {
                int maxPayloadLength = _headerBuffer.Capacity - SESSION_HEADER_LENGTH;
                if (length > maxPayloadLength)
                {
                    throw new ArgumentException(
                        "claim exceeds maxPayloadLength=" + maxPayloadLength + ", length=" + length
                    );
                }

                bufferClaim.Wrap(_messageBuffer, 0, DataHeaderFlyweight.HEADER_LENGTH + SESSION_HEADER_LENGTH + length);
                return ClientSessionConstants.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            long offset = publication.TryClaim(SESSION_HEADER_LENGTH + length, bufferClaim);
            if (offset > 0)
            {
                _sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId).Timestamp(_clusterTime);

                bufferClaim.PutBytes(_headerBuffer, 0, SESSION_HEADER_LENGTH);
            }

            return offset;
        }

        private void RecoverState(CountersReader counters)
        {
            int recoveryCounterId = AwaitRecoveryCounter(counters);
            _logPosition = RecoveryState.GetLogPosition(counters, recoveryCounterId);
            _clusterTime = RecoveryState.GetTimestamp(counters, recoveryCounterId);
            long leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);
            _sessionMessageHeaderEncoder.LeadershipTermId(leadershipTermId);

            _activeLifecycleCallback = LifecycleCallbackOnStart;
            try
            {
                if (NULL_VALUE != leadershipTermId)
                {
                    LoadSnapshot(RecoveryState.GetSnapshotRecordingId(counters, recoveryCounterId, _serviceId));
                }
                else
                {
                    _service.OnStart(this, null);
                }
            }
            finally
            {
                _activeLifecycleCallback = LifecycleCallbackNone;
            }

            long id = _ackId++;
            _idleStrategy.Reset();
            while (!_consensusModuleProxy.Ack(_logPosition, _clusterTime, id, _aeron.ClientId, _serviceId))
            {
                Idle();
            }
        }

        private int AwaitRecoveryCounter(CountersReader counters)
        {
            _idleStrategy.Reset();
            int counterId = RecoveryState.FindCounterId(counters, _ctx.ClusterId());
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = RecoveryState.FindCounterId(counters, _ctx.ClusterId());
            }

            return counterId;
        }

        private void CloseLog()
        {
            _logPosition = Math.Max(_logAdapter.Image().Position, _logPosition);
            CloseHelper.Dispose(_ctx.CountedErrorHandler().OnError, _logAdapter);
            DisconnectEgress(_ctx.CountedErrorHandler().AsErrorHandler);
            Role = ClusterRole.Follower;
        }

        private void DisconnectEgress(ErrorHandler errorHandler)
        {
            for (int i = 0, size = _sessions.Count; i < size; i++)
            {
                _sessions[i].Disconnect(errorHandler);
            }
        }

        private void JoinActiveLog(ActiveLogEvent activeLog)
        {
            if (ClusterRole.Leader != activeLog.role)
            {
                DisconnectEgress(_ctx.CountedErrorHandler().AsErrorHandler);
            }

            String channel = new ChannelUriStringBuilder(activeLog.channel).Alias(_subscriptionAlias).Build();

            Subscription logSubscription = _aeron.AddSubscription(channel, activeLog.streamId);
            try
            {
                Image image = AwaitImage(activeLog.sessionId, logSubscription);
                if (image.JoinPosition != _logPosition)
                {
                    throw new ClusterException(
                        "Cluster log must be contiguous for joining image: "
                            + "expectedPosition="
                            + _logPosition
                            + " joinPosition="
                            + image.JoinPosition
                    );
                }

                if (activeLog.logPosition != _logPosition)
                {
                    throw new ClusterException(
                        "Cluster log must be contiguous for active log event: "
                            + "expectedPosition="
                            + _logPosition
                            + " eventPosition="
                            + activeLog.logPosition
                    );
                }

                _logAdapter.Image(image);
                _logAdapter.MaxLogPosition(activeLog.maxLogPosition);
                logSubscription = null;

                long id = _ackId++;
                while (!_consensusModuleProxy.Ack(activeLog.logPosition, _clusterTime, id, NULL_VALUE, _serviceId))
                {
                    Idle();
                }
            }
            finally
            {
                CloseHelper.QuietDispose(logSubscription);
            }

            _memberId = activeLog.memberId;
            _markFile.MemberId(_memberId);

            if (ClusterRole.Leader == activeLog.role)
            {
                for (var i = 0; i < _sessions.Count; i++)
                {
                    var session = _sessions[i];

                    if (_ctx.IsRespondingService() && !activeLog.isStartup)
                    {
                        session.Connect(_aeron);
                    }

                    session.ResetClosing();
                }
            }

            Role = activeLog.role;
        }

        private Image AwaitImage(int sessionId, Subscription subscription)
        {
            _idleStrategy.Reset();
            Image image;
            while ((image = subscription.ImageBySessionId(sessionId)) == null)
            {
                Idle();
            }

            return image;
        }

        private ReadableCounter AwaitCommitPositionCounter(CountersReader counters, int clusterId)
        {
            _idleStrategy.Reset();
            int counterId = ClusterCounters.Find(
                counters,
                ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID,
                clusterId
            );
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = ClusterCounters.Find(
                    counters,
                    ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID,
                    clusterId
                );
            }

            return new ReadableCounter(counters, counters.GetCounterRegistrationId(counterId), counterId);
        }

        private void LoadSnapshot(long recordingId)
        {
            using (AeronArchive archive = Connect(_ctx.ArchiveContext().Clone()))
            {
                string channel = _ctx.ReplayChannel();
                int streamId = _ctx.ReplayStreamId();
                int sessionId = (int)archive.StartReplay(recordingId, 0, NULL_VALUE, channel, streamId);

                string replaySessionChannel = ChannelUri.AddSessionId(channel, sessionId);
                using (Subscription subscription = _aeron.AddSubscription(replaySessionChannel, streamId))
                {
                    Image image = AwaitImage(sessionId, subscription);
                    LoadState(image, archive);
                    _service.OnStart(this, image);
                }
            }
        }

        private void LoadState(Image image, AeronArchive archive)
        {
            ServiceSnapshotLoader snapshotLoader = new ServiceSnapshotLoader(image, this);
            while (true)
            {
                int fragments = snapshotLoader.Poll();
                if (snapshotLoader.IsDone())
                {
                    break;
                }

                if (0 == fragments)
                {
                    archive.CheckForErrorResponse();
                    if (image.Closed)
                    {
                        throw new ClusterException("snapshot ended unexpectedly" + image);
                    }
                }

                _idleStrategy.Idle(fragments);
            }

            int appVersion = snapshotLoader.AppVersion();
            if (!_ctx.AppVersionValidator().IsVersionCompatible(_ctx.AppVersion(), appVersion))
            {
                throw new ClusterException(
                    "incompatible app version: "
                        + SemanticVersion.ToString(_ctx.AppVersion())
                        + " snapshot="
                        + SemanticVersion.ToString(appVersion)
                );
            }

            _timeUnit = snapshotLoader.TimeUnit();
        }

        private long OnTakeSnapshot(long logPosition, long leadershipTermId)
        {
            try
            {
                using (AeronArchive archive = Connect(_ctx.ArchiveContext().Clone()))
                using (
                    ExclusivePublication publication = _aeron.AddExclusivePublication(
                        _ctx.SnapshotChannel(),
                        _ctx.SnapshotStreamId()
                    )
                )
                {
                    string channel = ChannelUri.AddSessionId(_ctx.SnapshotChannel(), publication.SessionId);
                    archive.StartRecording(channel, _ctx.SnapshotStreamId(), SourceLocation.LOCAL, true);
                    CountersReader counters = _aeron.CountersReader;
                    int counterId = AwaitRecordingCounter(publication.SessionId, counters, archive);
                    long recordingId = RecordingPos.GetRecordingId(counters, counterId);

                    SnapshotState(publication, logPosition, leadershipTermId);
                    CheckForClockTick(_nanoClock.NanoTime());
                    archive.CheckForErrorResponse();

                    _service.OnTakeSnapshot(publication);

                    AwaitRecordingComplete(recordingId, publication.Position, counters, counterId, archive);

                    return recordingId;
                }
            }
            catch (ArchiveException ex)
            {
                if (ex.ErrorCode == ArchiveException.STORAGE_SPACE)
                {
                    throw new AgentTerminationException(ex);
                }

                throw;
            }
        }

        private void AwaitRecordingComplete(
            long recordingId,
            long position,
            CountersReader counters,
            int counterId,
            AeronArchive archive
        )
        {
            _idleStrategy.Reset();
            while (counters.GetCounterValue(counterId) < position)
            {
                Idle();
                archive.CheckForErrorResponse();

                if (!RecordingPos.IsActive(counters, counterId, recordingId))
                {
                    throw new ClusterException("recording stopped unexpectedly: " + recordingId);
                }
            }
        }

        private void SnapshotState(ExclusivePublication publication, long logPosition, long leadershipTermId)
        {
            var snapshotTaker = new ServiceSnapshotTaker(publication, _idleStrategy, _aeronAgentInvoker);

            snapshotTaker.MarkBegin(
                ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID,
                logPosition,
                leadershipTermId,
                0,
                _timeUnit,
                _ctx.AppVersion()
            );

            for (int i = 0, size = _sessions.Count; i < size; i++)
            {
                snapshotTaker.SnapshotSession(_sessions[i]);
            }

            snapshotTaker.MarkEnd(
                ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID,
                logPosition,
                leadershipTermId,
                0,
                _timeUnit,
                _ctx.AppVersion()
            );
        }

        private void ExecuteAction(ClusterAction action, long logPosition, long leadershipTermId, int flags)
        {
            if (ClusterAction.SNAPSHOT == action && ShouldSnapshot(flags))
            {
                long recordingId = NULL_VALUE;
                Exception exception = null;
                _snapshotDurationTracker.OnSnapshotBegin(_nanoClock.NanoTime());
                try
                {
                    recordingId = OnTakeSnapshot(logPosition, leadershipTermId);
                }
                catch (Exception ex)
                {
                    exception = ex;
                }
                finally
                {
                    _snapshotDurationTracker.OnSnapshotEnd(_nanoClock.NanoTime());
                }

                long id = _ackId++;
                while (!_consensusModuleProxy.Ack(logPosition, _clusterTime, id, recordingId, _serviceId))
                {
                    Idle();
                }

                if (null != exception)
                {
                    ExceptionDispatchInfo.Capture(exception).Throw();
                }
            }
        }

        private bool ShouldSnapshot(int flags)
        {
            return CLUSTER_ACTION_FLAGS_DEFAULT == flags || 0 != (flags & _standbySnapshotFlags);
        }

        private int AwaitRecordingCounter(int sessionId, CountersReader counters, AeronArchive archive)
        {
            _idleStrategy.Reset();
            long archiveId = archive.ArchiveId();
            int counterId = RecordingPos.FindCounterIdBySession(counters, sessionId, archiveId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                archive.CheckForErrorResponse();
                counterId = RecordingPos.FindCounterIdBySession(counters, sessionId, archiveId);
            }

            return counterId;
        }

        private bool CheckForClockTick(long nowNs)
        {
            if (_isAbort || _aeron.IsClosed)
            {
                _isAbort = true;
                throw new AgentTerminationException("unexpected Aeron close");
            }

            if (nowNs - _lastSlowTickNs > s_oneMillisecondNs)
            {
                _lastSlowTickNs = nowNs;

                if (null != _aeronAgentInvoker)
                {
                    _aeronAgentInvoker.Invoke();

                    if (_isAbort || _aeron.IsClosed)
                    {
                        _isAbort = true;
                        throw new AgentTerminationException("unexpected Aeron close");
                    }
                }

                if (null != _commitPosition && _commitPosition.IsClosed)
                {
                    _ctx.ErrorLog()
                        .Record(new AeronEvent("commit-pos counter unexpectedly closed, terminating", Category.WARN));

                    throw new ClusterTerminationException(true);
                }

                long nowMs = _epochClock.Time();
                if (nowMs >= _markFileUpdateDeadlineMs)
                {
                    _markFileUpdateDeadlineMs = nowMs + s_markFileUpdateIntervalMs;
                    _ctx.ClusterMarkFile().UpdateActivityTimestamp(nowMs);
                }

                return true;
            }

            return false;
        }

        private int PollServiceAdapter()
        {
            int workCount = 0;

            workCount += _serviceAdapter.Poll();

            if (null != _activeLogEvent && null == _logAdapter.Image())
            {
                ActiveLogEvent @event = _activeLogEvent;
                _activeLogEvent = null;
                JoinActiveLog(@event);
            }

            if (NULL_POSITION != _terminationPosition && _logPosition >= _terminationPosition)
            {
                if (_logPosition > _terminationPosition)
                {
                    _ctx.CountedErrorHandler()
                        .OnError(
                            new ClusterException(
                                "service terminate: logPosition="
                                    + _logPosition
                                    + " > terminationPosition="
                                    + _terminationPosition
                            )
                        );
                }

                Terminate(_logPosition == _terminationPosition);
            }

            if (NULL_POSITION != _requestedAckPosition && _logPosition >= _requestedAckPosition)
            {
                if (_logPosition > _requestedAckPosition)
                {
                    _ctx.CountedErrorHandler()
                        .OnError(
                            new ClusterEvent(
                                "invalid ack request: logPosition="
                                    + _logPosition
                                    + " > requestedAckPosition="
                                    + _requestedAckPosition
                            )
                        );
                }

                long id = _ackId++;
                while (!_consensusModuleProxy.Ack(_logPosition, _clusterTime, id, NULL_VALUE, _serviceId))
                {
                    Idle();
                }

                _requestedAckPosition = NULL_POSITION;
            }

            return workCount;
        }

        private void Terminate(bool isTerminationExpected)
        {
            _isServiceActive = false;
            _activeLifecycleCallback = LifecycleCallbackOnTerminate;
            try
            {
                _service.OnTerminate(this);
            }
            catch (Exception ex)
            {
                _ctx.CountedErrorHandler().OnError(ex);
            }
            finally
            {
                _activeLifecycleCallback = LifecycleCallbackNone;
            }

            try
            {
                int attempts = 5;
                long id = _ackId++;
                while (!_consensusModuleProxy.Ack(_logPosition, _clusterTime, id, NULL_VALUE, _serviceId))
                {
                    if (0 == --attempts)
                    {
                        break;
                    }

                    Idle();
                }
            }
            catch (Exception ex)
            {
                _ctx.CountedErrorHandler().OnError(ex);
            }

            _terminationPosition = NULL_VALUE;
            throw new ClusterTerminationException(isTerminationExpected);
        }

        private void CheckForValidInvocation()
        {
            if (LifecycleCallbackNone != _activeLifecycleCallback)
            {
                var lifecycleName = LifecycleName(_activeLifecycleCallback);
                throw new ClusterException(
                    "sending messages or scheduling timers is not allowed from " + lifecycleName);
            }
        }

        private void Abort()
        {
            _isAbort = true;

            try
            {
                if (!_ctx.AbortLatch().Wait(TimeSpan.FromMilliseconds(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L)))
                {
                    _ctx.CountedErrorHandler()
                        .OnError(new AeronTimeoutException("awaiting abort latch", Category.WARN));
                }
            }
            catch (ThreadInterruptedException)
            {
                Thread.CurrentThread.Interrupt();
            }
        }

        private void CounterUnavailable(CountersReader countersReader, long registrationId, int counterId)
        {
            ReadableCounter commitPosition = this._commitPosition;
            if (
                null != commitPosition
                && commitPosition.CounterId == counterId
                && commitPosition.RegistrationId == registrationId
            )
            {
                commitPosition.Dispose();
            }
        }

        private int InvokeBackgroundWork(long nowNs)
        {
            try
            {
                _activeLifecycleCallback = LifecycleCallbackDoBackgroundWork;
                return _service.DoBackgroundWork(nowNs);
            }
            finally
            {
                _activeLifecycleCallback = LifecycleCallbackNone;
            }
        }

        private void RunTerminationHook()
        {
            try
            {
                _ctx.TerminationHook()();
            }
            catch (Exception ex)
            {
                _ctx.CountedErrorHandler().OnError(ex);
            }
        }
    }
}
