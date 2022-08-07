using System;
using System.Collections.Generic;
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
using MessageHeaderEncoder = Adaptive.Cluster.Codecs.MessageHeaderEncoder;

namespace Adaptive.Cluster.Service
{
    internal sealed class ClusteredServiceAgent : IAgent, ICluster, IIdleStrategy
    {
        static long MARK_FILE_UPDATE_INTERVAL_MS =
            Agrona.TimeUnit.NANOSECONDS.toMillis(ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS);

        private static readonly int MAX_UDP_PAYLOAD_LENGTH = 65504;

        private volatile bool isAbort;
        private bool isServiceActive;
        private readonly int serviceId;
        private int memberId = NULL_VALUE;
        private long closeHandlerRegistrationid;
        private long ackId = 0;
        private long terminationPosition = NULL_POSITION;
        private long markFileUpdateDeadlineMs;
        private long cachedTimeMs;
        private long clusterTime;
        private long logPosition = NULL_POSITION;

        private readonly IIdleStrategy idleStrategy;
        private readonly ClusterMarkFile markFile;
        private readonly ClusteredServiceContainer.Context ctx;
        private readonly Aeron.Aeron aeron;
        private readonly AgentInvoker aeronAgentInvoker;
        private readonly IClusteredService service;
        private readonly ConsensusModuleProxy _consensusModuleProxy;
        private readonly ServiceAdapter _serviceAdapter;
        private readonly IEpochClock epochClock;
        private readonly INanoClock nanoClock;
        
        private readonly UnsafeBuffer messageBuffer = new UnsafeBuffer(new byte[MAX_UDP_PAYLOAD_LENGTH]);
        private readonly UnsafeBuffer headerBuffer; // set in constructor
        private readonly DirectBufferVector headerVector; // set in constructor
        private readonly SessionMessageHeaderEncoder _sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private readonly Map<long, IClientSession> sessionByIdMap =
            new Map<long, IClientSession>();
        private readonly BoundedLogAdapter logAdapter;
        private readonly DutyCycleTracker dutyCycleTracker;
        
        private string activeLifecycleCallbackName;
        private ReadableCounter commitPosition;
        private ActiveLogEvent activeLogEvent;
        private ClusterRole role = ClusterRole.Follower;
        private ClusterTimeUnit timeUnit = ClusterTimeUnit.NULL_VALUE;
        
        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            headerBuffer = new UnsafeBuffer(messageBuffer, DataHeaderFlyweight.HEADER_LENGTH, MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH);
            headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);

            logAdapter = new BoundedLogAdapter(this, ctx.LogFragmentLimit());
            this.ctx = ctx;

            markFile = ctx.ClusterMarkFile();
            aeron = ctx.Aeron();
            aeronAgentInvoker = ctx.Aeron().ConductorAgentInvoker;
            service = ctx.ClusteredService();
            idleStrategy = ctx.IdleStrategy();
            serviceId = ctx.ServiceId();
            epochClock = ctx.EpochClock();
            nanoClock = ctx.NanoClock();
            dutyCycleTracker = ctx.DutyCycleTracker();

            var channel = ctx.ControlChannel();
            _consensusModuleProxy =
                new ConsensusModuleProxy(aeron.AddPublication(channel, ctx.ConsensusModuleStreamId()));
            _serviceAdapter = new ServiceAdapter(aeron.AddSubscription(channel, ctx.ServiceStreamId()), this);
            _sessionMessageHeaderEncoder.WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
        }

        public void OnStart()
        {
            closeHandlerRegistrationid = aeron.AddCloseHandler(Abort);
            CountersReader counters = aeron.CountersReader;
            commitPosition = AwaitCommitPositionCounter(counters, ctx.ClusterId());

            RecoverState(counters);
            dutyCycleTracker.Update(nanoClock.NanoTime());
        }

        public void OnClose()
        {
            aeron.RemoveCloseHandler(closeHandlerRegistrationid);

            if (isAbort)
            {
                ctx.AbortLatch().Signal();
            }
            else
            {
                ErrorHandler errorHandler = ctx.CountedErrorHandler().OnError;

                if (isServiceActive)
                {
                    isServiceActive = false;
                    try
                    {
                        service.OnTerminate(this);
                    }
                    catch (Exception ex)
                    {
                        errorHandler(ex);
                    }
                }

                if (!ctx.OwnsAeronClient() && !aeron.IsClosed)
                {
                    foreach (var session in sessionByIdMap.Values)
                    {
                        ((ContainerClientSession)session).Disconnect(errorHandler);
                    }

                    CloseHelper.Dispose(errorHandler, logAdapter);
                    CloseHelper.Dispose(errorHandler, _serviceAdapter);
                    CloseHelper.Dispose(errorHandler, _consensusModuleProxy);
                }
            }

            markFile.UpdateActivityTimestamp(NULL_VALUE);
            ctx.Dispose();
        }

        public int DoWork()
        {
            int workCount = 0;
            
            dutyCycleTracker.MeasureAndUpdate(nanoClock.NanoTime());

            try
            {
                if (CheckForClockTick())
                {
                    PollServiceAdapter();
                    workCount += 1;
                }

                if (null != logAdapter.Image())
                {
                    int polled = logAdapter.Poll(commitPosition.Get());
                    workCount += polled;

                    if (0 == polled && logAdapter.IsDone())
                    {
                        CloseLog();
                    }
                }
            }
            catch (AgentTerminationException)
            {
                RunTerminationHook();
                throw;
            }

            return workCount;
        }

        public string RoleName() => ctx.ServiceName();

        public ClusterRole Role
        {
            get => role;
            private set
            {
                if (value != role)
                {
                    role = value;
                    activeLifecycleCallbackName = "onRoleChange";
                    try
                    {
                        service.OnRoleChange(value);
                    }
                    finally
                    {
                        activeLifecycleCallbackName = null;
                    }
                }
            }
        }

        public int MemberId => memberId;

        public Aeron.Aeron Aeron => aeron;

        public ClusteredServiceContainer.Context Context => ctx;

        public IClientSession GetClientSession(long clusterSessionId)
        {
            return sessionByIdMap.Get(clusterSessionId);
        }

        public ICollection<IClientSession> ClientSessions => sessionByIdMap.Values;

        public void ForEachClientSession(Action<IClientSession> action)
        {
            foreach (var clientSession in sessionByIdMap.Values)
            {
                action(clientSession);
            }
        }

        public bool CloseClientSession(long clusterSessionId)
        {
            CheckForLifecycleCallback();

            if (!sessionByIdMap.ContainsKey(clusterSessionId))
            {
                throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
            }

            ContainerClientSession clientSession = (ContainerClientSession)sessionByIdMap.Get(clusterSessionId);

            if (clientSession.IsClosing)
            {
                return true;
            }

            if (_consensusModuleProxy.CloseSession(clusterSessionId))
            {
                clientSession.MarkClosing();
                return true;
            }

            return false;
        }

        public ClusterTimeUnit TimeUnit()
        {
            return timeUnit;
        }

        public long Time => clusterTime;

        public long LogPosition()
        {
            return logPosition;
        }

        public bool ScheduleTimer(long correlationId, long deadline)
        {
            CheckForLifecycleCallback();

            return _consensusModuleProxy.ScheduleTimer(correlationId, deadline);
        }

        public bool CancelTimer(long correlationId)
        {
            CheckForLifecycleCallback();

            return _consensusModuleProxy.CancelTimer(correlationId);
        }

        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            CheckForLifecycleCallback();
            _sessionMessageHeaderEncoder.ClusterSessionId(0);

            return _consensusModuleProxy.Offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
        }

        public long Offer(DirectBufferVector[] vectors)
        {
            CheckForLifecycleCallback();
            _sessionMessageHeaderEncoder.ClusterSessionId(0);
            vectors[0] = headerVector;

            return _consensusModuleProxy.Offer(vectors);
        }

        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            CheckForLifecycleCallback();
            _sessionMessageHeaderEncoder.ClusterSessionId(0);

            return _consensusModuleProxy.TryClaim(length + SESSION_HEADER_LENGTH, bufferClaim, headerBuffer);
        }

        public IIdleStrategy IdleStrategy()
        {
            return this;
        }

        public void Reset()
        {
            idleStrategy.Reset();
        }

        public void Idle()
        {
            idleStrategy.Idle();
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException();
            }

            CheckForClockTick();
        }

        public void Idle(int workCount)
        {
            idleStrategy.Idle(workCount);

            if (workCount <= 0)
            {
                try
                {
                    Thread.Sleep(0);
                }
                catch (ThreadInterruptedException)
                {
                    throw new AgentTerminationException();
                }

                CheckForClockTick();
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
            string logChannel)
        {
            logAdapter.MaxLogPosition(logPosition);
            activeLogEvent = new ActiveLogEvent(
                logPosition,
                maxLogPosition,
                memberId,
                logSessionId,
                logStreamId,
                isStartup,
                role,
                logChannel);
        }

        internal void OnServiceTerminationPosition(long logPosition)
        {
            terminationPosition = logPosition;
        }

        internal void OnSessionMessage(
            long logPosition,
            long clusterSessionId,
            long timestamp,
            IDirectBuffer buffer,
            int offset,
            int length,
            Header header)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;

            var clientSession = sessionByIdMap.Get(clusterSessionId);
            service.OnSessionMessage(clientSession, timestamp, buffer, offset, length, header);
        }

        internal void OnTimerEvent(long logPosition, long correlationId, long timestampMs)
        {
            this.logPosition = logPosition;
            clusterTime = timestampMs;

            service.OnTimerEvent(correlationId, timestampMs);
        }

        internal void OnSessionOpen(
            long leadershipTermId,
            long logPosition,
            long clusterSessionId,
            long timestamp,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;

            if (sessionByIdMap.ContainsKey(clusterSessionId))
            {
                throw new ClusterException("clashing open clusterSessionId=" + clusterSessionId +
                                           " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
            }

            ContainerClientSession session = new ContainerClientSession(
                clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

            if (ClusterRole.Leader == role && ctx.IsRespondingService())
            {
                session.Connect(aeron);
            }

            sessionByIdMap.Put(clusterSessionId, session);
            service.OnSessionOpen(session, timestamp);
        }

        internal void OnSessionClose(long leadershipTermId, long logPosition, long clusterSessionId, long timestamp,
            CloseReason closeReason)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;
            var session = sessionByIdMap.Remove(clusterSessionId);

            if (null == session)
            {
                throw new ClusterException(
                    "unknown clusterSessionId=" + clusterSessionId + " for close reason=" + closeReason +
                    " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
            }

            ((ContainerClientSession)session).Disconnect(ctx.CountedErrorHandler().OnError);
            service.OnSessionClose(session, timestamp, closeReason);
        }

        internal void OnServiceAction(long leadershipTermId, long logPosition, long timestamp, ClusterAction action)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;
            ExecuteAction(action, logPosition, leadershipTermId);
        }

        internal void OnNewLeadershipTermEvent(
            long leadershipTermId,
            long logPosition,
            long timestamp,
            long termBaseLogPosition,
            int leaderMemberId,
            int logSessionId,
            ClusterTimeUnit timeUnit,
            int appVersion)
        {
            if (SemanticVersion.Major(ctx.AppVersion()) != SemanticVersion.Major(appVersion))
            {
                ctx.ErrorHandler()(new ClusterException("incompatible version: " +
                                                        SemanticVersion.ToString(ctx.AppVersion()) + " log=" +
                                                        SemanticVersion.ToString(appVersion)));
                throw new AgentTerminationException();
            }

            _sessionMessageHeaderEncoder.LeadershipTermId(leadershipTermId);
            this.logPosition = logPosition;
            clusterTime = timestamp;
            this.timeUnit = timeUnit;

            service.OnNewLeadershipTermEvent(
                leadershipTermId,
                logPosition,
                timestamp,
                termBaseLogPosition,
                leaderMemberId,
                logSessionId,
                timeUnit,
                appVersion);
        }

        internal void OnMembershipChange(long logPosition, long timestamp, ChangeType changeType, int memberId)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;

            if (memberId == this.memberId && changeType == ChangeType.QUIT)
            {
                Terminate(true);
            }
        }

        internal void AddSession(
            long clusterSessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal)
        {
            var session = new ContainerClientSession(
                clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

            sessionByIdMap.Put(clusterSessionId, session);
        }

        internal void HandleError(Exception ex)
        {
            ctx.CountedErrorHandler().OnError(ex);
        }

        internal long Offer(long clusterSessionId, Publication publication, IDirectBuffer buffer, int offset,
            int length)
        {
            CheckForLifecycleCallback();

            if (ClusterRole.Leader != role)
            {
                return ClientSessionConstants.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            _sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId).Timestamp(clusterTime);

            return publication.Offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
        }

        internal long Offer(long clusterSessionId, Publication publication, DirectBufferVector[] vectors)
        {
            CheckForLifecycleCallback();

            if (ClusterRole.Leader != role)
            {
                return ClientSessionConstants.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            _sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId).Timestamp(clusterTime);

            vectors[0] = headerVector;

            return publication.Offer(vectors, null);
        }

        internal long TryClaim(long clusterSessionId, Publication publication, int length, BufferClaim bufferClaim)
        {
            CheckForLifecycleCallback();

            if (ClusterRole.Leader != role)
            {
                int maxPayloadLength = headerBuffer.Capacity - SESSION_HEADER_LENGTH;
                if (length > maxPayloadLength)
                {
                    throw new ArgumentException(
                        "claim exceeds maxPayloadLength=" + maxPayloadLength + ", length=" + length);
                }

                bufferClaim.Wrap(
                    messageBuffer, 0, DataHeaderFlyweight.HEADER_LENGTH + SESSION_HEADER_LENGTH + length
                    );
                return ClientSessionConstants.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            long offset = publication.TryClaim(SESSION_HEADER_LENGTH + length, bufferClaim);
            if (offset > 0)
            {
                _sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId).Timestamp(clusterTime);

                bufferClaim.PutBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
            }

            return offset;
        }

        private void RecoverState(CountersReader counters)
        {
            int recoveryCounterId = AwaitRecoveryCounter(counters);
            logPosition = RecoveryState.GetLogPosition(counters, recoveryCounterId);
            clusterTime = RecoveryState.GetTimestamp(counters, recoveryCounterId);
            long leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);
            _sessionMessageHeaderEncoder.LeadershipTermId(leadershipTermId);
            isServiceActive = true;

            activeLifecycleCallbackName = "onStart";
            try
            {
                if (NULL_VALUE != leadershipTermId)
                {
                    LoadSnapshot(RecoveryState.GetSnapshotRecordingId(counters, recoveryCounterId, serviceId));
                }
                else
                {
                    service.OnStart(this, null);
                }
            }
            finally
            {
                activeLifecycleCallbackName = null;
            }

            long id = ackId++;
            idleStrategy.Reset();
            while (!_consensusModuleProxy.Ack(logPosition, clusterTime, id, aeron.ClientId, serviceId))
            {
                Idle();
            }
        }

        private int AwaitRecoveryCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = RecoveryState.FindCounterId(counters, ctx.ClusterId());
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = RecoveryState.FindCounterId(counters, ctx.ClusterId());
            }

            return counterId;
        }

        private void CloseLog()
        {
            logPosition = Math.Max(logAdapter.Image().Position, logPosition);
            CloseHelper.Dispose(ctx.CountedErrorHandler().OnError, logAdapter);
            Role = ClusterRole.Follower;
        }

        private void JoinActiveLog(ActiveLogEvent activeLog)
        {
            if (ClusterRole.Leader != activeLog.role)
            {
                foreach (ContainerClientSession session in sessionByIdMap.Values)
                {
                    session.Disconnect(ctx.CountedErrorHandler().OnError);
                }
            }

            Subscription logSubscription = aeron.AddSubscription(activeLog.channel, activeLog.streamId);
            try
            {
                Image image = AwaitImage(activeLog.sessionId, logSubscription);
                if (image.JoinPosition != logPosition)
                {
                    throw new ClusterException("Cluster log must be contiguous for joining image: " +
                                               "expectedPosition=" + logPosition + " joinPosition=" +
                                               image.JoinPosition);
                }

                if (activeLog.logPosition != logPosition)
                {
                    throw new ClusterException("Cluster log must be contiguous for active log event: " +
                                               "expectedPosition=" + logPosition + " eventPosition=" +
                                               activeLog.logPosition);
                }

                logAdapter.Image(image);
                logAdapter.MaxLogPosition(activeLog.maxLogPosition);
                logSubscription = null;

                long id = ackId++;
                idleStrategy.Reset();
                while (!_consensusModuleProxy.Ack(activeLog.logPosition, clusterTime, id, NULL_VALUE, serviceId))
                {
                    Idle();
                }
            }
            finally
            {
                CloseHelper.QuietDispose(logSubscription);
            }

            memberId = activeLog.memberId;
            markFile.MemberId(memberId);

            if (ClusterRole.Leader == activeLog.role)
            {
                foreach (ContainerClientSession session in sessionByIdMap.Values)
                {
                    if (ctx.IsRespondingService() && !activeLog.isStartup)
                    {
                        session.Connect(aeron);
                    }

                    session.ResetClosing();
                }
            }

            Role = activeLog.role;
        }

        private Image AwaitImage(int sessionId, Subscription subscription)
        {
            idleStrategy.Reset();
            Image image;
            while ((image = subscription.ImageBySessionId(sessionId)) == null)
            {
                Idle();
            }

            return image;
        }

        private ReadableCounter AwaitCommitPositionCounter(CountersReader counters, int clusterId)
        {
            idleStrategy.Reset();
            int counterId = ClusterCounters.Find(counters,
                ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID, clusterId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = ClusterCounters.Find(counters,
                    ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID, clusterId);
            }

            return new ReadableCounter(counters, counterId);
        }

        private void LoadSnapshot(long recordingId)
        {
            using (AeronArchive archive = Connect(ctx.ArchiveContext().Clone()))
            {
                string channel = ctx.ReplayChannel();
                int streamId = ctx.ReplayStreamId();
                int sessionId = (int)archive.StartReplay(recordingId, 0, NULL_VALUE, channel, streamId);

                string replaySessionChannel = ChannelUri.AddSessionId(channel, sessionId);
                using (Subscription subscription = aeron.AddSubscription(replaySessionChannel, streamId))
                {
                    Image image = AwaitImage(sessionId, subscription);
                    LoadState(image, archive);
                    service.OnStart(this, image);
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

                if (fragments == 0)
                {
                    archive.CheckForErrorResponse();
                    if (image.Closed)
                    {
                        throw new ClusterException("snapshot ended unexpectedly" + image);
                    }
                }

                idleStrategy.Idle(fragments);
            }

            int appVersion = snapshotLoader.AppVersion();
            if (SemanticVersion.Major(ctx.AppVersion()) != SemanticVersion.Major(appVersion))
            {
                throw new ClusterException(
                    "incompatible app version: " + SemanticVersion.ToString(ctx.AppVersion()) +
                    " snapshot=" + SemanticVersion.ToString(appVersion));
            }

            timeUnit = snapshotLoader.TimeUnit();
        }

        private long OnTakeSnapshot(long logPosition, long leadershipTermId)
        {
            try
            {
                using (AeronArchive archive = Connect(ctx.ArchiveContext().Clone()))
                using (ExclusivePublication publication =
                       aeron.AddExclusivePublication(ctx.SnapshotChannel(), ctx.SnapshotStreamId()))
                {
                    string channel = ChannelUri.AddSessionId(ctx.SnapshotChannel(), publication.SessionId);
                    archive.StartRecording(channel, ctx.SnapshotStreamId(), SourceLocation.LOCAL, true);
                    CountersReader counters = aeron.CountersReader;
                    int counterId = AwaitRecordingCounter(publication.SessionId, counters, archive);
                    long recordingId = RecordingPos.GetRecordingId(counters, counterId);

                    SnapshotState(publication, logPosition, leadershipTermId);
                    CheckForClockTick();
                    archive.CheckForErrorResponse();
                    service.OnTakeSnapshot(publication);
                    AwaitRecordingComplete(recordingId, publication.Position, counters, counterId, archive);

                    return recordingId;
                }
            }
            catch (ArchiveException ex)
            {
                if (ex.ErrorCode == ArchiveException.STORAGE_SPACE)
                {
                    throw new AgentTerminationException();
                }

                throw;
            }
        }

        private void AwaitRecordingComplete(
            long recordingId,
            long position,
            CountersReader counters,
            int counterId,
            AeronArchive archive)
        {
            idleStrategy.Reset();
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
            var snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, aeronAgentInvoker);

            snapshotTaker.MarkBegin(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID, logPosition,
                leadershipTermId, 0,
                timeUnit, ctx.AppVersion());

            foreach (IClientSession clientSession in sessionByIdMap.Values)
            {
                snapshotTaker.SnapshotSession(clientSession);
            }

            snapshotTaker.MarkEnd(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID, logPosition,
                leadershipTermId, 0,
                timeUnit, ctx.AppVersion());
        }

        private void ExecuteAction(ClusterAction action, long logPosition, long leadershipTermId)
        {
            if (ClusterAction.SNAPSHOT == action)
            {
                long recordingId = OnTakeSnapshot(logPosition, leadershipTermId);
                long id = ackId++;
                idleStrategy.Reset();
                while (!_consensusModuleProxy.Ack(logPosition, clusterTime, id, recordingId, serviceId))
                {
                    Idle();
                }
            }
        }

        private int AwaitRecordingCounter(int sessionId, CountersReader counters, AeronArchive archive)
        {
            idleStrategy.Reset();
            int counterId = RecordingPos.FindCounterIdBySession(counters, sessionId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                archive.CheckForErrorResponse();
                counterId = RecordingPos.FindCounterIdBySession(counters, sessionId);
            }

            return counterId;
        }

        private bool CheckForClockTick()
        {
            if (isAbort || aeron.IsClosed)
            {
                isAbort = true;
                throw new AgentTerminationException("unexpected Aeron close");
            }

            long nowMs = epochClock.Time();
            if (cachedTimeMs != nowMs)
            {
                cachedTimeMs = nowMs;

                if (null != aeronAgentInvoker)
                {
                    aeronAgentInvoker.Invoke();

                    if (isAbort || aeron.IsClosed)
                    {
                        throw new AgentTerminationException("unexpected Aeron close");
                    }
                }

                if (nowMs >= markFileUpdateDeadlineMs)
                {
                    markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
                    ctx.ClusterMarkFile().UpdateActivityTimestamp(nowMs);
                }

                return true;
            }

            return false;
        }

        private void PollServiceAdapter()
        {
            _serviceAdapter.Poll();

            if (null != activeLogEvent && null == logAdapter.Image())
            {
                ActiveLogEvent @event = activeLogEvent;
                activeLogEvent = null;
                JoinActiveLog(@event);
            }

            if (NULL_POSITION != terminationPosition && logPosition >= terminationPosition)
            {
                if (logPosition > terminationPosition)
                {
                    ctx.CountedErrorHandler().OnError(new ClusterException(
                        "service terminate: logPosition=" + logPosition + " > terminationPosition=" + terminationPosition));
                }

                Terminate(logPosition == terminationPosition);
            }
        }

        private void Terminate(bool isTerminationExpected)
        {
            isServiceActive = false;
            activeLifecycleCallbackName = "onTerminate";
            try
            {
                service.OnTerminate(this);
            }
            catch (Exception ex)
            {
                ctx.CountedErrorHandler().OnError(ex);
            }
            finally
            {
                activeLifecycleCallbackName = null;
            }

            try
            {
                int attempts = 5;
                long id = ackId++;
                while (!_consensusModuleProxy.Ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
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
                ctx.CountedErrorHandler().OnError(ex);
            }

            terminationPosition = NULL_VALUE;
            throw new ClusterTerminationException(isTerminationExpected);
        }


        private void CheckForLifecycleCallback()
        {
            if (null != activeLifecycleCallbackName)
            {
                throw new ClusterException(
                    "sending messages or scheduling timers is not allowed from " + activeLifecycleCallbackName);
            }
        }

        private void Abort()
        {
            isAbort = true;

            try
            {
                if (!ctx.AbortLatch().Wait(TimeSpan.FromMilliseconds(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L)))
                {
                    ctx.CountedErrorHandler().OnError(
                        new AeronTimeoutException("awaiting abort latch", Category.WARN));
                }
            }
            catch (ThreadInterruptedException)
            {
                Thread.CurrentThread.Interrupt();
            }
        }

        private void RunTerminationHook()
        {
            try
            {
                ctx.TerminationHook()();
            }
            catch (Exception ex)
            {
                ctx.CountedErrorHandler().OnError(ex);
            }
        }
    }
}