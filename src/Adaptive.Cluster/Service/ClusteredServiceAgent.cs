using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
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
        private long ackId = 0;
        private long terminationPosition = NULL_POSITION;
        private long timeOfLastMarkFileUpdateMs;
        private long cachedTimeMs;
        private long clusterTime;
        private long logPosition = NULL_POSITION;

        private readonly Action abortHandler;
        private readonly IIdleStrategy idleStrategy;
        private readonly ClusteredServiceContainer.Context ctx;
        private readonly Aeron.Aeron aeron;
        private readonly AgentInvoker aeronAgentInvoker;
        private readonly IClusteredService service;
        private readonly ConsensusModuleProxy _consensusModuleProxy;
        private readonly ServiceAdapter _serviceAdapter;
        private readonly IEpochClock epochClock;

        private readonly UnsafeBuffer headerBuffer =
            new UnsafeBuffer(new byte[MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH]);

        private readonly DirectBufferVector headerVector;
        private readonly SessionMessageHeaderEncoder _sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private readonly Dictionary<long, ClientSession> sessionByIdMap = new Dictionary<long, ClientSession>();

        private BoundedLogAdapter logAdapter;
        private ReadableCounter roleCounter;
        private ReadableCounter commitPosition;
        private ActiveLogEvent activeLogEvent;
        private ClusterRole role = ClusterRole.Follower;
        private ClusterTimeUnit timeUnit = ClusterTimeUnit.NULL_VALUE;

        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            headerVector = new DirectBufferVector(headerBuffer, 0, headerBuffer.Capacity);
            abortHandler = Abort;

            this.ctx = ctx;

            logAdapter = new BoundedLogAdapter(this);

            aeron = ctx.Aeron();
            aeronAgentInvoker = ctx.Aeron().ConductorAgentInvoker;
            service = ctx.ClusteredService();
            idleStrategy = ctx.IdleStrategy();
            serviceId = ctx.ServiceId();
            epochClock = ctx.EpochClock();

            var channel = ctx.ControlChannel();
            _consensusModuleProxy =
                new ConsensusModuleProxy(aeron.AddPublication(channel, ctx.ConsensusModuleStreamId()));
            _serviceAdapter = new ServiceAdapter(aeron.AddSubscription(channel, ctx.ServiceStreamId()), this);
            _sessionMessageHeaderEncoder.WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
        }

        public void OnStart()
        {
            aeron.AddCloseHandler(abortHandler);
            CountersReader counters = aeron.CountersReader;
            roleCounter = AwaitClusterRoleCounter(counters, ctx.ClusterId());
            commitPosition = AwaitCommitPositionCounter(counters, ctx.ClusterId());

            RecoverState(counters);
        }

        public void OnClose()
        {
            aeron.RemoveCloseHandler(abortHandler);

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
                        session.Disconnect(errorHandler);
                    }


                    CloseHelper.Dispose(errorHandler, logAdapter);
                    CloseHelper.Dispose(errorHandler, _serviceAdapter);
                    CloseHelper.Dispose(errorHandler, _consensusModuleProxy);
                }
            }

            ctx.Dispose();
        }

        public int DoWork()
        {
            int workCount = 0;

            if (CheckForClockTick())
            {
                PollServiceAdapter();
                workCount += 1;
            }

            if (null != logAdapter.Image())
            {
                int polled = logAdapter.Poll(commitPosition.Get());
                if (0 == polled && logAdapter.IsDone())
                {
                    CloseLog();
                }

                workCount += polled;
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
                    service.OnRoleChange(value);
                }
            }
        }

        public int MemberId => memberId;

        public Aeron.Aeron Aeron => aeron;

        public ClusteredServiceContainer.Context Context => ctx;

        public ClientSession GetClientSession(long clusterSessionId)
        {
            return sessionByIdMap[clusterSessionId];
        }

        public ICollection<ClientSession> ClientSessions => sessionByIdMap.Values;

        public void ForEachClientSession(Action<ClientSession> action)
        {
            foreach (var clientSession in sessionByIdMap.Values)
            {
                action(clientSession);
            }
        }

        public bool CloseClientSession(long clusterSessionId)
        {
            if (!sessionByIdMap.ContainsKey(clusterSessionId))
            {
                throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
            }

            ClientSession clientSession = sessionByIdMap[clusterSessionId];

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
            return _consensusModuleProxy.ScheduleTimer(correlationId, deadline);
        }

        public bool CancelTimer(long correlationId)
        {
            return _consensusModuleProxy.CancelTimer(correlationId);
        }

        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            _sessionMessageHeaderEncoder.ClusterSessionId(0);

            return _consensusModuleProxy.Offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
        }

        public long Offer(DirectBufferVector[] vectors)
        {
            _sessionMessageHeaderEncoder.ClusterSessionId(0);
            vectors[0] = headerVector;

            return _consensusModuleProxy.Offer(vectors);
        }

        public long TryClaim(int length, BufferClaim bufferClaim)
        {
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
            CheckForClockTick();
        }

        public void Idle(int workCount)
        {
            idleStrategy.Idle(workCount);

            if (workCount <= 0)
            {
                CheckForClockTick();
            }
        }

        public void OnJoinLog(
            long leadershipTermId,
            long logPosition,
            long maxLogPosition,
            int memberId,
            int logSessionId,
            int logStreamId,
            bool isStartup,
            string logChannel)
        {
            logAdapter.MaxLogPosition(logPosition);
            activeLogEvent = new ActiveLogEvent(
                leadershipTermId, logPosition, maxLogPosition, memberId, logSessionId, logStreamId, isStartup,
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
            var clientSession = sessionByIdMap[clusterSessionId];

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

            ClientSession session = new ClientSession(
                clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

            if (ClusterRole.Leader == role && ctx.IsRespondingService())
            {
                session.Connect(aeron);
            }

            sessionByIdMap[clusterSessionId] = session;
            service.OnSessionOpen(session, timestamp);
        }

        internal void OnSessionClose(long leadershipTermId, long logPosition, long clusterSessionId, long timestamp,
            CloseReason closeReason)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;
            ClientSession session = sessionByIdMap[clusterSessionId];
            sessionByIdMap.Remove(clusterSessionId);

            if (null == session)
            {
                throw new ClusterException(
                    "unknown clusterSessionId=" + clusterSessionId + " for close reason=" + closeReason +
                    " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
            }

            session.Disconnect(ctx.CountedErrorHandler().OnError);
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
                ctx.TerminationHook()();
                return;
            }
            else
            {
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
        }

        internal void OnMembershipChange(long logPosition, long timestamp, ChangeType changeType, int memberId)
        {
            this.logPosition = logPosition;
            clusterTime = timestamp;

            if (memberId == this.memberId && changeType == ChangeType.QUIT)
            {
                Terminate();
            }
        }

        internal void AddSession(
            long clusterSessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal)
        {
            var session = new ClientSession(
                clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

            sessionByIdMap[clusterSessionId] = session;
        }

        internal void HandleError(Exception ex)
        {
            ctx.CountedErrorHandler().OnError(ex);
        }

        internal long Offer(long clusterSessionId, Publication publication, IDirectBuffer buffer, int offset,
            int length)
        {
            if (role != ClusterRole.Leader)
            {
                return ClientSession.MOCKED_OFFER;
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
            if (role != ClusterRole.Leader)
            {
                return ClientSession.MOCKED_OFFER;
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
            if (role != ClusterRole.Leader)
            {
                int maxPayloadLength = headerBuffer.Capacity - SESSION_HEADER_LENGTH;
                if (length > maxPayloadLength)
                {
                    throw new ArgumentException(
                        "claim exceeds maxPayloadLength of " + maxPayloadLength + ", length=" + length);
                }

                bufferClaim.Wrap(headerBuffer, 0, length + SESSION_HEADER_LENGTH);
                return ClientSession.MOCKED_OFFER;
            }

            if (null == publication)
            {
                return Publication.NOT_CONNECTED;
            }

            long offset = publication.TryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);
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

            if (NULL_VALUE != leadershipTermId)
            {
                LoadSnapshot(RecoveryState.GetSnapshotRecordingId(counters, recoveryCounterId, serviceId));
            }
            else
            {
                service.OnStart(this, null);
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
            Role = (ClusterRole) roleCounter.Get();
        }

        private void JoinActiveLog(ActiveLogEvent activeLog)
        {
            Subscription logSubscription = aeron.AddSubscription(activeLog.channel, activeLog.streamId);
            Role = (ClusterRole) roleCounter.Get();

            long id = ackId++;
            idleStrategy.Reset();
            while (!_consensusModuleProxy.Ack(activeLog.logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                Idle();
            }

            _sessionMessageHeaderEncoder.LeadershipTermId(activeLog.leadershipTermId);
            memberId = activeLog.memberId;
            ctx.MarkFile().MemberId(memberId);
            logAdapter.MaxLogPosition(activeLog.maxLogPosition);
            logAdapter.Image(AwaitImage(activeLog.sessionId, logSubscription));

            foreach (ClientSession session in sessionByIdMap.Values)
            {
                if (ClusterRole.Leader == role)
                {
                    if (ctx.IsRespondingService() && !activeLog.isStartup)
                    {
                        session.Connect(aeron);
                    }

                    session.ResetClosing();
                }
                else
                {
                    session.Disconnect(ctx.CountedErrorHandler().OnError);
                }
            }
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

        private ReadableCounter AwaitClusterRoleCounter(CountersReader counters, int clusterId)
        {
            idleStrategy.Reset();
            int counterId = ClusterCounters.Find(counters,
                ClusteredServiceContainer.Configuration.CLUSTER_NODE_ROLE_TYPE_ID, clusterId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = ClusterCounters.Find(counters,
                    ClusteredServiceContainer.Configuration.CLUSTER_NODE_ROLE_TYPE_ID, clusterId);
            }

            return new ReadableCounter(counters, counterId);
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
                int sessionId = (int) archive.StartReplay(recordingId, 0, NULL_VALUE, channel, streamId);

                string replaySessionChannel = ChannelUri.AddSessionId(channel, sessionId);
                using (Subscription subscription = aeron.AddSubscription(replaySessionChannel, streamId))
                {
                    Image image = AwaitImage(sessionId, subscription);
                    LoadState(image);
                    service.OnStart(this, image);
                }
            }
        }

        private void LoadState(Image image)
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
                    if (image.Closed)
                    {
                        throw new ClusterException("snapshot ended unexpectedly");
                    }
                }

                idleStrategy.Idle(fragments);
            }

            int appVersion = snapshotLoader.AppVersion();
            if (SemanticVersion.Major(ctx.AppVersion()) != SemanticVersion.Major(appVersion))
            {
                throw new ClusterException(
                    "incompatible version: " + SemanticVersion.ToString(ctx.AppVersion()) +
                    " snapshot=" + SemanticVersion.ToString(appVersion));
            }

            timeUnit = snapshotLoader.TimeUnit();
        }

        private long OnTakeSnapshot(long logPosition, long leadershipTermId)
        {
            long recordingId;

            using (AeronArchive archive = AeronArchive.Connect(ctx.ArchiveContext().Clone()))
            using (ExclusivePublication publication =
                aeron.AddExclusivePublication(ctx.SnapshotChannel(), ctx.SnapshotStreamId()))
            {
                string channel = ChannelUri.AddSessionId(ctx.SnapshotChannel(), publication.SessionId);
                archive.StartRecording(channel, ctx.SnapshotStreamId(), SourceLocation.LOCAL, true);
                CountersReader counters = aeron.CountersReader;
                int counterId = AwaitRecordingCounter(publication.SessionId, counters);
                recordingId = RecordingPos.GetRecordingId(counters, counterId);

                SnapshotState(publication, logPosition, leadershipTermId);
                CheckForClockTick();
                service.OnTakeSnapshot(publication);

                AwaitRecordingComplete(recordingId, publication.Position, counters, counterId, archive);
            }

            return recordingId;
        }

        private void AwaitRecordingComplete(
            long recordingId,
            long position,
            CountersReader counters,
            int counterId,
            AeronArchive archive)
        {
            idleStrategy.Reset();
            do
            {
                Idle();

                if (!RecordingPos.IsActive(counters, counterId, recordingId))
                {
                    throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
                }

                archive.CheckForErrorResponse();
            } while (counters.GetCounterValue(counterId) < position);
        }


        private void SnapshotState(ExclusivePublication publication, long logPosition, long leadershipTermId)
        {
            var snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, aeronAgentInvoker);

            snapshotTaker.MarkBegin(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0,
                timeUnit, ctx.AppVersion());

            foreach (ClientSession clientSession in sessionByIdMap.Values)
            {
                snapshotTaker.SnapshotSession(clientSession);
            }

            snapshotTaker.MarkEnd(ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0,
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

        private int AwaitRecordingCounter(int sessionId, CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = RecordingPos.FindCounterIdBySession(counters, sessionId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = RecordingPos.FindCounterIdBySession(counters, sessionId);
            }

            return counterId;
        }

        private static void CheckInterruptedStatus()
        {
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException("unexpected interrupt during operation");
            }
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

                try
                {
                    Thread.Sleep(0);
                }
                catch (ThreadInterruptedException)
                {
                    isAbort = true;
                    throw new AgentTerminationException("unexpected interrupt during operation");
                }

                if (null != aeronAgentInvoker)
                {
                    aeronAgentInvoker.Invoke();

                    if (isAbort || aeron.IsClosed)
                    {
                        throw new AgentTerminationException("unexpected Aeron close");
                    }
                }

                if (nowMs >= (timeOfLastMarkFileUpdateMs + MARK_FILE_UPDATE_INTERVAL_MS))
                {
                    ctx.MarkFile().UpdateActivityTimestamp(nowMs);
                    timeOfLastMarkFileUpdateMs = nowMs;
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
                Terminate();
            }
        }

        private void Terminate()
        {
            isServiceActive = false;

            try
            {
                service.OnTerminate(this);
            }
            catch (Exception ex)
            {
                ctx.CountedErrorHandler().OnError(ex);
            }

            long id = ackId++;
            while (!_consensusModuleProxy.Ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                Idle();
            }

            terminationPosition = NULL_VALUE;
            ctx.TerminationHook().Invoke();
        }

        private void Abort()
        {
            isAbort = true;

            try
            {
                ctx.AbortLatch().Wait(TimeSpan.FromMilliseconds(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L));
            }
            catch (ThreadInterruptedException)
            {
                Thread.CurrentThread.Interrupt();
            }
        }
    }
}