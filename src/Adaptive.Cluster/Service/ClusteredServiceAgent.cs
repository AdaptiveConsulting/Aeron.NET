using System;
using System.Collections.Generic;
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
    internal sealed class ClusteredServiceAgent : IAgent, ICluster
    {
        private static readonly int MAX_UDP_PAYLOAD_LENGTH = 65504;

        private readonly int serviceId;
        private int memberId = NULL_VALUE;
        private long ackId = 0;
        private long cachedTimeMs;
        private long clusterTime;
        private long clusterLogPosition = NULL_POSITION;
        private long terminationPosition = NULL_POSITION;
        private long roleChangePosition = NULL_POSITION;
        private bool isServiceActive;
        private volatile bool isAbort;
        
        private readonly AeronArchive.Context archiveCtx;
        private readonly ClusteredServiceContainer.Context ctx;
        private readonly Aeron.Aeron aeron;
        private readonly AgentInvoker aeronAgentInvoker;
        private readonly Dictionary<long, ClientSession> sessionByIdMap = new Dictionary<long, ClientSession>();
        private readonly IClusteredService service;
        private readonly ConsensusModuleProxy _consensusModuleProxy;
        private readonly ServiceAdapter _serviceAdapter;
        private readonly IIdleStrategy idleStrategy;
        private readonly IEpochClock epochClock;
        private readonly ClusterMarkFile markFile;
        private readonly UnsafeBuffer headerBuffer =
            new UnsafeBuffer(new byte[MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH]);
        private readonly DirectBufferVector headerVector;
        private readonly SessionMessageHeaderEncoder _sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private readonly Action abortHandler;

        private BoundedLogAdapter logAdapter;
        private ReadableCounter roleCounter;
        private ReadableCounter commitPosition;
        private ActiveLogEvent activeLogEvent;
        private ClusterRole role = ClusterRole.Follower;
        private string logChannel = null;
        private ClusterTimeUnit timeUnit = ClusterTimeUnit.NULL_VALUE;

        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            this.ctx = ctx;

            headerVector = new DirectBufferVector(headerBuffer, 0, headerBuffer.Capacity);

            abortHandler = Abort;
            archiveCtx = ctx.ArchiveContext();
            aeron = ctx.Aeron();
            aeronAgentInvoker = ctx.Aeron().ConductorAgentInvoker;
            service = ctx.ClusteredService();
            idleStrategy = ctx.IdleStrategy();
            serviceId = ctx.ServiceId();
            epochClock = ctx.EpochClock();
            markFile = ctx.MarkFile();


            var channel = ctx.ServiceControlChannel();
            _consensusModuleProxy =
                new ConsensusModuleProxy(aeron.AddPublication(channel, ctx.ConsensusModuleStreamId()));
            _serviceAdapter = new ServiceAdapter(aeron.AddSubscription(channel, ctx.ServiceStreamId()), this);
            _sessionMessageHeaderEncoder.WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
            aeron.AddCloseHandler(abortHandler);
        }

        public void OnStart()
        {
            CountersReader counters = aeron.CountersReader;
            roleCounter = AwaitClusterRoleCounter(counters);
            commitPosition = AwaitCommitPositionCounter(counters);

            int recoveryCounterId = AwaitRecoveryCounter(counters);

            isServiceActive = true;
            CheckForSnapshot(counters, recoveryCounterId);
            CheckForReplay(counters, recoveryCounterId);
        }

        public void OnClose()
        {
            if (isAbort)
            {
                ctx.AbortLatch().Signal();
            }
            else
            {
                aeron.RemoveCloseHandler(abortHandler);

                if (isServiceActive)
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
                }

                if (!ctx.OwnsAeronClient())
                {
                    foreach (var session in sessionByIdMap.Values)
                    {
                        session.Disconnect();
                    }

                    logAdapter?.Dispose();
                    _serviceAdapter?.Dispose();
                    _consensusModuleProxy?.Dispose();
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

            if (null != logAdapter)
            {
                int polled = logAdapter.Poll();

                if (0 == polled)
                {
                    if (logAdapter.IsDone())
                    {
                        CheckPosition(logAdapter.Position(), activeLogEvent);
                        logAdapter.Dispose();
                        logAdapter = null;
                    }
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

        public bool CloseSession(long clusterSessionId)
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
            return clusterLogPosition;
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

        public void Idle()
        {
            CheckForClockTick();
            idleStrategy.Idle();
        }

        public void Idle(int workCount)
        {
            CheckForClockTick();
            idleStrategy.Idle(workCount);
        }

        public void OnJoinLog(
            long leadershipTermId,
            long logPosition,
            long maxLogPosition,
            int memberId,
            int logSessionId,
            int logStreamId,
            string logChannel)
        {
            if (null != logAdapter && !logChannel.Equals(this.logChannel))
            {
                long existingPosition = logAdapter.Position();
                if (existingPosition != logPosition)
                {
                    throw new ClusterException("existing position " + existingPosition + " new position " +
                                               logPosition);
                }

                logAdapter.Dispose();
                logAdapter = null;
            }

            roleChangePosition = NULL_POSITION;
            activeLogEvent = new ActiveLogEvent(
                leadershipTermId, logPosition, maxLogPosition, memberId, logSessionId, logStreamId, logChannel);
        }

        public void OnServiceTerminationPosition(long logPosition)
        {
            terminationPosition = logPosition;
        }

        public void OnElectionStartEvent(long logPosition)
        {
            roleChangePosition = logPosition;
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
            clusterLogPosition = logPosition;
            clusterTime = timestamp;
            var clientSession = sessionByIdMap[clusterSessionId];

            service.OnSessionMessage(clientSession, timestamp, buffer, offset, length, header);
        }

        internal void OnTimerEvent(long logPosition, long correlationId, long timestampMs)
        {
            clusterLogPosition = logPosition;
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
            clusterLogPosition = logPosition;
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
            clusterLogPosition = logPosition;
            clusterTime = timestamp;
            ClientSession session = sessionByIdMap[clusterSessionId];
            sessionByIdMap.Remove(clusterSessionId);

            if (null == session)
            {
                throw new ClusterException(
                    "unknown clusterSessionId=" + clusterSessionId + " for close reason=" + closeReason +
                    " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
            }

            session.Disconnect();
            service.OnSessionClose(session, timestamp, closeReason);
        }

        internal void OnServiceAction(long leadershipTermId, long logPosition, long timestamp, ClusterAction action)
        {
            clusterLogPosition = logPosition;
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

            _sessionMessageHeaderEncoder.LeadershipTermId(leadershipTermId);
            clusterLogPosition = logPosition;
            clusterTime = timestamp;
            this.timeUnit = timeUnit;
        }

        internal void OnMembershipChange(
            long leadershipTermId,
            long logPosition,
            long timestamp,
            int leaderMemberId,
            int clusterSize,
            ChangeType changeType,
            int memberId,
            string clusterMembers)
        {
            clusterLogPosition = logPosition;
            clusterTime = timestamp;

            if (memberId == this.memberId && changeType == ChangeType.QUIT)
            {
                Terminate(logPosition);
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
                bufferClaim.Wrap(headerBuffer, 0, length);
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

        private void CheckForSnapshot(CountersReader counters, int recoveryCounterId)
        {
            clusterLogPosition = RecoveryState.GetLogPosition(counters, recoveryCounterId);
            clusterTime = RecoveryState.GetTimestamp(counters, recoveryCounterId);
            long leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);

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
            while (!_consensusModuleProxy.Ack(clusterLogPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                Idle();
            }
        }


        private void CheckForReplay(CountersReader counters, int recoveryCounterId)
        {
            if (RecoveryState.HasReplay(counters, recoveryCounterId))
            {
                AwaitActiveLog();

                using (Subscription subscription =
                    aeron.AddSubscription(activeLogEvent.channel, activeLogEvent.streamId))
                {
                    long id = ackId++;
                    idleStrategy.Reset();
                    while (!_consensusModuleProxy.Ack(activeLogEvent.logPosition, clusterTime, id, NULL_VALUE,
                        serviceId))
                    {
                        Idle();
                    }

                    Image image = AwaitImage(activeLogEvent.sessionId, subscription);
                    BoundedLogAdapter adapter = new BoundedLogAdapter(image, commitPosition, this);

                    ConsumeImage(image, adapter, activeLogEvent.maxLogPosition);
                }

                activeLogEvent = null;
            }
        }

        private void AwaitActiveLog()
        {
            idleStrategy.Reset();

            while (null == activeLogEvent)
            {
                Idle();
                _serviceAdapter.Poll();
            }
        }

        private void ConsumeImage(Image image, BoundedLogAdapter adapter, long maxLogPosition)
        {
            while (true)
            {
                int workCount = adapter.Poll();
                if (workCount == 0)
                {
                    if (adapter.Position() >= maxLogPosition)
                    {
                        long id = ackId++;
                        while (!_consensusModuleProxy.Ack(image.Position, clusterTime, id, NULL_VALUE, serviceId))
                        {
                            Idle();
                        }

                        break;
                    }

                    if (image.Closed)
                    {
                        throw new ClusterException("unexpected close of replay");
                    }
                }

                Idle(workCount);
            }
        }

        private int AwaitRecoveryCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = RecoveryState.FindCounterId(counters);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = RecoveryState.FindCounterId(counters);
            }

            return counterId;
        }

        private void JoinActiveLog()
        {
            Subscription logSubscription = aeron.AddSubscription(activeLogEvent.channel, activeLogEvent.streamId);

            long id = ackId++;
            idleStrategy.Reset();
            while (!_consensusModuleProxy.Ack(activeLogEvent.logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                Idle();
            }

            Image image = AwaitImage(activeLogEvent.sessionId, logSubscription);

            _sessionMessageHeaderEncoder.LeadershipTermId(activeLogEvent.leadershipTermId);
            memberId = activeLogEvent.memberId;
            ctx.MarkFile().MemberId(memberId);
            logChannel = activeLogEvent.channel;
            activeLogEvent = null;
            logAdapter = new BoundedLogAdapter(image, commitPosition, this);

            Role = (ClusterRole) roleCounter.Get();

            foreach (ClientSession session in sessionByIdMap.Values)
            {
                if (ClusterRole.Leader == role)
                {
                    if (ctx.IsRespondingService())
                    {
                        session.Connect(aeron);
                    }

                    session.ResetClosing();
                }
                else
                {
                    session.Disconnect();
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

        private ReadableCounter AwaitClusterRoleCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = ClusterNodeRole.FindCounterId(counters);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = ClusterNodeRole.FindCounterId(counters);
            }

            return new ReadableCounter(counters, counterId);
        }

        private ReadableCounter AwaitCommitPositionCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = CommitPos.FindCounterId(counters);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Idle();
                counterId = CommitPos.FindCounterId(counters);
            }

            return new ReadableCounter(counters, counterId);
        }

        private void LoadSnapshot(long recordingId)
        {
            using (AeronArchive archive = Connect(archiveCtx.Clone()))
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

            using (AeronArchive archive = Connect(archiveCtx.Clone()))
            using (Publication publication =
                aeron.AddExclusivePublication(ctx.SnapshotChannel(), ctx.SnapshotStreamId()))
            {
                var channel = ChannelUri.AddSessionId(ctx.SnapshotChannel(), publication.SessionId);
                long subscriptionId = archive.StartRecording(channel, ctx.SnapshotStreamId(), SourceLocation.LOCAL);

                try
                {
                    CountersReader counters = aeron.CountersReader;
                    int counterId = AwaitRecordingCounter(publication.SessionId, counters);

                    recordingId = RecordingPos.GetRecordingId(counters, counterId);
                    SnapshotState(publication, logPosition, leadershipTermId);

                    CheckForClockTick();
                    service.OnTakeSnapshot(publication);

                    AwaitRecordingComplete(recordingId, publication.Position, counters, counterId, archive);
                }
                finally
                {
                    archive.StopRecording(subscriptionId);
                }
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


        private void SnapshotState(Publication publication, long logPosition, long leadershipTermId)
        {
            var snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, aeronAgentInvoker);

            snapshotTaker.MarkBegin(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0,
                timeUnit, ctx.AppVersion());

            foreach (ClientSession clientSession in sessionByIdMap.Values)
            {
                snapshotTaker.SnapshotSession(clientSession);
            }

            snapshotTaker.MarkEnd(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0,
                timeUnit, ctx.AppVersion());
        }

        private void ExecuteAction(ClusterAction action, long position, long leadershipTermId)
        {
            if (ClusterAction.SNAPSHOT == action)
            {
                long recordingId = OnTakeSnapshot(position, leadershipTermId);
                long id = ackId++;
                idleStrategy.Reset();
                while (!_consensusModuleProxy.Ack(position, clusterTime, id, recordingId, serviceId))
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
            if (isAbort)
            {
                throw new AgentTerminationException("unexpected Aeron close");
            }

            long nowMs = epochClock.Time();
            if (cachedTimeMs != nowMs)
            {
                cachedTimeMs = nowMs;

                CheckInterruptedStatus(); // TODO check how expensive, is equivalent to Thread.currentThread().isInterrupted()?

                if (null != aeronAgentInvoker)
                {
                    aeronAgentInvoker.Invoke();

                    if (isAbort)
                    {
                        throw new AgentTerminationException("unexpected Aeron close");
                    }
                }

                markFile.UpdateActivityTimestamp(nowMs);

                return true;
            }

            return false;
        }

        private void PollServiceAdapter()
        {
            _serviceAdapter.Poll();

            if (null != activeLogEvent && null == logAdapter)
            {
                JoinActiveLog();
            }

            if (NULL_POSITION != terminationPosition)
            {
                CheckForTermination();
            }

            if (NULL_POSITION != roleChangePosition)
            {
                CheckForRoleChange();
            }
        }

        private void CheckForTermination()
        {
            if (null != logAdapter && logAdapter.Position() >= terminationPosition)
            {
                var logPosition = terminationPosition;
                terminationPosition = NULL_POSITION;
                Terminate(logPosition);
            }
        }

        private void CheckForRoleChange()
        {
            if (null != logAdapter && logAdapter.Position() >= roleChangePosition)
            {
                roleChangePosition = NULL_VALUE;
                Role = (ClusterRole) roleCounter.Get();
            }
        }


        private void Terminate(long logPosition)
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

            ctx.TerminationHook().Invoke();
        }

        private void Abort()
        {
            isAbort = true;

            try
            {
                ctx.AbortLatch().Wait(TimeSpan.FromMilliseconds(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 2L));
            }
            catch (ThreadInterruptedException)
            {
                Thread.CurrentThread.Interrupt();
            }
        }

        private static void CheckPosition(long existingPosition, ActiveLogEvent activeLogEvent)
        {
            if (null != activeLogEvent && existingPosition != activeLogEvent.logPosition)
            {
                throw new ClusterException("existing position " + existingPosition + " new position " +
                                           activeLogEvent.logPosition);
            }
        }
    }
}