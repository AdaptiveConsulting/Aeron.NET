using System;
using System.Collections.Generic;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal sealed class ClusteredServiceAgent : IAgent, ICluster
    {
        private readonly int serviceId;
        private bool isRecovering;
        private readonly AeronArchive.Context archiveCtx;
        private readonly ClusteredServiceContainer.Context ctx;
        private readonly Aeron.Aeron aeron;
        private readonly Dictionary<long, ClientSession> sessionByIdMap = new Dictionary<long, ClientSession>();
        private readonly IClusteredService service;
        private readonly ConsensusModuleProxy _consensusModuleProxy;
        private readonly ServiceAdapter _serviceAdapter;
        private readonly IIdleStrategy idleStrategy;
        private readonly IEpochClock epochClock;
        private readonly ClusterMarkFile markFile;

        private long ackId = 0;
        private long clusterTimeMs;
        private long cachedTimeMs;
        private BoundedLogAdapter logAdapter;
        private ActiveLogEvent _activeLogEvent;
        private AtomicCounter heartbeatCounter;
        private ReadableCounter roleCounter;
        private ClusterRole role = ClusterRole.Follower;

        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            this.ctx = ctx;

            archiveCtx = ctx.ArchiveContext();
            aeron = ctx.Aeron();
            service = ctx.ClusteredService();
            idleStrategy = ctx.IdleStrategy();
            serviceId = ctx.ServiceId();
            epochClock = ctx.EpochClock();
            markFile = ctx.MarkFile();


            var channel = ctx.ServiceControlChannel();
            _consensusModuleProxy = new ConsensusModuleProxy(aeron.AddPublication(channel, ctx.ConsensusModuleStreamId()));
            _serviceAdapter = new ServiceAdapter(aeron.AddSubscription(channel, ctx.ServiceStreamId()), this);
        }

        public void OnStart()
        {
            CountersReader counters = aeron.CountersReader();
            roleCounter = AwaitClusterRoleCounter(counters);
            heartbeatCounter = AwaitHeartbeatCounter(counters);

            service.OnStart(this);
            isRecovering = true;
            int recoveryCounterId = AwaitRecoveryCounter(counters);
            heartbeatCounter.SetOrdered(epochClock.Time());
            CheckForSnapshot(counters, recoveryCounterId);
            CheckForReplay(counters, recoveryCounterId);
            isRecovering = false;
            service.OnReady();
        }

        public void OnClose()
        {
            if (!ctx.OwnsAeronClient())
            {
                logAdapter?.Dispose();
                _consensusModuleProxy?.Dispose();
                _serviceAdapter?.Dispose();

                foreach (ClientSession session in sessionByIdMap.Values)
                {
                    session.Disconnect();
                }
            }
        }

        public int DoWork()
        {
            int workCount = 0;

            long nowMs = epochClock.Time();
            if (cachedTimeMs != nowMs)
            {
                cachedTimeMs = nowMs;

                if (_consensusModuleProxy.IsConnected())
                {
                    markFile.UpdateActivityTimestamp(nowMs);
                    heartbeatCounter.SetOrdered(nowMs);
                }
                else
                {
                    ctx.ErrorHandler()(new ClusterException("Consensus Module not connected"));
                    ctx.TerminationHook().Invoke();
                }

                workCount += _serviceAdapter.Poll();

                if (null != _activeLogEvent && null == logAdapter)
                {
                    JoinActiveLog();
                }
            }

            if (null != logAdapter)
            {
                int polled = logAdapter.Poll();

                if (0 == polled)
                {
                    if (logAdapter.IsConsumed(aeron.CountersReader()))
                    {
                        _consensusModuleProxy.Ack(logAdapter.Position(), ackId++, serviceId);
                        logAdapter.Dispose();
                        logAdapter = null;
                    }
                    else if (logAdapter.IsImageClosed())
                    {
                        logAdapter.Dispose();
                        logAdapter = null;
                    }
                }

                workCount += polled;
            }

            return workCount;
        }

        public string RoleName()
        {
            return ctx.ServiceName();
        }

        public ClusterRole Role()
        {
            return role;
        }

        public Aeron.Aeron Aeron()
        {
            return aeron;
        }

        public ClientSession GetClientSession(long clusterSessionId)
        {
            return sessionByIdMap[clusterSessionId];
        }

        public ICollection<ClientSession> GetClientSessions()
        {
            return sessionByIdMap.Values;
        }

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

        public long TimeMs()
        {
            return clusterTimeMs;
        }

        public bool ScheduleTimer(long correlationId, long deadlineMs)
        {
            return _consensusModuleProxy.ScheduleTimer(correlationId, deadlineMs);
        }

        public bool CancelTimer(long correlationId)
        {
            return _consensusModuleProxy.CancelTimer(correlationId);
        }

        public void Idle()
        {
            CheckInterruptedStatus();
            idleStrategy.Idle();
        }

        public void OnJoinLog(
            long leadershipTermId,
            int commitPositionId,
            int logSessionId,
            int logStreamId,
            string logChannel)
        {
            _activeLogEvent = new ActiveLogEvent(
                leadershipTermId, commitPositionId, logSessionId, logStreamId, logChannel);
        }

        internal void OnSessionMessage(long clusterSessionId, long correlationId, long timestampMs,
            IDirectBuffer buffer, int offset, int length, Header header)
        {
            clusterTimeMs = timestampMs;
            var clientSession = sessionByIdMap[clusterSessionId];

            try
            {
                service.OnSessionMessage(clientSession, correlationId, timestampMs, buffer, offset, length, header);
            }
            finally
            {
                clientSession.LastCorrelationId(correlationId);
            }
        }

        internal void OnTimerEvent(long correlationId, long timestampMs)
        {
            clusterTimeMs = timestampMs;

            service.OnTimerEvent(correlationId, timestampMs);
        }

        internal void OnSessionOpen(
            long clusterSessionId,
            long correlationId,
            long timestampMs,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal)
        {
            clusterTimeMs = timestampMs;

            ClientSession session = new ClientSession(
                clusterSessionId, correlationId, responseStreamId, responseChannel, encodedPrincipal, this);

            if (ClusterRole.Leader == role)
            {
                session.Connect(aeron);
            }

            sessionByIdMap[clusterSessionId] = session;
            service.OnSessionOpen(session, timestampMs);
        }

        internal void OnSessionClose(long clusterSessionId, long timestampMs, CloseReason closeReason)
        {
            clusterTimeMs = timestampMs;

            var session = sessionByIdMap[clusterSessionId];
            sessionByIdMap.Remove(clusterSessionId);
            session.Disconnect();
            service.OnSessionClose(session, timestampMs, closeReason);
        }

        internal void OnServiceAction(long logPosition, long leadershipTermId, long timestampMs, ClusterAction action)
        {
            clusterTimeMs = timestampMs;

            ExecuteAction(action, logPosition, leadershipTermId);
        }

        internal void OnNewLeadershipTermEvent(
            long leadershipTermId,
            long logPosition,
            long timestampMs,
            int leaderMemberId,
            int logSessionId)
        {
            clusterTimeMs = timestampMs;
        }

        internal void AddSession(
            long clusterSessionId,
            long lastCorrelationId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal)
        {
            var session = new ClientSession(
                clusterSessionId, lastCorrelationId, responseStreamId, responseChannel, encodedPrincipal, this);

            sessionByIdMap[clusterSessionId] = session;
        }

        private void Role(ClusterRole newRole)
        {
            if (newRole != role)
            {
                role = newRole;
                service.OnRoleChange(newRole);
            }
        }

        private void CheckForSnapshot(CountersReader counters, int recoveryCounterId)
        {
            long leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);
            clusterTimeMs = RecoveryState.GetTimestamp(counters, recoveryCounterId);

            if (Adaptive.Aeron.Aeron.NULL_VALUE != leadershipTermId)
            {
                LoadSnapshot(RecoveryState.GetSnapshotRecordingId(counters, recoveryCounterId, serviceId));
            }

            heartbeatCounter.SetOrdered(epochClock.Time());
            _consensusModuleProxy.Ack(RecoveryState.GetLogPosition(counters, recoveryCounterId), ackId++, serviceId);
        }


        private void CheckForReplay(CountersReader counters, int recoveryCounterId)
        {
            if (RecoveryState.HasReplay(counters, recoveryCounterId))
            {
                service.OnReplayBegin();
                AwaitActiveLog();

                int counterId = _activeLogEvent.commitPositionId;

                using (Subscription subscription = aeron.AddSubscription(_activeLogEvent.channel, _activeLogEvent.streamId))
                {
                    _consensusModuleProxy.Ack(CommitPos.GetLogPosition(counters, counterId), ackId++, serviceId);

                    Image image = AwaitImage(_activeLogEvent.sessionId, subscription);
                    ReadableCounter limit = new ReadableCounter(counters, counterId);
                    BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

                    ConsumeImage(image, adapter);
                }

                _activeLogEvent = null;
                heartbeatCounter.SetOrdered(epochClock.Time());
                service.OnReplayEnd();
            }
        }

        private void AwaitActiveLog()
        {
            idleStrategy.Reset();

            while (null == _activeLogEvent)
            {
                _serviceAdapter.Poll();
                CheckInterruptedStatus();
                idleStrategy.Idle();
            }
        }

        private void ConsumeImage(Image image, BoundedLogAdapter adapter)
        {
            while (true)
            {
                int workCount = adapter.Poll();
                if (workCount == 0)
                {
                    if (adapter.IsConsumed(aeron.CountersReader()))
                    {
                        _consensusModuleProxy.Ack(image.Position(), ackId++, serviceId);
                        break;
                    }

                    if (image.Closed)
                    {
                        throw new ClusterException("unexpected close of replay");
                    }
                }

                CheckInterruptedStatus();
                idleStrategy.Idle(workCount);
            }
        }

        private int AwaitRecoveryCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = RecoveryState.FindCounterId(counters);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();

                counterId = RecoveryState.FindCounterId(counters);
            }

            return counterId;
        }

        private void JoinActiveLog()
        {
            CountersReader counters = aeron.CountersReader();
            int commitPositionId = _activeLogEvent.commitPositionId;
            if (!CommitPos.IsActive(counters, commitPositionId))
            {
                throw new ClusterException("CommitPos counter not active: " + commitPositionId);
            }

            Subscription logSubscription = aeron.AddSubscription(_activeLogEvent.channel, _activeLogEvent.streamId);
            _consensusModuleProxy.Ack(CommitPos.GetLogPosition(counters, commitPositionId), ackId++, serviceId);

            Image image = AwaitImage(_activeLogEvent.sessionId, logSubscription);
            heartbeatCounter.SetOrdered(epochClock.Time());

            _activeLogEvent = null;
            logAdapter = new BoundedLogAdapter(image, new ReadableCounter(counters, commitPositionId), this);

            Role((ClusterRole) roleCounter.Get());

            foreach (ClientSession session in sessionByIdMap.Values)
            {
                if (ClusterRole.Leader == role)
                {
                    session.Connect(aeron);
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
                CheckInterruptedStatus();
                idleStrategy.Idle();
            }

            return image;
        }

        private ReadableCounter AwaitClusterRoleCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = ClusterNodeRole.FindCounterId(counters);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
                counterId = ClusterNodeRole.FindCounterId(counters);
            }

            return new ReadableCounter(counters, counterId);
        }
        
        private AtomicCounter AwaitHeartbeatCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = ServiceHeartbeat.FindCounterId(counters, ctx.ServiceId());
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
                counterId = ServiceHeartbeat.FindCounterId(counters, ctx.ServiceId());
            }

            return new AtomicCounter(counters.ValuesBuffer, counterId);
        }

        private void LoadSnapshot(long recordingId)
        {
            using (AeronArchive archive = AeronArchive.Connect(archiveCtx))
            {
                string channel = ctx.ReplayChannel();
                int streamId = ctx.ReplayStreamId();
                int sessionId = (int) archive.StartReplay(recordingId, 0, Adaptive.Aeron.Aeron.NULL_VALUE, channel, streamId);

                string replaySessionChannel = ChannelUri.AddSessionId(channel, sessionId);
                using (Subscription subscription = aeron.AddSubscription(replaySessionChannel, streamId))
                {
                    Image image = AwaitImage(sessionId, subscription);
                    LoadState(image);
                    service.OnLoadSnapshot(image);
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
                    CheckInterruptedStatus();

                    if (image.Closed)
                    {
                        throw new ClusterException("snapshot ended unexpectedly");
                    }

                    idleStrategy.Idle(fragments);
                }
            }
        }

        private long OnTakeSnapshot(long logPosition, long leadershipTermId)
        {
            long recordingId;

            using (AeronArchive archive = AeronArchive.Connect(archiveCtx)) 
            using(Publication publication = archive.AddRecordedExclusivePublication(ctx.SnapshotChannel(), ctx.SnapshotStreamId()))
            {
                try
                {
                    CountersReader counters = aeron.CountersReader();
                    int counterId = AwaitRecordingCounter(publication.SessionId, counters);

                    recordingId = RecordingPos.GetRecordingId(counters, counterId);
                    SnapshotState(publication, logPosition, leadershipTermId);
                    service.OnTakeSnapshot(publication);

                    AwaitRecordingComplete(recordingId, publication.Position, counters, counterId, archive);
                }
                finally
                {
                    archive.StopRecording(publication);
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
                idleStrategy.Idle();
                CheckInterruptedStatus();

                if (!RecordingPos.IsActive(counters, counterId, recordingId))
                {
                    throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
                }

                archive.CheckForErrorResponse();
            } while (counters.GetCounterValue(counterId) < position);
        }


        private void SnapshotState(Publication publication, long logPosition, long leadershipTermId)
        {
            var snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, null);

            snapshotTaker.MarkBegin(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

            foreach (ClientSession clientSession in sessionByIdMap.Values)
            {
                snapshotTaker.SnapshotSession(clientSession);
            }

            snapshotTaker.MarkEnd(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
        }

        private void ExecuteAction(ClusterAction action, long position, long leadershipTermId)
        {
            if (isRecovering)
            {
                return;
            }

            switch (action)
            {
                case ClusterAction.SNAPSHOT:
                    _consensusModuleProxy.Ack(position, ackId++, OnTakeSnapshot(position, leadershipTermId), serviceId);
                    break;

                case ClusterAction.SHUTDOWN:
                    _consensusModuleProxy.Ack(position, ackId++, OnTakeSnapshot(position, leadershipTermId), serviceId);
                    ctx.TerminationHook().Invoke();
                    break;

                case ClusterAction.ABORT:
                    _consensusModuleProxy.Ack(position, ackId++, serviceId);
                    ctx.TerminationHook().Invoke();
                    break;
            }
        }

        private int AwaitRecordingCounter(int sessionId, CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = RecordingPos.FindCounterIdBySession(counters, sessionId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
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
    }
}