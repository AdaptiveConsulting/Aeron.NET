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
using Adaptive.Archiver.Codecs;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using MessageHeaderEncoder = Adaptive.Cluster.Codecs.MessageHeaderEncoder;

namespace Adaptive.Cluster.Service
{
    internal sealed class ClusteredServiceAgent : IAgent, ICluster
    {
        public static readonly int SESSION_HEADER_LENGTH = 
            MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;
        
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
        private readonly DirectBufferVector[] _vectors = new DirectBufferVector[2];
        private readonly DirectBufferVector _messageVector = new DirectBufferVector();
        private readonly EgressMessageHeaderEncoder _egressMessageHeaderEncoder = new EgressMessageHeaderEncoder();

        private long ackId = 0;
        private long clusterTimeMs;
        private long cachedTimeMs;
        private int memberId = Adaptive.Aeron.Aeron.NULL_VALUE;
        private BoundedLogAdapter logAdapter;
        private AtomicCounter heartbeatCounter;
        private ReadableCounter roleCounter;
        private ReadableCounter commitPosition;
        private ActiveLogEvent activeLogEvent;
        private ClusterRole role = ClusterRole.Follower;
        private string logChannel = null;

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
            
            UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
            _egressMessageHeaderEncoder.WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());

            _vectors[0] = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
            _vectors[1] = _messageVector;
        }

        public void OnStart()
        {
            CountersReader counters = aeron.CountersReader;
            roleCounter = AwaitClusterRoleCounter(counters);
            heartbeatCounter = AwaitHeartbeatCounter(counters);
            commitPosition = AwaitCommitPositionCounter(counters);

            service.OnStart(this);
            
            isRecovering = true;
            
            int recoveryCounterId = AwaitRecoveryCounter(counters);
            heartbeatCounter.SetOrdered(epochClock.Time());
            CheckForSnapshot(counters, recoveryCounterId);
            CheckForReplay(counters, recoveryCounterId);

            isRecovering = false;
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

        public int MemberId()
        {
            return memberId;
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
            CheckForClockTick();
            idleStrategy.Idle();
        }

        public void Idle(int workCount)
        {
            CheckInterruptedStatus();
            CheckForClockTick();
            idleStrategy.Idle(workCount);
        }
        
        public long Offer(
            long clusterSessionId,
            Publication publication,
            IDirectBuffer buffer,
            int offset,
            int length)
        {
            if (role != ClusterRole.Leader)
            {
                return ClientSession.MOCKED_OFFER;
            }

            _egressMessageHeaderEncoder
                .ClusterSessionId(clusterSessionId)
                .Timestamp(clusterTimeMs);

            _messageVector.Reset(buffer, offset, length);

            return publication.Offer(_vectors);
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
                logAdapter.Dispose();
                logAdapter = null;
            }
            
            activeLogEvent = new ActiveLogEvent(
                leadershipTermId, logPosition, maxLogPosition, memberId, logSessionId, logStreamId, logChannel);
        }

        internal void OnSessionMessage(
            long clusterSessionId, 
            long timestampMs, 
            IDirectBuffer buffer, 
            int offset, 
            int length, 
            Header header)
        {
            clusterTimeMs = timestampMs;
            var clientSession = sessionByIdMap[clusterSessionId];
            
            service.OnSessionMessage(clientSession, timestampMs, buffer, offset, length, header);
        }

        internal void OnTimerEvent(long correlationId, long timestampMs)
        {
            clusterTimeMs = timestampMs;
            service.OnTimerEvent(correlationId, timestampMs);
        }

        internal void OnSessionOpen(
            long clusterSessionId,
            long timestampMs,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal)
        {
            clusterTimeMs = timestampMs;

            ClientSession session = new ClientSession(
                clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

            if (ClusterRole.Leader == role && ctx.IsRespondingService())
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
            _egressMessageHeaderEncoder.LeadershipTermId(leadershipTermId);
            clusterTimeMs = timestampMs;
        }
        
        internal void OnClusterChange(
            long leadershipTermId,
            long logPosition,
            long timestampMs,
            int leaderMemberId,
            int clusterSize,
            ChangeType eventType,
            int memberId,
            string clusterMembers)
        {
            clusterTimeMs = timestampMs;

            if (memberId == this.memberId && eventType == ChangeType.LEAVE)
            {
                _consensusModuleProxy.Ack(logPosition, ackId++, serviceId);
                ctx.TerminationHook().Invoke();
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
            clusterTimeMs = RecoveryState.GetTimestamp(counters, recoveryCounterId);
            long leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);

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
                AwaitActiveLog();

                using (Subscription subscription = aeron.AddSubscription(activeLogEvent.channel, activeLogEvent.streamId))
                {
                    _consensusModuleProxy.Ack(activeLogEvent.logPosition, ackId++, serviceId);

                    Image image = AwaitImage(activeLogEvent.sessionId, subscription);
                    BoundedLogAdapter adapter = new BoundedLogAdapter(image, commitPosition, this);

                    ConsumeImage(image, adapter, activeLogEvent.maxLogPosition);
                }

                activeLogEvent = null;
                heartbeatCounter.SetOrdered(epochClock.Time());
            }
        }

        private void AwaitActiveLog()
        {
            idleStrategy.Reset();

            while (null == activeLogEvent)
            {
                _serviceAdapter.Poll();
                CheckInterruptedStatus();
                idleStrategy.Idle();
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
                        _consensusModuleProxy.Ack(image.Position, ackId++, serviceId);
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
                CheckInterruptedStatus();
                idleStrategy.Idle();
                
                heartbeatCounter.SetOrdered(epochClock.Time());
                counterId = RecoveryState.FindCounterId(counters);
            }

            return counterId;
        }

        private void JoinActiveLog()
        {
            Subscription logSubscription = aeron.AddSubscription(activeLogEvent.channel, activeLogEvent.streamId);
            _consensusModuleProxy.Ack(activeLogEvent.logPosition, ackId++, serviceId);

            Image image = AwaitImage(activeLogEvent.sessionId, logSubscription);
            heartbeatCounter.SetOrdered(epochClock.Time());

            _egressMessageHeaderEncoder.LeadershipTermId(activeLogEvent.leadershipTermId);
            memberId = activeLogEvent.memberId;
            ctx.MarkFile().MemberId(memberId);
            logChannel = activeLogEvent.channel;
            activeLogEvent = null;
            logAdapter = new BoundedLogAdapter(image, commitPosition, this);

            Role((ClusterRole) roleCounter.Get());

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
        
        private ReadableCounter AwaitCommitPositionCounter(CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = CommitPos.FindCounterId(counters);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
                heartbeatCounter.SetOrdered(epochClock.Time());
                counterId = CommitPos.FindCounterId(counters);
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
            using(Publication publication = aeron.AddExclusivePublication(ctx.SnapshotChannel(), ctx.SnapshotStreamId()))
            {
                var channel = ChannelUri.AddSessionId(ctx.SnapshotChannel(), publication.SessionId);
                long subscriptionId = archive.StartRecording(channel, ctx.SnapshotStreamId(), SourceLocation.LOCAL);
                
                try
                {
                    CountersReader counters = aeron.CountersReader;
                    int counterId = AwaitRecordingCounter(publication.SessionId, counters);

                    recordingId = RecordingPos.GetRecordingId(counters, counterId);
                    SnapshotState(publication, logPosition, leadershipTermId);
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

        private bool CheckForClockTick()
        {
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
        }
    }
}