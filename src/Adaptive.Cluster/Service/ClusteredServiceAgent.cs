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
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal sealed class ClusteredServiceAgent : IAgent, ICluster, IServiceControlListener
    {
        private readonly int serviceId;
        private bool isRecovering;
        private readonly bool shouldCloseResources;
        private readonly AeronArchive.Context archiveCtx;
        private readonly ClusteredServiceContainer.Context ctx;
        private readonly Aeron.Aeron aeron;
        private readonly Dictionary<long, ClientSession> sessionByIdMap = new Dictionary<long, ClientSession>();
        private readonly IClusteredService service;
        private readonly ServiceControlPublisher serviceControlPublisher;
        private readonly ServiceControlAdapter serviceControlAdapter;
        private readonly IIdleStrategy idleStrategy;
        private readonly RecordingLog recordingLog;
        private readonly IEpochClock epochClock;
        private readonly CachedEpochClock cachedEpochClock = new CachedEpochClock();
        private readonly ClusterMarkFile markFile;

        private long termBaseLogPosition;
        private long leadershipTermId;
        private long timestampMs;
        private BoundedLogAdapter logAdapter;
        private ActiveLog activeLog;
        private ReadableCounter roleCounter;
        private AtomicCounter heartbeatCounter;
        private ClusterRole role = ClusterRole.Follower;

        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            this.ctx = ctx;

            archiveCtx = ctx.ArchiveContext();
            aeron = ctx.Aeron();
            shouldCloseResources = ctx.OwnsAeronClient();
            service = ctx.ClusteredService();
            recordingLog = ctx.RecordingLog();
            idleStrategy = ctx.IdleStrategy();
            serviceId = ctx.ServiceId();
            epochClock = ctx.EpochClock();
            markFile = ctx.MarkFile();


            var channel = ctx.ServiceControlChannel();
            var streamId = ctx.ServiceControlStreamId();
            
            serviceControlPublisher = new ServiceControlPublisher(aeron.AddPublication(channel, streamId));
            serviceControlAdapter = new ServiceControlAdapter(aeron.AddSubscription(channel, streamId), this);
        }

        public void OnStart()
        {
            service.OnStart(this);

            CountersReader counters = aeron.CountersReader();
            int recoveryCounterId = AwaitRecoveryCounterId(counters);
            FindHeartbeatCounter(counters);

            isRecovering = true;
            CheckForSnapshot(counters, recoveryCounterId);
            CheckForReplay(counters, recoveryCounterId);
            isRecovering = false;

            service.OnReady();
            
            JoinActiveLog(counters);

            roleCounter = AwaitClusterRoleCounter(counters);
            role = (ClusterRole) roleCounter.Get();

            if (ClusterRole.Leader == role)
            {
                foreach (ClientSession session in sessionByIdMap.Values)
                {
                    session.Connect(aeron);
                }
            }
        }

        public void OnClose()
        {
            if (shouldCloseResources)
            {
                logAdapter?.Dispose();
                serviceControlPublisher?.Dispose();
                serviceControlAdapter?.Dispose();

                foreach (ClientSession session in sessionByIdMap.Values)
                {
                    session.Disconnect();
                }
            }
        }

        public int DoWork()
        {
            long nowMs = epochClock.Time();
            if (cachedEpochClock.Time() != nowMs)
            {
                markFile.UpdateActivityTimestamp(nowMs);
                cachedEpochClock.Update(nowMs);
                heartbeatCounter.SetOrdered(nowMs);
            }

            int workCount = logAdapter.Poll();
            if (0 == workCount)
            {
                if (logAdapter.Image().Closed)
                {
                    throw new AgentTerminationException("Image closed unexpectedly");
                }

                if (!CommitPos.IsActive(aeron.CountersReader(), logAdapter.UpperBoundCounterId()))
                {
                    throw new AgentTerminationException("Commit position is not active");
                }
            }

            workCount += serviceControlAdapter.Poll();
            if (activeLog != null)
            {
                // TODO: handle new log case
                activeLog = null;
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
            if (sessionByIdMap.ContainsKey(clusterSessionId))
            {
                serviceControlPublisher.CloseSession(clusterSessionId);
                return true;
            }

            return false;
        }

        public long TimeMs()
        {
            return timestampMs;
        }

        public void ScheduleTimer(long correlationId, long deadlineMs)
        {
            serviceControlPublisher.ScheduleTimer(correlationId, deadlineMs);
        }

        public void CancelTimer(long correlationId)
        {
            serviceControlPublisher.CancelTimer(correlationId);
        }

        public void OnScheduleTimer(long correlationId, long deadline)
        {
            // Not Implemented
        }

        public void OnCancelTimer(long correlationId)
        {
            // Not Implemented
        }

        public void OnServiceAck(long logPosition, long leadershipTermId, int serviceId, ClusterAction action)
        {
            // Not Implemented
        }

        public void OnJoinLog(long leadershipTermId, int commitPositionId, int logSessionId, int logStreamId,
            string logChannel)
        {
            activeLog = new ActiveLog(leadershipTermId, commitPositionId, logSessionId, logStreamId, logChannel);
        }

        public void OnServiceCloseSession(long clusterSessionId)
        {
            // Not Implemented
        }

        internal void OnSessionMessage(long clusterSessionId, long correlationId, long timestampMs,
            IDirectBuffer buffer, int offset, int length, Header header)
        {
            this.timestampMs = timestampMs;

            service.OnSessionMessage(clusterSessionId, correlationId, timestampMs, buffer, offset, length, header);
        }

        internal void OnTimerEvent(long correlationId, long timestampMs)
        {
            this.timestampMs = timestampMs;

            service.OnTimerEvent(correlationId, timestampMs);
        }

        internal void OnSessionOpen(long clusterSessionId, long timestampMs, int responseStreamId,
            string responseChannel, byte[] encodedPrincipal)
        {
            this.timestampMs = timestampMs;

            ClientSession session =
                new ClientSession(clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

            if (ClusterRole.Leader == role)
            {
                session.Connect(aeron);
            }

            sessionByIdMap[clusterSessionId] = session;
            service.OnSessionOpen(session, timestampMs);
        }

        internal void OnSessionClose(long clusterSessionId, long timestampMs, CloseReason closeReason)
        {
            this.timestampMs = timestampMs;

            var session = sessionByIdMap[clusterSessionId];
            sessionByIdMap.Remove(clusterSessionId);
            session.Disconnect();
            service.OnSessionClose(session, timestampMs, closeReason);
        }

        internal void OnServiceAction(long termPosition, long timestampMs, ClusterAction action)
        {
            this.timestampMs = timestampMs;

            ExecuteAction(action, termPosition);
        }

        internal void AddSession(long clusterSessionId, int responseStreamId, string responseChannel,
            byte[] encodedPrincipal)
        {
            ClientSession session =
                new ClientSession(clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

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
            long termPosition = RecoveryState.GetTermPosition(counters, recoveryCounterId);
            leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);
            timestampMs = RecoveryState.GetTimestamp(counters, recoveryCounterId);

            if (AeronArchive.NULL_POSITION != termPosition)
            {
                RecordingLog.Entry snapshotEntry = recordingLog.GetSnapshot(leadershipTermId, termPosition);
                if (null == snapshotEntry)
                {
                    throw new System.InvalidOperationException(
                        "No snapshot available for term position: " + termPosition);
                }

                termBaseLogPosition = snapshotEntry.termBaseLogPosition + snapshotEntry.termPosition;
                LoadSnapshot(snapshotEntry.recordingId);
            }

            serviceControlPublisher.AckAction(termBaseLogPosition, leadershipTermId, serviceId, ClusterAction.INIT);
        }


        private void CheckForReplay(CountersReader counters, int recoveryCounterId)
        {
            long replayTermCount = RecoveryState.GetReplayTermCount(counters, recoveryCounterId);
            if (0 == replayTermCount)
            {
                return;
            }

            service.OnReplayBegin();

            for (int i = 0; i < replayTermCount; i++)
            {
                AwaitActiveLog();

                int counterId = activeLog.commitPositionId;
                leadershipTermId = CommitPos.GetLeadershipTermId(counters, counterId);
                termBaseLogPosition = CommitPos.GetTermBaseLogPosition(counters, counterId); // TODO MARK

                if (CommitPos.GetLeadershipTermLength(counters, counterId) > 0)
                {
                    using (Subscription subscription = aeron.AddSubscription(activeLog.channel, activeLog.streamId))
                    {
                        serviceControlPublisher.AckAction(termBaseLogPosition, leadershipTermId, serviceId, ClusterAction.READY);

                        Image image = AwaitImage(activeLog.sessionId, subscription);
                        ReadableCounter limit = new ReadableCounter(counters, counterId);
                        BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

                        ConsumeImage(image, adapter);

                        termBaseLogPosition += image.Position();
                    }
                }

                serviceControlPublisher.AckAction(termBaseLogPosition, leadershipTermId, serviceId, ClusterAction.REPLAY);
            }

            service.OnReplayEnd();
        }

        private void AwaitActiveLog()
        {
            activeLog = null;
            idleStrategy.Reset();
            while (true)
            {
                int fragments = serviceControlAdapter.Poll();
                if (activeLog != null)
                {
                    break;
                }

                CheckInterruptedStatus();
                idleStrategy.Idle(fragments);
            }
        }

        private void ConsumeImage(Image image, BoundedLogAdapter adapter)
        {
            while (true)
            {
                int workCount = adapter.Poll();
                if (workCount == 0)
                {
                    if (image.Closed)
                    {
                        if (!image.IsEndOfStream())
                        {
                            throw new System.InvalidOperationException("Unexpected close of replay");
                        }

                        break;
                    }

                    CheckInterruptedStatus();
                }

                idleStrategy.Idle(workCount);
            }
        }

        private int AwaitRecoveryCounterId(CountersReader counters)
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

        private void JoinActiveLog(CountersReader counters)
        {
            AwaitActiveLog();

            int commitPositionId = activeLog.commitPositionId;
            if (!CommitPos.IsActive(counters, commitPositionId))
            {
                throw new System.InvalidOperationException("CommitPos counter not active: " + commitPositionId);
            }

            int logSessionId = activeLog.sessionId;
            leadershipTermId = activeLog.leadershipTermId;
            termBaseLogPosition = CommitPos.GetTermBaseLogPosition(counters, commitPositionId);

            Subscription logSubscription = aeron.AddSubscription(activeLog.channel, activeLog.streamId);
            Image image = AwaitImage(logSessionId, logSubscription);
            heartbeatCounter.SetOrdered(epochClock.Time());

            serviceControlPublisher.AckAction(termBaseLogPosition, leadershipTermId, serviceId, ClusterAction.READY);

            logAdapter = new BoundedLogAdapter(image, new ReadableCounter(counters, commitPositionId), this);
            activeLog = null;
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

        private void LoadSnapshot(long recordingId)
        {
            using (AeronArchive archive = AeronArchive.Connect(archiveCtx))
            {
                RecordingExtent recordingExtent = new RecordingExtent();
                if (0 == archive.ListRecording(recordingId, recordingExtent))
                {
                    throw new System.InvalidOperationException("Could not find recordingId: " + recordingId);
                }

                string channel = ctx.ReplayChannel();
                int streamId = ctx.ReplayStreamId();

                long length = recordingExtent.stopPosition - recordingExtent.startPosition;
                int sessionId = (int) archive.StartReplay(recordingId, 0, length, channel, streamId);

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
                        throw new System.InvalidOperationException("Snapshot ended unexpectedly");
                    }

                    idleStrategy.Idle(fragments);
                }
            }
        }

        private void OnTakeSnapshot(long termPosition)
        {
            long recordingId;
            string channel = ctx.SnapshotChannel();
            int streamId = ctx.SnapshotStreamId();

            using (AeronArchive archive = AeronArchive.Connect(archiveCtx))
            using (Publication publication = archive.AddRecordedExclusivePublication(channel, streamId))
            {
                try
                {
                    CountersReader counters = aeron.CountersReader();
                    int counterId = AwaitRecordingCounter(publication, counters);

                    recordingId = RecordingPos.GetRecordingId(counters, counterId);
                    SnapshotState(publication, termBaseLogPosition + termPosition);
                    service.OnTakeSnapshot(publication);

                    AwaitRecordingComplete(recordingId, publication.Position, counters, counterId, archive);
                }
                finally
                {
                    archive.StopRecording(publication);
                }
            }

            recordingLog.AppendSnapshot(recordingId, leadershipTermId, termBaseLogPosition, termPosition, timestampMs);
        }

        private void AwaitRecordingComplete(
            long recordingId,
            long completePosition,
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
                    throw new InvalidOperationException("Recording has stopped unexpectedly: " + recordingId);
                }

                archive.CheckForErrorResponse();
            } while (counters.GetCounterValue(counterId) < completePosition);
        }


        private void SnapshotState(Publication publication, long logPosition)
        {
            var snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, null);

            snapshotTaker.MarkBegin(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

            foreach (ClientSession clientSession in sessionByIdMap.Values)
            {
                snapshotTaker.SnapshotSession(clientSession);
            }

            snapshotTaker.MarkEnd(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
        }

        private void ExecuteAction(ClusterAction action, long termPosition)
        {
            if (isRecovering)
            {
                return;
            }

            long logPosition = termBaseLogPosition + termPosition;

            switch (action)
            {
                case ClusterAction.SNAPSHOT:
                    OnTakeSnapshot(termPosition);
                    serviceControlPublisher.AckAction(logPosition, leadershipTermId, serviceId, action);
                    break;

                case ClusterAction.SHUTDOWN:
                    OnTakeSnapshot(termPosition);
                    serviceControlPublisher.AckAction(logPosition, leadershipTermId, serviceId, action);
                    ctx.TerminationHook().Invoke();
                    break;

                case ClusterAction.ABORT:
                    serviceControlPublisher.AckAction(logPosition, leadershipTermId, serviceId, action);
                    ctx.TerminationHook().Invoke();
                    break;
            }
        }

        private int AwaitRecordingCounter(Publication publication, CountersReader counters)
        {
            idleStrategy.Reset();
            int counterId = RecordingPos.FindCounterIdBySession(counters, publication.SessionId);
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
                counterId = RecordingPos.FindCounterIdBySession(counters, publication.SessionId);
            }

            return counterId;
        }

        private void FindHeartbeatCounter(CountersReader counters)
        {
            var heartbeatCounterId = ServiceHeartbeat.FindCounterId(counters, ctx.ServiceId());

            if (CountersReader.NULL_COUNTER_ID == heartbeatCounterId)
            {
                throw new InvalidOperationException("Failed to find heartbeat counter");
            }
            
            heartbeatCounter = new AtomicCounter(counters.ValuesBuffer, heartbeatCounterId);
        }

        private static void CheckInterruptedStatus()
        {
            try
            {
                Thread.Sleep(0);
            }
            catch (ThreadInterruptedException)
            {
                throw new AgentTerminationException("Unexpected interrupt during operation");
            }
        }

        private class ActiveLog
        {
            internal readonly long leadershipTermId;
            internal readonly int commitPositionId;
            internal readonly int sessionId;
            internal readonly int streamId;
            internal readonly string channel;

            internal ActiveLog(long leadershipTermId, int commitPositionId, int sessionId, int streamId, string channel)
            {
                this.leadershipTermId = leadershipTermId;
                this.commitPositionId = commitPositionId;
                this.sessionId = sessionId;
                this.streamId = streamId;
                this.channel = channel;
            }
        }
    }
}