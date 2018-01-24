using System.Collections.Generic;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver;
using Io.Aeron.Archive.Codecs;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    internal sealed class ClusteredServiceAgent : IAgent, ICluster
    {
        internal enum State
        {
            INIT,
            REPLAY,
            ACTIVE,
            SNAPSHOT,
            CLOSED
        }

        private readonly bool shouldCloseResources;
        private readonly AeronArchive.Context archiveCtx;
        private readonly ClusteredServiceContainer.Context ctx;
        private readonly Aeron.Aeron aeron;
        private readonly Subscription logSubscription;
        private readonly Dictionary<long, ClientSession> sessionByIdMap = new Dictionary<long, ClientSession>();
        private readonly IClusteredService service;
        private readonly ConsensusModuleProxy consensusModule;
        private readonly IIdleStrategy idleStrategy;
        private readonly RecordingLog recordingLog;

        private long leadershipTermBeginPosition = 0;
        private long leadershipTermId;
        private long currentRecordingId;
        private long timestampMs;
        private BoundedLogAdapter logAdapter;
        private ReadableCounter consensusPosition;
        private ReadableCounter roleCounter;
        private State state = State.INIT;
        private ClusterRole role = ClusterRole.Candidate;

        internal ClusteredServiceAgent(ClusteredServiceContainer.Context ctx)
        {
            this.ctx = ctx;

            archiveCtx = ctx.ArchiveContext();
            aeron = ctx.Aeron();
            shouldCloseResources = ctx.OwnsAeronClient();
            service = ctx.ClusteredService();
            recordingLog = ctx.RecordingLog();
            idleStrategy = ctx.IdleStrategy();

            string logChannel = ctx.LogChannel();
            logChannel = logChannel.Contains(Adaptive.Aeron.Aeron.Context.IPC_CHANNEL) ? logChannel : Adaptive.Aeron.Aeron.Context.SPY_PREFIX + logChannel;

            logSubscription = aeron.AddSubscription(logChannel, ctx.LogStreamId());

            consensusModule = new ConsensusModuleProxy(ctx.ServiceId(), aeron.AddExclusivePublication(ctx.ConsensusModuleChannel(), ctx.ConsensusModuleStreamId()), idleStrategy);
        }

        public void OnStart()
        {
            service.OnStart(this);

            CountersReader counters = aeron.CountersReader();
            int recoveryCounterId = FindRecoveryCounterId(counters);

            CheckForSnapshot(counters, recoveryCounterId);
            CheckForReplay(counters, recoveryCounterId);

            FindConsensusPosition(counters, logSubscription);

            int sessionId = ConsensusPos.GetSessionId(counters, consensusPosition.CounterId());
            Image image = logSubscription.ImageBySessionId(sessionId);
            logAdapter = new BoundedLogAdapter(image, consensusPosition, this);

            FindClusterRole(counters);

            idleStrategy.Reset();
            var roleValue = (ClusterRole)roleCounter.Get();
            while (roleValue == ClusterRole.Candidate)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
                roleValue = (ClusterRole)roleCounter.Get();
            }

            role = roleValue;
            state = State.ACTIVE;

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
            state = State.CLOSED;

            if (shouldCloseResources)
            {
                logSubscription?.Dispose();
                consensusModule?.Dispose();

                foreach (ClientSession session in sessionByIdMap.Values)
                {
                    session.Disconnect();
                }
            }
        }

        public int DoWork()
        {
            int workCount = logAdapter.Poll();
            if (0 == workCount)
            {
                if (logAdapter.Image().Closed)
                {
                    throw new System.InvalidOperationException("Image closed unexpectedly");
                }

                if (!ConsensusPos.IsActive(aeron.CountersReader(), consensusPosition.CounterId(), currentRecordingId))
                {
                    throw new System.InvalidOperationException("Consensus position is not active");
                }
            }

            return workCount;
        }

        public string RoleName()
        {
            return "clustered-service";
        }

        public ClusterRole Role()
        {
            return role;
        }

        public bool IsReplay()
        {
            return State.REPLAY == state;
        }

        public Aeron.Aeron Aeron()
        {
            return aeron;
        }

        public ClientSession GetClientSession(long clusterSessionId)
        {
            return sessionByIdMap[clusterSessionId];
        }

        public long TimeMs()
        {
            return timestampMs;
        }

        public void ScheduleTimer(long correlationId, long deadlineMs)
        {
            consensusModule.ScheduleTimer(correlationId, deadlineMs);
        }

        public void CancelTimer(long correlationId)
        {
            consensusModule.CancelTimer(correlationId);
        }

        internal void OnSessionMessage(long clusterSessionId, long correlationId, long timestampMs, IDirectBuffer buffer, int offset, int length, Header header)
        {
            this.timestampMs = timestampMs;

            service.OnSessionMessage(clusterSessionId, correlationId, timestampMs, buffer, offset, length, header);
        }

        internal void OnTimerEvent(long correlationId, long timestampMs)
        {
            this.timestampMs = timestampMs;

            service.OnTimerEvent(correlationId, timestampMs);
        }

        internal void OnSessionOpen(long clusterSessionId, long timestampMs, int responseStreamId, string responseChannel, byte[] principalData)
        {
            this.timestampMs = timestampMs;

            ClientSession session = new ClientSession(clusterSessionId, responseStreamId, responseChannel, principalData, this);

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

        internal void OnServiceAction(long resultingPosition, long timestampMs, ServiceAction action)
        {
            this.timestampMs = timestampMs;

            ExecuteAction(action, leadershipTermBeginPosition + resultingPosition);
        }

        internal void AddSession(long clusterSessionId, int responseStreamId, string responseChannel, byte[] principalData)
        {
            ClientSession session = new ClientSession(clusterSessionId, responseStreamId, responseChannel, principalData, this);

            sessionByIdMap[clusterSessionId] = session;
        }

        private void CheckForSnapshot(CountersReader counters, int recoveryCounterId)
        {
            long logPosition = RecoveryState.GetLogPosition(counters, recoveryCounterId);

            if (logPosition > 0)
            {
                RecordingLog.Entry snapshotEntry = recordingLog.GetSnapshotByPosition(logPosition);
                if (null == snapshotEntry)
                {
                    throw new System.InvalidOperationException("No snapshot available for position: " + logPosition);
                }

                leadershipTermBeginPosition = logPosition;
                leadershipTermId = RecoveryState.GetLeadershipTermId(counters, recoveryCounterId);
                timestampMs = RecoveryState.GetTimestamp(counters, recoveryCounterId);

                snapshotEntry.ConfirmMatch(logPosition, leadershipTermId, timestampMs);
                LoadSnapshot(snapshotEntry.recordingId);
            }
        }

        private void CheckForReplay(CountersReader counters, int recoveryCounterId)
        {
            long replayTermCount = RecoveryState.GetReplayTermCount(counters, recoveryCounterId);
            if (0 == replayTermCount)
            {
                consensusModule.SendAcknowledgment(ServiceAction.INIT, leadershipTermBeginPosition, leadershipTermId, timestampMs);

                return;
            }

            state = State.REPLAY;

            using (Subscription subscription = aeron.AddSubscription(ctx.ReplayChannel(), ctx.ReplayStreamId()))
            {
                consensusModule.SendAcknowledgment(ServiceAction.INIT, leadershipTermBeginPosition, leadershipTermId, timestampMs);

                for (int i = 0; i < replayTermCount; i++)
                {
                    int counterId = FindReplayConsensusCounterId(counters, i);
                    int sessionId = ConsensusPos.GetSessionId(counters, counterId);
                    leadershipTermBeginPosition = ConsensusPos.GetBeginningLogPosition(counters, counterId);

                    idleStrategy.Reset();
                    Image image;
                    while ((image = subscription.ImageBySessionId(sessionId)) == null)
                    {
                        CheckInterruptedStatus();
                        idleStrategy.Idle();
                    }

                    ReadableCounter limit = new ReadableCounter(counters, counterId);
                    BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

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

                    consensusModule.SendAcknowledgment(ServiceAction.REPLAY, leadershipTermBeginPosition, leadershipTermId, timestampMs);
                }
            }
        }

        private int FindRecoveryCounterId(CountersReader counters)
        {
            int counterId = RecoveryState.FindCounterId(counters);

            while (RecoveryState.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();

                counterId = RecoveryState.FindCounterId(counters);
            }

            return counterId;
        }

        private int FindReplayConsensusCounterId(CountersReader counters, int replayStep)
        {
            int counterId = ConsensusPos.FindCounterIdByReplayStep(counters, replayStep);

            while (RecoveryState.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();

                counterId = ConsensusPos.FindCounterIdByReplayStep(counters, replayStep);
            }

            return counterId;
        }

        private void FindConsensusPosition(CountersReader counters, Subscription logSubscription)
        {
            idleStrategy.Reset();
            while (!logSubscription.IsConnected)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
            }

            int sessionId = logSubscription.ImageAtIndex(0).SessionId;

            int counterId = ConsensusPos.FindCounterIdBySession(counters, sessionId);
            while (ConsensusPos.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();

                counterId = ConsensusPos.FindCounterIdBySession(counters, sessionId);
            }

            currentRecordingId = ConsensusPos.GetRecordingId(counters, counterId);
            consensusPosition = new ReadableCounter(counters, counterId);
        }

        private void FindClusterRole(CountersReader counters)
        {
            idleStrategy.Reset();

            int counterId = ClusterNodeRole.FindCounterId(counters);
            while (ClusterNodeRole.NULL_COUNTER_ID == counterId)
            {
                CheckInterruptedStatus();
                idleStrategy.Idle();
                counterId = ClusterNodeRole.FindCounterId(counters);
            }

            roleCounter = new ReadableCounter(counters, counterId);
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

                string replaySubscriptionChannel = ChannelUri.AddSessionId(channel, sessionId);

                using (Subscription subscription = aeron.AddSubscription(replaySubscriptionChannel, streamId))
                {
                    Image image;
                    while ((image = subscription.ImageBySessionId(sessionId)) == null)
                    {
                        CheckInterruptedStatus();
                        idleStrategy.Idle();
                    }

                    LoadState(image);
                    service.OnLoadSnapshot(image);
                }
            }
        }

        private void LoadState(Image image)
        {
            SnapshotLoader snapshotLoader = new SnapshotLoader(image, this);
            while (snapshotLoader.InProgress())
            {
                int fragments = snapshotLoader.Poll();
                if (fragments == 0)
                {
                    CheckInterruptedStatus();

                    if (image.Closed && snapshotLoader.InProgress())
                    {
                        throw new System.InvalidOperationException("Snapshot ended unexpectedly");
                    }

                    idleStrategy.Idle(fragments);
                }
            }
        }

        private void OnTakeSnapshot(long logPosition)
        {
            state = State.SNAPSHOT;
            long recordingId;
            string channel = ctx.SnapshotChannel();
            int streamId = ctx.SnapshotStreamId();

            using (AeronArchive archive = AeronArchive.Connect(archiveCtx))
            using (Publication publication = aeron.AddExclusivePublication(channel, streamId))
            {
                string recordingChannel = ChannelUri.AddSessionId(channel, publication.SessionId);

                archive.StartRecording(recordingChannel, streamId, SourceLocation.LOCAL);
                idleStrategy.Reset();

                try
                {
                    CountersReader counters = aeron.CountersReader();
                    int counterId = RecordingPos.FindCounterIdBySession(counters, publication.SessionId);
                    while (RecordingPos.NULL_COUNTER_ID == counterId)
                    {
                        CheckInterruptedStatus();
                        idleStrategy.Idle();
                        counterId = RecordingPos.FindCounterIdBySession(counters, publication.SessionId);
                    }

                    recordingId = RecordingPos.GetRecordingId(counters, counterId);
                    SnapshotState(publication, logPosition);
                    service.OnTakeSnapshot(publication);

                    do
                    {
                        idleStrategy.Idle();
                        CheckInterruptedStatus();

                        if (!RecordingPos.IsActive(counters, counterId, recordingId))
                        {
                            throw new System.InvalidOperationException("Recording has stopped unexpectedly: " + recordingId);
                        }
                    } while (counters.GetCounterValue(counterId) < publication.Position);
                }
                finally
                {
                    archive.StopRecording(recordingChannel, streamId);
                }
            }

            recordingLog.AppendSnapshot(recordingId, logPosition, leadershipTermId, timestampMs);
            state = State.ACTIVE;
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

        private void ExecuteAction(ServiceAction action, long position)
        {
            if (State.ACTIVE != state)
            {
                return;
            }

            switch (action)
            {
                case ServiceAction.SNAPSHOT:
                    OnTakeSnapshot(position);
                    consensusModule.SendAcknowledgment(ServiceAction.SNAPSHOT, position, leadershipTermId, timestampMs);
                    break;

                case ServiceAction.SHUTDOWN:
                    OnTakeSnapshot(position);
                    consensusModule.SendAcknowledgment(ServiceAction.SHUTDOWN, position, leadershipTermId, timestampMs);
                    state = State.CLOSED;
                    ctx.TerminationHook().Invoke();
                    break;

                case ServiceAction.ABORT:
                    consensusModule.SendAcknowledgment(ServiceAction.ABORT, position, leadershipTermId, timestampMs);
                    state = State.CLOSED;
                    ctx.TerminationHook().Invoke();
                    break;
            }
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
    }
}