using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.Aeron.Context;

/// <summary>
/// Replay a recorded stream from a starting position and merge with live stream for a full history of a stream.
/// <para>
/// Once constructed either of <seealso cref="Poll(FragmentHandler, int)"/> or <seealso cref="DoWork()"/>, interleaved with consumption
/// of the <seealso cref="Image()"/>, should be called in a duty cycle loop until <seealso cref="Merged()"/> is {@code true}.
/// After which the <seealso cref="ReplayMerge"/> can be closed and continued usage can be made of the <seealso cref="Image"/> or its
/// parent <seealso cref="Subscription"/>. If an exception occurs or progress stops, the merge will fail and
/// <seealso cref="HasFailed()"/> will be {@code true}.
/// </para>
/// <para>
/// NOTE: Merging is only supported with UDP streams.
/// </para>
/// </summary>
public class ReplayMerge : IDisposable
{
    /// <summary>
    /// The maximum window at which a live destination should be added when trying to merge.
    /// </summary>
    public const int LIVE_ADD_MAX_WINDOW = 32 * 1024 * 1024;

    private const int REPLAY_REMOVE_THRESHOLD = 0;
    private static readonly long MERGE_PROGRESS_TIMEOUT_DEFAULT_MS = 10_000L;

    internal enum State
    {
        GET_RECORDING_POSITION,
        REPLAY,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        MERGED,
        FAILED,
        CLOSED
    }

    private readonly AeronArchive archive;
    private readonly Subscription subscription;
    private readonly IEpochClock epochClock;
    private readonly string replayChannel;
    private readonly string replayDestination;
    private readonly string liveDestination;
    private readonly long recordingId;
    private readonly long startPosition;
    private readonly long mergeProgressTimeoutMs;

    private State state = State.GET_RECORDING_POSITION;
    private Image image;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long nextTargetPosition = Aeron.NULL_VALUE;
    private long replaySessionId = Aeron.NULL_VALUE;
    private long positionOfLastProgress = Aeron.NULL_VALUE;
    private long timeOfLastProgressMs;
    private bool isLiveAdded = false;
    private bool isReplayActive = false;

    /// <summary>
    /// Create a <seealso cref="ReplayMerge"/> to manage the merging of a replayed stream and switching over to live stream as
    /// appropriate.
    /// </summary>
    /// <param name="subscription">           to use for the replay and live stream. Must be a multi-destination subscription. </param>
    /// <param name="archive">                to use for the replay. </param>
    /// <param name="replayChannel">          to use for the replay. </param>
    /// <param name="replayDestination">      to send the replay to and the destination added by the <seealso cref="Subscription"/>. </param>
    /// <param name="liveDestination">        for the live stream and the destination added by the <seealso cref="Subscription"/>. </param>
    /// <param name="recordingId">            for the replay. </param>
    /// <param name="startPosition">          for the replay. </param>
    /// <param name="epochClock">             to use for progress checks. </param>
    /// <param name="mergeProgressTimeoutMs"> to use for progress checks. </param>
    public ReplayMerge(Subscription subscription, AeronArchive archive, string replayChannel, string replayDestination,
        string liveDestination, long recordingId, long startPosition, IEpochClock epochClock,
        long mergeProgressTimeoutMs)
    {
        if (subscription.Channel.StartsWith(IPC_CHANNEL) || replayChannel.StartsWith(IPC_CHANNEL) ||
            replayDestination.StartsWith(IPC_CHANNEL) || liveDestination.StartsWith(IPC_CHANNEL))
        {
            throw new System.ArgumentException("IPC merging is not supported");
        }

        ChannelUri subscriptionChannelUri = ChannelUri.Parse(subscription.Channel);
        if (!MDC_CONTROL_MODE_MANUAL.Equals(subscriptionChannelUri.Get(MDC_CONTROL_MODE_PARAM_NAME)))
        {
            throw new ArgumentException("Subscription must have manual control-mode: control-mode=" +
                                        subscriptionChannelUri.Get(MDC_CONTROL_MODE_PARAM_NAME));
        }

        ChannelUri replayChannelUri = ChannelUri.Parse(replayChannel);
        replayChannelUri.Put(LINGER_PARAM_NAME, "0");
        replayChannelUri.Put(EOS_PARAM_NAME, "false");

        this.archive = archive;
        this.subscription = subscription;
        this.epochClock = epochClock;
        this.replayDestination = replayDestination;
        this.replayChannel = replayChannelUri.ToString();
        this.liveDestination = liveDestination;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.timeOfLastProgressMs = epochClock.Time();
        this.mergeProgressTimeoutMs = mergeProgressTimeoutMs;

        subscription.AsyncAddDestination(replayDestination);
    }

    /// <summary>
    /// Create a <seealso cref="ReplayMerge"/> to manage the merging of a replayed stream and switching over to live stream as
    /// appropriate.
    /// </summary>
    /// <param name="subscription">      to use for the replay and live stream. Must be a multi-destination subscription. </param>
    /// <param name="archive">           to use for the replay. </param>
    /// <param name="replayChannel">     to use for the replay. </param>
    /// <param name="replayDestination"> to send the replay to and the destination added by the <seealso cref="Subscription"/>. </param>
    /// <param name="liveDestination">   for the live stream and the destination added by the <seealso cref="Subscription"/>. </param>
    /// <param name="recordingId">       for the replay. </param>
    /// <param name="startPosition">     for the replay. </param>
    public ReplayMerge(Subscription subscription, AeronArchive archive, string replayChannel, string replayDestination,
        string liveDestination, long recordingId, long startPosition) : this(subscription, archive, replayChannel,
        replayDestination, liveDestination, recordingId, startPosition,
        archive.Ctx().AeronClient().Ctx.EpochClock(), MERGE_PROGRESS_TIMEOUT_DEFAULT_MS)
    {
    }

    /// <summary>
    /// Close and stop any active replay. Will remove the replay destination from the subscription.
    /// This operation Will NOT remove the live destination if it has been added so it can be used for live consumption.
    /// </summary>
    public void Dispose()
    {
        State state = this.state;
        if (State.CLOSED != state)
        {
            if (!archive.Ctx().AeronClient().IsClosed)
            {
                if (State.MERGED != state)
                {
                    subscription.AsyncRemoveDestination(replayDestination);
                }

                if (isReplayActive && archive.Proxy().Pub().IsConnected)
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
        return subscription;
    }

    /// <summary>
    /// Perform the work of replaying and merging. Should only be used if polling the underlying <seealso cref="Image"/> directly,
    /// call <seealso cref="Poll(FragmentHandler, int)"/> on this class.
    /// </summary>
    /// <returns> indication of work done processing the merge. </returns>
    public int DoWork()
    {
        int workCount = 0;
        long nowMs = epochClock.Time();

        try
        {
            switch (state)
            {
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
            }
        }
        catch (Exception ex)
        {
            SetState(State.FAILED);
            throw ex;
        }

        return workCount;
    }

    /// <summary>
    /// Poll the <seealso cref="Image"/> used for replay and merging and live stream. The <seealso cref="ReplayMerge.DoWork()"/> method
    /// will be called before the poll so that processing of the merge can be done.
    /// </summary>
    /// <param name="fragmentHandler"> to call for fragments. </param>
    /// <param name="fragmentLimit">   for poll call. </param>
    /// <returns> number of fragments processed. </returns>
    public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
    {
        DoWork();
        return null == image ? 0 : image.Poll(fragmentHandler, fragmentLimit);
    }

    /// <summary>
    /// Is the live stream merged and the replay stopped?
    /// </summary>
    /// <returns> true if live stream is merged and the replay stopped or false if not. </returns>
    public bool Merged
    {
        get { return state == State.MERGED; }
    }

    /// <summary>
    /// Has the replay merge failed due to an error?
    /// </summary>
    /// <returns> true if replay merge has failed due to an error. </returns>
    public bool HasFailed()
    {
        return state == State.FAILED;
    }

    /// <summary>
    /// The <seealso cref="Image"/> which is a merge of the replay and live stream.
    /// </summary>
    /// <returns> the <seealso cref="Image"/> which is a merge of the replay and live stream. </returns>
    public Image Image()
    {
        return image;
    }

    /// <summary>
    /// Is the live destination added to the <seealso cref="Subscription()"/>?
    /// </summary>
    /// <returns> true if live destination added or false if not. </returns>
    public bool LiveAdded
    {
        get { return isLiveAdded; }
    }

    private int GetRecordingPosition(long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

            if (archive.Proxy().GetRecordingPosition(recordingId, correlationId, archive.ControlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (PollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = PolledRelevantId(archive);
            activeCorrelationId = Aeron.NULL_VALUE;

            if (AeronArchive.NULL_POSITION == nextTargetPosition)
            {
                long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                if (archive.Proxy().GetStopPosition(recordingId, correlationId, archive.ControlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    timeOfLastProgressMs = nowMs;
                    workCount += 1;
                }
            }
            else
            {
                timeOfLastProgressMs = nowMs;
                SetState(State.REPLAY);
            }

            workCount += 1;
        }

        return workCount;
    }

    private int Replay(long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

            if (archive.Proxy().Replay(recordingId, startPosition, long.MaxValue, replayChannel,
                subscription.StreamId, correlationId, archive.ControlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (PollForResponse(archive, activeCorrelationId))
        {
            isReplayActive = true;
            replaySessionId = PolledRelevantId(archive);
            timeOfLastProgressMs = nowMs;
            SetState(State.CATCHUP);
            workCount += 1;
        }

        return workCount;
    }

    private int Catchup(long nowMs)
    {
        int workCount = 0;

        if (null == image && subscription.IsConnected)
        {
            timeOfLastProgressMs = nowMs;
            image = subscription.ImageBySessionId((int) replaySessionId);
            positionOfLastProgress = null == image ? Aeron.NULL_VALUE : image.Position;
        }

        if (null != image)
        {
            if (image.Position >= nextTargetPosition)
            {
                timeOfLastProgressMs = nowMs;
                SetState(State.ATTEMPT_LIVE_JOIN);
                workCount += 1;
            }
            else if (image.Closed)
            {
                throw new System.InvalidOperationException("ReplayMerge Image closed unexpectedly.");
            }
            else if (image.Position > positionOfLastProgress)
            {
                timeOfLastProgressMs = nowMs;
                positionOfLastProgress = image.Position;
            }
        }

        return workCount;
    }

    private int AttemptLiveJoin(long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

            if (archive.Proxy().GetRecordingPosition(recordingId, correlationId, archive.ControlSessionId()))
            {
                activeCorrelationId = correlationId;
                timeOfLastProgressMs = nowMs;
                workCount += 1;
            }
        }
        else if (PollForResponse(archive, activeCorrelationId))
        {
            nextTargetPosition = PolledRelevantId(archive);
            activeCorrelationId = Aeron.NULL_VALUE;

            if (AeronArchive.NULL_POSITION == nextTargetPosition)
            {
                long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                if (archive.Proxy().GetRecordingPosition(recordingId, correlationId, archive.ControlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    timeOfLastProgressMs = nowMs;
                }
            }
            else
            {
                State nextState = State.CATCHUP;

                if (null != image)
                {
                    long position = image.Position;

                    if (ShouldAddLiveDestination(position))
                    {
                        subscription.AsyncAddDestination(liveDestination);
                        timeOfLastProgressMs = nowMs;
                        isLiveAdded = true;
                    }
                    else if (ShouldStopAndRemoveReplay(position))
                    {
                        subscription.AsyncRemoveDestination(replayDestination);
                        StopReplay();
                        timeOfLastProgressMs = nowMs;
                        nextState = State.MERGED;
                    }
                }

                SetState(nextState);
            }

            workCount += 1;
        }

        return workCount;
    }

    private void StopReplay()
    {
        long correlationId = archive.Ctx().AeronClient().NextCorrelationId();
        if (archive.Proxy().StopReplay(replaySessionId, correlationId, archive.ControlSessionId()))
        {
            isReplayActive = false;
        }
    }

    private void SetState(ReplayMerge.State newState)
    {
        //System.out.println(state + " -> " + newState);
        state = newState;
        activeCorrelationId = Aeron.NULL_VALUE;
    }

    private bool ShouldAddLiveDestination(long position)
    {
        return !isLiveAdded && (nextTargetPosition - position) <=
            Math.Min(image.TermBufferLength >> 2, LIVE_ADD_MAX_WINDOW);
    }

    private bool ShouldStopAndRemoveReplay(long position)
    {
        return isLiveAdded && (nextTargetPosition - position) <= REPLAY_REMOVE_THRESHOLD &&
               image.ActiveTransportCount() >= 2;
    }

    private bool HasProgressStalled(long nowMs)
    {
        return nowMs > (timeOfLastProgressMs + mergeProgressTimeoutMs);
    }

    private void CheckProgress(long nowMs)
    {
        if (HasProgressStalled(nowMs))
        {
            throw new TimeoutException("ReplayMerge no progress state=" + state);
        }
    }

    private static bool PollForResponse(AeronArchive archive, long correlationId)
    {
        ControlResponsePoller poller = archive.ControlResponsePoller();
        if (poller.Poll() > 0 && poller.PollComplete)
        {
            if (poller.ControlSessionId() == archive.ControlSessionId())
            {
                if (poller.Code() == ControlResponseCode.ERROR)
                {
                    throw new ArchiveException(
                        "archive response for correlationId=" + poller.CorrelationId() + ", error: " +
                        poller.ErrorMessage(), (int) poller.RelevantId(), poller.CorrelationId());
                }

                return poller.CorrelationId() == correlationId;
            }
        }

        return false;
    }

    private static long PolledRelevantId(AeronArchive archive)
    {
        return archive.ControlResponsePoller().RelevantId();
    }

    public override string ToString()
    {
        return "ReplayMerge{" +
               "state=" + state +
               ", positionOfLastProgress=" + positionOfLastProgress +
               ", isLiveAdded=" + isLiveAdded +
               ", isReplayActive=" + isReplayActive +
               '}';
    }
}