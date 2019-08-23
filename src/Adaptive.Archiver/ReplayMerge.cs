using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Archiver.Codecs;
using static Adaptive.Aeron.Aeron;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Replay a recorded stream from a starting position and merge with live stream to consume a full history of a stream.
    /// <para>
    /// Once constructed the either of <seealso cref="Poll(FragmentHandler, int)"/> or <seealso cref="DoWork()"/> interleaved with consumption
    /// of the <seealso cref="Image()"/> should be called in a duty cycle loop until <seealso cref="Merged"/> is {@code true},
    /// after which the <seealso cref="ReplayMerge"/> can be closed and continued usage can be made of the <seealso cref="Image"/> or its
    /// parent <seealso cref="Subscription"/>.
    /// </para>
    /// </summary>
    public class ReplayMerge : IDisposable
    {
        private static readonly int LIVE_ADD_THRESHOLD = LogBufferDescriptor.TERM_MIN_LENGTH >> 2;
        private const int REPLAY_REMOVE_THRESHOLD = 0;

        public enum State
        {
            AWAIT_INITIAL_RECORDING_POSITION,
            AWAIT_REPLAY,
            AWAIT_CATCH_UP,
            AWAIT_CURRENT_RECORDING_POSITION,
            AWAIT_STOP_REPLAY,
            MERGED,
            CLOSED
        }

        private readonly AeronArchive archive;
        private readonly Subscription subscription;
        private readonly string replayChannel;
        private readonly string replayDestination;
        private readonly string liveDestination;
        private readonly long recordingId;
        private readonly long startPosition;
        private readonly long liveAddThreshold;
        private readonly long replayRemoveThreshold;

        private State state = State.AWAIT_INITIAL_RECORDING_POSITION;
        private Image image;
        private long activeCorrelationId = NULL_VALUE;
        private long initialMaxPosition = NULL_VALUE;
        private long nextTargetPosition = NULL_VALUE;
        private long replaySessionId = NULL_VALUE;
        private bool isLiveAdded = false;
        private bool isReplayActive = false;

        /// <summary>
        /// Create a <seealso cref="ReplayMerge"/> to manage the merging of a replayed stream and switching to live stream as
        /// appropriate.
        /// </summary>
        /// <param name="subscription"> to use for the replay and live stream. Must be a multi-destination subscription. </param>
        /// <param name="archive"> to use for the replay. </param>
        /// <param name="replayChannel"> to use for the replay. </param>
        /// <param name="replayDestination"> to send the replay to and the destination added by the <seealso cref="Subscription"/>. </param>
        /// <param name="liveDestination"> for the live stream and the destination added by the <seealso cref="Subscription"/>. </param>
        /// <param name="recordingId"> for the replay. </param>
        /// <param name="startPosition"> for the replay. </param>
        public ReplayMerge(Subscription subscription, AeronArchive archive, string replayChannel,
            string replayDestination, string liveDestination, long recordingId, long startPosition)
        {
            var subscriptionChannelUri = ChannelUri.Parse(subscription.Channel);

            if (!Context.MDC_CONTROL_MODE_MANUAL.Equals(subscriptionChannelUri.Get(Context.MDC_CONTROL_MODE_PARAM_NAME))
            )
            {
                throw new ArgumentException("Subscription channel must be manual control mode: mode=" +
                                            subscriptionChannelUri.Get(Context.MDC_CONTROL_MODE_PARAM_NAME)
                );
            }

            this.archive = archive;
            this.subscription = subscription;
            this.replayDestination = replayDestination;
            this.replayChannel = replayChannel;
            this.liveDestination = liveDestination;
            this.recordingId = recordingId;
            this.startPosition = startPosition;
            this.liveAddThreshold = LIVE_ADD_THRESHOLD;
            this.replayRemoveThreshold = REPLAY_REMOVE_THRESHOLD;

            subscription.AddDestination(replayDestination);
        }

        /// <summary>
        /// Close the merge and stop any active replay. Will remove the replay destination from the subscription. Will
        /// NOT remove the live destination if it has been added.
        /// </summary>
        public void Dispose()
        {
            var state = this.state;
            if (State.CLOSED != state)
            {
                if (isReplayActive)
                {
                    isReplayActive = false;
                    archive.StopReplay(replaySessionId);
                }

                if (State.MERGED != state)
                {
                    subscription.RemoveDestination(replayDestination);
                }

                SetState(State.CLOSED);
            }
        }

        /// <summary>
        /// Process the operation of the merge. Do not call the processing of fragments on the subscription.
        /// </summary>
        /// <returns> indication of work done processing the merge. </returns>
        public int DoWork()
        {
            int workCount = 0;

            switch (state)
            {
                case State.AWAIT_INITIAL_RECORDING_POSITION:
                    workCount += AwaitInitialRecordingPosition();
                    break;

                case State.AWAIT_REPLAY:
                    workCount += AwaitReplay();
                    break;

                case State.AWAIT_CATCH_UP:
                    workCount += AwaitCatchUp();
                    break;

                case State.AWAIT_CURRENT_RECORDING_POSITION:
                    workCount += AwaitUpdatedRecordingPosition();
                    break;

                case State.AWAIT_STOP_REPLAY:
                    workCount += AwaitStopReplay();
                    break;
            }

            return workCount;
        }

        /// <summary>
        /// Poll the <seealso cref="Image"/> used for the merging replay and live stream. The <seealso cref="ReplayMerge.DoWork()"/> method
        /// will be called before the poll so that processing of the merge can be done.
        /// </summary>
        /// <param name="fragmentHandler"> to call for fragments </param>
        /// <param name="fragmentLimit"> for poll call </param>
        /// <returns> number of fragments processed. </returns>
        public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
        {
            DoWork();
            return image?.Poll(fragmentHandler, fragmentLimit) ?? 0;
        }

        /// <summary>
        /// State of this <seealso cref="ReplayMerge"/>.
        /// </summary>
        /// <returns> state of this <seealso cref="ReplayMerge"/>. </returns>
        public State GetState()
        {
            return state;
        }

        /// <summary>
        /// Is the live stream merged and the replay stopped?
        /// </summary>
        /// <returns> true if live stream is merged and the replay stopped or false if not. </returns>
        public bool Merged => state == State.MERGED;

        /// <summary>
        /// The <seealso cref="Image"/> used for the replay and live stream.
        /// </summary>
        /// <returns> the <seealso cref="Image"/> used for the replay and live stream. </returns>
        public Image Image()
        {
            return image;
        }

        /// <summary>
        /// Is the live destination added to the subscription?
        /// </summary>
        /// <returns> true if live destination added or false if not. </returns>
        public bool LiveAdded => isLiveAdded;

        private int AwaitInitialRecordingPosition()
        {
            int workCount = 0;

            if (NULL_VALUE == activeCorrelationId)
            {
                long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                if (archive.Proxy().GetRecordingPosition(recordingId, correlationId, archive.ControlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    workCount += 1;
                }
            }
            else if (PollForResponse(archive, activeCorrelationId))
            {
                nextTargetPosition = PolledRelevantId(archive);
                if (AeronArchive.NULL_POSITION == nextTargetPosition)
                {
                    var correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                    if (archive.Proxy().GetStopPosition(recordingId, correlationId, archive.ControlSessionId()))
                    {
                        activeCorrelationId = correlationId;
                        workCount += 1;
                    }
                }
                else
                {
                    initialMaxPosition = nextTargetPosition;
                    activeCorrelationId = NULL_VALUE;
                    SetState(State.AWAIT_REPLAY);
                }

                workCount += 1;
            }

            return workCount;
        }

        private int AwaitReplay()
        {
            int workCount = 0;

            if (NULL_VALUE == activeCorrelationId)
            {
                long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                if (archive.Proxy().Replay(recordingId, startPosition, long.MaxValue, replayChannel,
                    subscription.StreamId, correlationId, archive.ControlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    workCount += 1;
                }
            }
            else if (PollForResponse(archive, activeCorrelationId))
            {
                isReplayActive = true;
                replaySessionId = PolledRelevantId(archive);
                activeCorrelationId = NULL_VALUE;
                SetState(State.AWAIT_CATCH_UP);
                workCount += 1;
            }

            return workCount;
        }

        private int AwaitCatchUp()
        {
            int workCount = 0;

            if (null == image && subscription.IsConnected)
            {
                image = subscription.ImageBySessionId((int) replaySessionId);
            }

            if (null != image && image.Position >= nextTargetPosition)
            {
                activeCorrelationId = NULL_VALUE;
                SetState(State.AWAIT_CURRENT_RECORDING_POSITION);
                workCount += 1;
            }

            return workCount;
        }

        private int AwaitUpdatedRecordingPosition()
        {
            int workCount = 0;

            if (NULL_VALUE == activeCorrelationId)
            {
                long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                if (archive.Proxy().GetRecordingPosition(recordingId, correlationId, archive.ControlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    workCount += 1;
                }
            }
            else if (PollForResponse(archive, activeCorrelationId))
            {
                nextTargetPosition = PolledRelevantId(archive);
                if (AeronArchive.NULL_POSITION == nextTargetPosition)
                {
                    long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                    if (archive.Proxy()
                        .GetRecordingPosition(recordingId, correlationId, archive.ControlSessionId()))
                    {
                        activeCorrelationId = correlationId;
                    }
                }
                else
                {
                    State nextState = State.AWAIT_CATCH_UP;

                    if (null != image)
                    {
                        long position = image.Position;

                        if (ShouldAddLiveDestination(position))
                        {
                            subscription.AddDestination(liveDestination);
                            isLiveAdded = true;
                        }
                        else if (ShouldStopAndRemoveReplay(position))
                        {
                            nextState = State.AWAIT_STOP_REPLAY;
                        }
                    }

                    activeCorrelationId = NULL_VALUE;
                    SetState(nextState);
                }

                workCount += 1;
            }

            return workCount;
        }

        private int AwaitStopReplay()
        {
            int workCount = 0;

            if (NULL_VALUE == activeCorrelationId)
            {
                long correlationId = archive.Ctx().AeronClient().NextCorrelationId();

                if (archive.Proxy().StopReplay(replaySessionId, correlationId, archive.ControlSessionId()))
                {
                    activeCorrelationId = correlationId;
                    workCount += 1;
                }
            }
            else if (PollForResponse(archive, activeCorrelationId))
            {
                isReplayActive = false;
                replaySessionId = NULL_VALUE;
                activeCorrelationId = NULL_VALUE;
                subscription.RemoveDestination(replayDestination);
                SetState(State.MERGED);
                workCount += 1;
            }

            return workCount;
        }

        private void SetState(State state)
        {
            //System.out.println(this.state + " -> " + state);
            this.state = state;
        }

        private bool ShouldAddLiveDestination(long position)
        {
            return !isLiveAdded && (nextTargetPosition - position) <= liveAddThreshold;
        }

        private bool ShouldStopAndRemoveReplay(long position)
        {
            return nextTargetPosition > initialMaxPosition &&
                   isLiveAdded && (nextTargetPosition - position) <= replayRemoveThreshold;
        }

        private static bool PollForResponse(AeronArchive archive, long correlationId)
        {
            ControlResponsePoller poller = archive.ControlResponsePoller();

            if (poller.Poll() > 0 && poller.IsPollComplete())
            {
                if (poller.ControlSessionId() == archive.ControlSessionId() && poller.CorrelationId() == correlationId)
                {
                    if (poller.Code() == ControlResponseCode.ERROR)
                    {
                        throw new ArchiveException("archive response for correlationId=" + correlationId +
                                                   ", error: " + poller.ErrorMessage(), (int) poller.RelevantId());
                    }

                    return true;
                }
            }

            return false;
        }

        private static long PolledRelevantId(AeronArchive archive)
        {
            return archive.ControlResponsePoller().RelevantId();
        }
    }
}