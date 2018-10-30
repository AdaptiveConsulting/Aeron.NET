namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Event to signal a change of active log to follow.
    /// </summary>
    internal class ActiveLogEvent
    {
        public readonly long leadershipTermId;
        public readonly long logPosition;
        public readonly long maxLogPosition;
        public readonly int memberId;
        public readonly int sessionId;
        public readonly int streamId;
        public readonly string channel;

        internal ActiveLogEvent(
            long leadershipTermId,
            long logPosition,
            long maxLogPosition,
            int memberId,
            int sessionId,
            int streamId,
            string channel)
        {
            this.leadershipTermId = leadershipTermId;
            this.logPosition = logPosition;
            this.maxLogPosition = maxLogPosition;
            this.memberId = memberId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.channel = channel;
        }

        public override string ToString()
        {
            return "NewActiveLogEvent{"
                   + "leadershipTermId=" + leadershipTermId
                   + ", logPosition=" + logPosition
                   + ", maxLogPosition=" + maxLogPosition
                   + ", memberId=" + memberId
                   + ", sessionId=" + sessionId
                   + ", streamId=" + streamId
                   + ", channel='" + channel + "'"
                   + '}';
        }
    }
}