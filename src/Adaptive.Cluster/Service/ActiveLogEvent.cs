namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Event to signal a change of active log to follow.
    /// </summary>
    internal class ActiveLogEvent
    {
        public long leadershipTermId { get; }
        public int commitPositionId { get; }
        public int sessionId { get; }
        public int streamId { get; }
        public string channel { get; }

        internal ActiveLogEvent(
            long leadershipTermId,
            int commitPositionId,
            int sessionId,
            int streamId,
            string channel)
        {
            this.leadershipTermId = leadershipTermId;
            this.commitPositionId = commitPositionId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.channel = channel;
        }

        public override string ToString()
        {
            return "NewActiveLogEvent{"
                   + "leadershipTermId=" + leadershipTermId
                   + ", commitPositionId=" + commitPositionId
                   + ", sessionId=" + sessionId
                   + ", streamId=" + streamId
                   + ", channel='" + channel + "'"
                   + '}';
        }
    }
}