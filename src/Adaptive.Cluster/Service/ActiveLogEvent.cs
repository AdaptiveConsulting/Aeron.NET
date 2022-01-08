namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Event to signal a change of active log to follow.
    /// </summary>
    internal class ActiveLogEvent
    {
        public readonly long logPosition;
        public readonly long maxLogPosition;
        public readonly int memberId;
        public readonly int sessionId;
        public readonly int streamId;
        public readonly bool isStartup;
        public readonly ClusterRole role;
        public readonly string channel;

        internal ActiveLogEvent(
            long logPosition,
            long maxLogPosition,
            int memberId,
            int sessionId,
            int streamId,
            bool isStartup,
            ClusterRole role,
            string channel)
        {
            this.logPosition = logPosition;
            this.maxLogPosition = maxLogPosition;
            this.memberId = memberId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.isStartup = isStartup;
            this.role = role;
            this.channel = channel;
        }

        public override string ToString()
        {
            return "NewActiveLogEvent{"
                   + "logPosition=" + logPosition
                   + ", maxLogPosition=" + maxLogPosition
                   + ", memberId=" + memberId
                   + ", sessionId=" + sessionId
                   + ", streamId=" + streamId
                   + ", isStartup=" + isStartup
                   + ", role=" + role
                   + ", channel='" + channel + "'"
                   + '}';
        }
    }
}