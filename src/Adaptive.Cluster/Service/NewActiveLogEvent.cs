namespace Adaptive.Cluster.Service
{
    internal class NewActiveLogEvent
    {
        public long leadershipTermId { get; }
        public int commitPositionId { get; }
        public int sessionId { get; }
        public int streamId { get; }
        public bool ackBeforeImage { get; }
        public string channel { get; }

        internal NewActiveLogEvent(
            long leadershipTermId,
            int commitPositionId,
            int sessionId,
            int streamId,
            bool ackBeforeImage,
            string channel)
        {
            this.leadershipTermId = leadershipTermId;
            this.commitPositionId = commitPositionId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.ackBeforeImage = ackBeforeImage;
            this.channel = channel;
        }

        public override string ToString()
        {
            return "NewActiveLogEvent{"
                   + "leadershipTermId=" + leadershipTermId
                   + ", commitPositionId=" + commitPositionId
                   + ", sessionId=" + sessionId
                   + ", streamId=" + streamId
                   + ", ackBeforeImage=" + ackBeforeImage
                   + ", channel='" + channel + "'"
                   + '}';
        }
    }
}