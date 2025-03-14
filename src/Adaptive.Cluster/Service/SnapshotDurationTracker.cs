using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Snapshot duration tracker that tracks maximum snapshot duration and also keeps count of how many times a predefined
    /// duration threshold is breached.
    /// </summary>
    public class SnapshotDurationTracker
    {
        private readonly AtomicCounter maxSnapshotDuration;
        private readonly AtomicCounter snapshotDurationThresholdExceededCount;
        private readonly long durationThresholdNs;
        private long snapshotStartTimeNs = long.MinValue;

        /// <summary>
        /// Create a tracker to track max snapshot duration and breaches of a threshold.
        /// </summary>
        /// <param name="maxSnapshotDuration">                    counter for tracking. </param>
        /// <param name="snapshotDurationThresholdExceededCount"> counter for tracking. </param>
        /// <param name="durationThresholdNs">                    to use for tracking breaches. </param>
        public SnapshotDurationTracker(AtomicCounter maxSnapshotDuration,
            AtomicCounter snapshotDurationThresholdExceededCount, long durationThresholdNs)
        {
            this.maxSnapshotDuration = maxSnapshotDuration;
            this.snapshotDurationThresholdExceededCount = snapshotDurationThresholdExceededCount;
            this.durationThresholdNs = durationThresholdNs;
        }

        /// <summary>
        /// Get max snapshot duration counter.
        /// </summary>
        /// <returns> max snapshot duration counter. </returns>
        public AtomicCounter MaxSnapshotDuration()
        {
            return maxSnapshotDuration;
        }

        /// <summary>
        /// Get counter tracking number of times <seealso cref="SnapshotDurationTracker.durationThresholdNs"/> was exceeded.
        /// </summary>
        /// <returns> duration threshold exceeded counter. </returns>
        public AtomicCounter SnapshotDurationThresholdExceededCount()
        {
            return snapshotDurationThresholdExceededCount;
        }

        /// <summary>
        /// Called when snapshotting has started.
        /// </summary>
        /// <param name="timeNanos"> snapshot start time in nanoseconds. </param>
        public void OnSnapshotBegin(long timeNanos)
        {
            snapshotStartTimeNs = timeNanos;
        }

        /// <summary>
        /// Called when snapshot has been taken.
        /// </summary>
        /// <param name="timeNanos"> snapshot end time in nanoseconds. </param>
        public void OnSnapshotEnd(long timeNanos)
        {
            if (snapshotStartTimeNs != long.MinValue)
            {
                long snapshotDurationNs = timeNanos - snapshotStartTimeNs;

                if (snapshotDurationNs > durationThresholdNs)
                {
                    snapshotDurationThresholdExceededCount.Increment();
                }

                maxSnapshotDuration.ProposeMax(snapshotDurationNs);
            }
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "SnapshotDurationTracker{" + "maxSnapshotDuration=" + maxSnapshotDuration +
                   ", snapshotDurationThresholdExceededCount=" + snapshotDurationThresholdExceededCount +
                   ", durationThresholdNs=" + durationThresholdNs + '}';
        }
    }
}