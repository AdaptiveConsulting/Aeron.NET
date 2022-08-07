using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Duty cycle tracker that detects when a cycle exceeds a threshold and tracks max cycle time reporting both through
    /// counters.
    /// </summary>
    public class DutyCycleStallTracker : DutyCycleTracker
    {
        private readonly AtomicCounter maxCycleTime;
        private readonly AtomicCounter cycleTimeThresholdExceededCount;
        private readonly long cycleTimeThresholdNs;

        /// <summary>
        /// Create a tracker to track max cycle time and excesses of a threshold.
        /// </summary>
        /// <param name="maxCycleTime"> counter for tracking. </param>
        /// <param name="cycleTimeThresholdExceededCount"> counter for tracking. </param>
        /// <param name="cycleTimeThresholdNs"> to use for tracking excesses. </param>
        public DutyCycleStallTracker(AtomicCounter maxCycleTime, AtomicCounter cycleTimeThresholdExceededCount, long cycleTimeThresholdNs)
        {
            this.maxCycleTime = maxCycleTime;
            this.cycleTimeThresholdExceededCount = cycleTimeThresholdExceededCount;
            this.cycleTimeThresholdNs = cycleTimeThresholdNs;
        }

        /// <summary>
        /// Get max cycle time counter.
        /// </summary>
        /// <returns> max cycle time counter. </returns>
        public AtomicCounter MaxCycleTime()
        {
            return maxCycleTime;
        }

        /// <summary>
        /// Get threshold exceeded counter.
        /// </summary>
        /// <returns> threshold exceeded counter. </returns>
        public AtomicCounter CycleTimeThresholdExceededCount()
        {
            return cycleTimeThresholdExceededCount;
        }

        /// <summary>
        /// Get threshold value.
        /// </summary>
        /// <returns> threshold value. </returns>
        public long CycleTimeThresholdNs()
        {
            return cycleTimeThresholdNs;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override void ReportMeasurement(long durationNs)
        {
            maxCycleTime.ProposeMaxOrdered(durationNs);

            if (durationNs > cycleTimeThresholdNs)
            {
                cycleTimeThresholdExceededCount.IncrementOrdered();
            }
        }
    }
}