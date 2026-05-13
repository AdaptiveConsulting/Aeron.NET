/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Duty cycle tracker that detects when a cycle exceeds a threshold and tracks max cycle time reporting both
    /// through counters.
    /// </summary>
    public class DutyCycleStallTracker : DutyCycleTracker
    {
        private readonly AtomicCounter _maxCycleTime;
        private readonly AtomicCounter _cycleTimeThresholdExceededCount;
        private readonly long _cycleTimeThresholdNs;

        /// <summary>
        /// Create a tracker to track max cycle time and excesses of a threshold.
        /// </summary>
        /// <param name="maxCycleTime"> counter for tracking. </param>
        /// <param name="cycleTimeThresholdExceededCount"> counter for tracking. </param>
        /// <param name="cycleTimeThresholdNs"> to use for tracking excesses. </param>
        public DutyCycleStallTracker(
            AtomicCounter maxCycleTime,
            AtomicCounter cycleTimeThresholdExceededCount,
            long cycleTimeThresholdNs
        )
        {
            _maxCycleTime = maxCycleTime;
            _cycleTimeThresholdExceededCount = cycleTimeThresholdExceededCount;
            _cycleTimeThresholdNs = cycleTimeThresholdNs;
        }

        /// <summary>
        /// Get max cycle time counter.
        /// </summary>
        /// <returns> max cycle time counter. </returns>
        public AtomicCounter MaxCycleTime()
        {
            return _maxCycleTime;
        }

        /// <summary>
        /// Get threshold exceeded counter.
        /// </summary>
        /// <returns> threshold exceeded counter. </returns>
        public AtomicCounter CycleTimeThresholdExceededCount()
        {
            return _cycleTimeThresholdExceededCount;
        }

        /// <summary>
        /// Get threshold value.
        /// </summary>
        /// <returns> threshold value. </returns>
        public long CycleTimeThresholdNs()
        {
            return _cycleTimeThresholdNs;
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override void ReportMeasurement(long durationNs)
        {
            _maxCycleTime.ProposeMaxOrdered(durationNs);

            if (durationNs > _cycleTimeThresholdNs)
            {
                _cycleTimeThresholdExceededCount.IncrementOrdered();
            }
        }
    }
}
