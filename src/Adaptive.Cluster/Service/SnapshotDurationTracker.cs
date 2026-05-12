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

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Snapshot duration tracker that tracks maximum snapshot duration and also keeps count of how many times a
    /// predefined duration threshold is breached.
    /// </summary>
    public class SnapshotDurationTracker
    {
        private readonly AtomicCounter _maxSnapshotDuration;
        private readonly AtomicCounter _snapshotDurationThresholdExceededCount;
        private readonly long _durationThresholdNs;
        private long _snapshotStartTimeNs = long.MinValue;

        /// <summary>
        /// Create a tracker to track max snapshot duration and breaches of a threshold.
        /// </summary>
        /// <param name="maxSnapshotDuration">                    counter for tracking. </param>
        /// <param name="snapshotDurationThresholdExceededCount"> counter for tracking. </param>
        /// <param name="durationThresholdNs">                    to use for tracking breaches. </param>
        public SnapshotDurationTracker(
            AtomicCounter maxSnapshotDuration,
            AtomicCounter snapshotDurationThresholdExceededCount,
            long durationThresholdNs
        )
        {
            this._maxSnapshotDuration = maxSnapshotDuration;
            this._snapshotDurationThresholdExceededCount = snapshotDurationThresholdExceededCount;
            this._durationThresholdNs = durationThresholdNs;
        }

        /// <summary>
        /// Get max snapshot duration counter.
        /// </summary>
        /// <returns> max snapshot duration counter. </returns>
        public AtomicCounter MaxSnapshotDuration()
        {
            return _maxSnapshotDuration;
        }

        /// <summary>
        /// Get counter tracking number of times <seealso cref="SnapshotDurationTracker._durationThresholdNs"/> was
        /// exceeded.
        /// </summary>
        /// <returns> duration threshold exceeded counter. </returns>
        public AtomicCounter SnapshotDurationThresholdExceededCount()
        {
            return _snapshotDurationThresholdExceededCount;
        }

        /// <summary>
        /// Called when snapshotting has started.
        /// </summary>
        /// <param name="timeNanos"> snapshot start time in nanoseconds. </param>
        public void OnSnapshotBegin(long timeNanos)
        {
            _snapshotStartTimeNs = timeNanos;
        }

        /// <summary>
        /// Called when snapshot has been taken.
        /// </summary>
        /// <param name="timeNanos"> snapshot end time in nanoseconds. </param>
        public void OnSnapshotEnd(long timeNanos)
        {
            if (_snapshotStartTimeNs != long.MinValue)
            {
                long snapshotDurationNs = timeNanos - _snapshotStartTimeNs;

                if (snapshotDurationNs > _durationThresholdNs)
                {
                    _snapshotDurationThresholdExceededCount.Increment();
                }

                _maxSnapshotDuration.ProposeMax(snapshotDurationNs);
            }
        }

        /// <summary>
        /// {@inheritDoc}
        /// </summary>
        public override string ToString()
        {
            return "SnapshotDurationTracker{"
                + "maxSnapshotDuration="
                + _maxSnapshotDuration
                + ", snapshotDurationThresholdExceededCount="
                + _snapshotDurationThresholdExceededCount
                + ", durationThresholdNs="
                + _durationThresholdNs
                + '}';
        }
    }
}
