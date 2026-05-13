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

namespace Adaptive.Aeron
{
    /// <summary>
    /// Tracker to handle tracking the duration of a duty cycle.
    /// </summary>
    public class DutyCycleTracker
    {
        private long _timeOfLastUpdateNs;

        /// <summary>
        /// Update the last known clock time.
        /// </summary>
        /// <param name="nowNs"> to update with. </param>
        public void Update(long nowNs)
        {
            _timeOfLastUpdateNs = nowNs;
        }

        /// <summary>
        /// Pass measurement to tracker and report updating last known clock time with time.
        /// </summary>
        /// <param name="nowNs"> of the measurement. </param>
        public void MeasureAndUpdate(long nowNs)
        {
            long cycleTimeNs = nowNs - _timeOfLastUpdateNs;

            ReportMeasurement(cycleTimeNs);
            _timeOfLastUpdateNs = nowNs;
        }

        /// <summary>
        /// Callback called to report duration of cycle.
        /// </summary>
        /// <param name="durationNs"> of the duty cycle. </param>
        public virtual void ReportMeasurement(long durationNs)
        {
        }
    }
}
