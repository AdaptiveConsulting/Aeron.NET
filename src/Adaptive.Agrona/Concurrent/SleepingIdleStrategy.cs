/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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

using System.Threading;

namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// When idle this strategy is to sleep for a specified period.
    /// </summary>
    public sealed class SleepingIdleStrategy : IIdleStrategy
    {
        private readonly int _sleepPeriodMs;

        /// <summary>
        /// Constructed a new strategy that will sleep for a given period when idle.
        /// </summary>
        /// <param name="sleepPeriodMs"> period in millisecond for which the strategy will sleep when work count is 0. </param>

        public SleepingIdleStrategy(int sleepPeriodMs)
        {
            _sleepPeriodMs = sleepPeriodMs;
        }

        public void Idle(int workCount)
        {
            if (workCount > 0)
            {
                return;
            }

            Thread.Sleep(_sleepPeriodMs);
        }

        public void Idle()
        {
            Thread.Sleep(_sleepPeriodMs);
        }

        public void Reset()
        {
        }
    }

}