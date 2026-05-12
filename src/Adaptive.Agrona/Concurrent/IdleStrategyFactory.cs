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

namespace Adaptive.Agrona.Concurrent
{
    public static class IdleStrategyFactory
    {
        public static IIdleStrategy Create(string strategyName, StatusIndicator controllableStatus)
        {
            switch (strategyName)
            {
                case "ControllableIdleStrategy":
                    var idleStrategy = new ControllableIdleStrategy(controllableStatus);
                    controllableStatus.SetOrdered(ControllableIdleStrategy.PARK);
                    return idleStrategy;

                case "YieldingIdleStrategy":
                    return new YieldingIdleStrategy();

                case "SleepingIdleStrategy":
                    return new SleepingIdleStrategy(1);

                case "BusySpinIdleStrategy":
                    return new BusySpinIdleStrategy();

                case "NoOpIdleStrategy":
                    return new NoOpIdleStrategy();

                default:
                    return new BackoffIdleStrategy(
                        Configuration.IDLE_MAX_SPINS,
                        Configuration.IDLE_MAX_YIELDS,
                        Configuration.IDLE_MIN_PARK_MS,
                        Configuration.IDLE_MAX_PARK_MS
                    );
            }
        }
    }
}
