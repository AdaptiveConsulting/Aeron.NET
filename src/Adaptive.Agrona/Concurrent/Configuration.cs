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
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
    public class Configuration
    {
        /// <summary>
        /// Spin on no activity before backing off to yielding.
        /// </summary>
        public const long IDLE_MAX_SPINS = 10;

        /// <summary>
        /// Yield the thread so others can run before backing off to parking.
        /// </summary>
        public const long IDLE_MAX_YIELDS = 40;

        /// <summary>
        /// Park for the minimum period of time which is typically 50-55 microseconds on 64-bit non-virtualised Linux.
        /// You will typically get 50-55 microseconds plus the number of nanoseconds requested if a core is available.
        /// On Windows expect to wait for at least 16ms or 1ms if the high-res timers are enabled.
        /// </summary>
        public const long IDLE_MIN_PARK_MS = 1;

        /// <summary>
        /// Maximum back-off park time which doubles on each interval stepping up from the min park idle.
        /// </summary>
        public static readonly long IDLE_MAX_PARK_MS = 16;
    }
}
