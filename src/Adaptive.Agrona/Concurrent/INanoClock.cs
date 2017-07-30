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

namespace Adaptive.Agrona.Concurrent
{

    /// <summary>
    /// Functional interface for return the current time as system wide monotonic tick of 1 nanosecond precision.
    /// </summary>
    public interface INanoClock
    {
        /// <summary>
        /// The number of ticks in nanoseconds the clock has advanced since starting.
        /// </summary>
        /// <returns> number of ticks in nanoseconds the clock has advanced since starting.</returns>
        long NanoTime();
    }

}