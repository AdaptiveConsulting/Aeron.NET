/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for encapsulating the strategy of mapping <seealso cref="LogBuffers"/> at a giving file location.
    /// </summary>
    public interface ILogBuffersFactory
    {
        /// <summary>
        /// Map a log file into memory and wrap each section with a <seealso cref="UnsafeBuffer"/>.
        /// </summary>
        /// <param name="logFileName"> to be mapped into memory. </param>
        /// <param name="mapMode"> the mode to be used for the file.</param>
        /// <returns> a representation of the mapped log buffer. </returns>
        LogBuffers Map(string logFileName, MapMode mapMode);
    }
}