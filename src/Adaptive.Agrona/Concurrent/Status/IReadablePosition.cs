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

using System;

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Indicates how far through an abstract task a component has progressed as a counter value.
    /// </summary>
    public interface IReadablePosition : IDisposable
    {
        /// <summary>
        /// Identifier for this position.
        /// </summary>
        /// <returns> the identifier for this position. </returns>
        int Id();

        /// <summary>
        /// Get the current position of a component with volatile semantics
        /// </summary>
        /// <returns> the current position of a component with volatile semantics </returns>
        long Volatile { get; }
    }
}