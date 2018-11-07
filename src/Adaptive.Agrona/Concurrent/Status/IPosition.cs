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

namespace Adaptive.Agrona.Concurrent.Status
{
    /// <summary>
    /// Reports on how far through a buffer some component has progressed.
    /// 
    /// Threadsafe to write to from a single writer.
    /// </summary>
    public interface IPosition : IReadablePosition
    {
        /// <summary>
        /// Get the current position of a component without memory ordering semantics.
        /// </summary>
        /// <returns> the current position of a component </returns>
        long Get();

        /// <summary>
        /// Sets the current position of the component without memory ordering semantics.
        /// </summary>
        /// <param name="value"> the current position of the component. </param>
        void Set(long value);

        /// <summary>
        /// Sets the current position of the component with ordered memory semantics.
        /// </summary>
        /// <param name="value"> the current position of the component. </param>
        void SetOrdered(long value);

        /// <summary>
        /// Sets the current position of the component with volatile memory semantics.
        /// </summary>
        /// <param name="value"> the current position of the component. </param>
        void SetVolatile(long value);
        
        /// <summary>
        /// Set the position to a new proposedValue if greater than the current value with memory ordering semantics.
        /// </summary>
        /// <param name="proposedValue"> for the new max. </param>
        /// <returns> true if a new max as been set otherwise false. </returns>
        bool ProposeMax(long proposedValue);

        /// <summary>
        /// Set the position to the new proposedValue if greater than the current value with memory ordering semantics.
        /// </summary>
        /// <param name="proposedValue"> for the new max. </param>
        /// <returns> true if a new max as been set otherwise false. </returns>
        bool ProposeMaxOrdered(long proposedValue);
    }
}