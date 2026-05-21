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

using System;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Interface for delivering notifications about changes in state of a <see cref="PersistentSubscription"/>.
    /// </summary>
    /// <remarks>Since 1.51.0</remarks>
    public interface IPersistentSubscriptionListener
    {
        /// <summary>
        /// Called when the <see cref="PersistentSubscription"/> transitions to consuming from the live channel.
        /// </summary>
        void OnLiveJoined();

        /// <summary>
        /// Called when the <see cref="PersistentSubscription"/> stops consuming from the live channel.
        /// </summary>
        void OnLiveLeft();

        /// <summary>
        /// Called when the <see cref="PersistentSubscription"/> encounters an error.
        /// <para>
        /// This will be called for both terminal and non-terminal errors.
        /// </para>
        /// </summary>
        /// <param name="e"> the exception that caused the error. </param>
        /// <seealso cref="PersistentSubscription.HasFailed"/>
        void OnError(Exception e);
    }
}
