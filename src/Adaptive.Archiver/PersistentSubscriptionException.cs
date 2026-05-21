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

using Adaptive.Aeron.Exceptions;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Exception raised when using a <see cref="PersistentSubscription"/>.
    /// </summary>
    /// <remarks>Since 1.51.0</remarks>
    public sealed class PersistentSubscriptionException : AeronException
    {
        /// <summary>
        /// Reason a <see cref="PersistentSubscriptionException"/> occurred.
        /// </summary>
        public enum Reason
        {
            /// <summary>A generic reason in case no specific reason is available.</summary>
            GENERIC,

            /// <summary>No recording exists with the specified recording id.</summary>
            RECORDING_NOT_FOUND,

            /// <summary>The requested live stream id does not match the stream id for the recording.</summary>
            STREAM_ID_MISMATCH,

            /// <summary>The requested start position is not available for the specified recording.</summary>
            INVALID_START_POSITION
        }

        /// <summary>
        /// Persistent Subscription exception with a detailed message and provided reason.
        /// </summary>
        /// <param name="reason">  for the error. </param>
        /// <param name="message"> providing detail on the error. </param>
        public PersistentSubscriptionException(Reason reason, string message)
            : base(message)
        {
            ReasonValue = reason;
        }

        /// <summary>
        /// The reason indicating the type of error that caused the exception.
        /// </summary>
        public Reason ReasonValue { get; }
    }
}
