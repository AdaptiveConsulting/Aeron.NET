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

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Status of the Aeron media channel for a <seealso cref="Publication"/> or <seealso cref="Subscription"/> .
    /// </summary>
    public static class ChannelEndpointStatus
    {
        /// <summary>
        /// Channel is being initialized.
        /// </summary>
        public const long INITIALIZING = 0;

        /// <summary>
        /// Channel has errored. Check error log for information.
        /// </summary>
        public const long ERRORED = -1;

        /// <summary>
        /// Channel has finished initialization successfully and is active.
        /// </summary>
        public const long ACTIVE = 1;

        /// <summary>
        /// Channel is being closed.
        /// </summary>
        public const long CLOSING = 2;

        /// <summary>
        /// No counter ID is allocated yet.
        /// </summary>
        public const int NO_ID_ALLOCATED = Aeron.NULL_VALUE;

        /// <summary>
        /// Offset in the key metadata for the channel of the counter.
        /// </summary>
        public const int CHANNEL_OFFSET = 0;
    }
}
