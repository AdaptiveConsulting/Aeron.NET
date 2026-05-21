/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
    /// A means to capture an Archive event of significance that does not require a stack trace,
    /// so it can be lighter-weight and take up less space in a
    /// <seealso cref="Adaptive.Agrona.Concurrent.Errors.DistinctErrorLog"/>.
    /// </summary>
    public class ArchiveEvent : AeronEvent
    {
        /// <summary>
        /// Archive event with provided message and <seealso cref="Category.WARN"/>.
        /// </summary>
        public ArchiveEvent(string message)
            : base(message, Category.WARN)
        {
        }

        /// <summary>
        /// Archive event with provided message and <seealso cref="Category"/>.
        /// </summary>
        public ArchiveEvent(string message, Category category)
            : base(message, category)
        {
        }
    }
}
