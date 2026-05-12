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

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Category of <seealso cref="Exception"/>
    /// </summary>
    public enum Category
    {
        /// <summary>
        /// Exception indicates a fatal condition. Recommendation is to terminate process immediately to avoid state
        /// corruption.
        /// </summary>
        FATAL,

        /// <summary>
        /// Exception is an error. Corrective action is recommended if understood, otherwise treat as fatal.
        /// </summary>
        ERROR,

        /// <summary>
        /// Exception is a warning. Action has been, or will be, taken to handle the condition. Additional corrective
        /// action by the application may be needed.
        /// </summary>
        WARN,
    }
}
