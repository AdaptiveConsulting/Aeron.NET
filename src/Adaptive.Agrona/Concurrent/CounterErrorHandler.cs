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

using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Agrona.Concurrent
{
    using System;

    /// <summary>
    /// An <seealso cref="ErrorHandler"/> which calls <seealso cref="AtomicCounter.Increment()"/> before delegating the
    /// exception.
    /// </summary>
    public class CountedErrorHandler : IErrorHandler
    {
        private readonly IErrorHandler _errorHandler;
        private readonly AtomicCounter _errorCounter;

        public readonly ErrorHandler AsErrorHandler;

        /// <summary>
        /// Construct a counted error handler with a delegate and counter.
        /// </summary>
        /// <param name="errorHandler"> to delegate to. </param>
        /// <param name="errorCounter"> to increment before delegation. </param>
        public CountedErrorHandler(IErrorHandler errorHandler, AtomicCounter errorCounter)
        {
            Objects.RequireNonNull(errorHandler, "errorHandler");
            Objects.RequireNonNull(errorCounter, "errorCounter");

            _errorHandler = errorHandler;
            _errorCounter = errorCounter;

            AsErrorHandler = OnError;
        }

        public void OnError(Exception throwable)
        {
            _errorCounter.Increment();
            _errorHandler.OnError(throwable);
        }
    }
}
