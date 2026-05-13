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
using System.IO;

namespace Adaptive.Agrona.Concurrent.Errors
{
    /// <summary>
    /// A logging <seealso cref="ErrorHandler"/> that records to a <seealso cref="DistinctErrorLog"/> and if the log is
    /// full then overflows to a <seealso cref="TextWriter"/> .
    /// </summary>
    public class LoggingErrorHandler : IErrorHandler, IDisposable
    {
        private readonly DistinctErrorLog _log;
        private readonly TextWriter _errorOverflow;

        /// <summary>
        /// Construct error handler wrapping a <seealso cref="DistinctErrorLog"/> with a default of
        /// <seealso cref="Console.Error"/> for the
        /// <seealso cref="ErrorOverflow"/>.
        /// </summary>
        /// <param name="log"> to wrap. </param>
        public LoggingErrorHandler(DistinctErrorLog log)
            : this(log, Console.Error)
        {
        }

        /// <summary>
        /// Construct error handler wrapping a <seealso cref="DistinctErrorLog"/> and <seealso cref="TextWriter"/> for
        /// error overflow.
        /// </summary>
        /// <param name="log">           to wrap. </param>
        /// <param name="errorOverflow"> to be used if the log fills. </param>
        public LoggingErrorHandler(DistinctErrorLog log, TextWriter errorOverflow)
        {
            Objects.RequireNonNull(log, "log");
            Objects.RequireNonNull(log, "errorOverflow");

            _log = log;
            _errorOverflow = errorOverflow;
        }

        /// <summary>
        /// The wrapped log.
        /// </summary>
        /// <returns> the wrapped log. </returns>
        public DistinctErrorLog DistinctErrorLog()
        {
            return _log;
        }

        /// <summary>
        /// The wrapped <seealso cref="TextWriter"/> for error log overflow when the log is full.
        /// </summary>
        /// <returns> wrapped <seealso cref="TextWriter"/> for error log overflow when the log is full. </returns>
        public TextWriter ErrorOverflow()
        {
            return _errorOverflow;
        }

        public void OnError(Exception exception)
        {
            if (IsDiposed)
            {
                _errorOverflow.WriteLine("error log is closed");
                _errorOverflow.WriteLine(exception.ToString());
            }
            else if (!_log.Record(exception))
            {
                _errorOverflow.WriteLine("error log is full, consider increasing length of error buffer");
                _errorOverflow.WriteLine(exception.ToString());
            }
        }

        public void Dispose()
        {
            IsDiposed = true;
        }

        public bool IsDiposed { get; private set; }
    }
}
