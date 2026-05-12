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
    public class ControlProtocolException : AeronException
    {
        private readonly ErrorCode _code;

        /// <summary>
        /// Construct an exception to indicate an invalid command has been sent to the media driver.
        /// </summary>
        /// <param name="code"> for the type of error. </param>
        /// <param name="msg">  providing more detail. </param>
        public ControlProtocolException(ErrorCode code, string msg)
            : base(msg)
        {
            _code = code;
        }

        /// <summary>
        /// Construct an exception to indicate an invalid command has been sent to the media driver.
        /// </summary>
        /// <param name="code">      for the type of error. </param>
        /// <param name="rootCause"> of the error. </param>
        public ControlProtocolException(ErrorCode code, Exception rootCause)
            : base(rootCause)
        {
            _code = code;
        }

        /// <summary>
        /// Construct an exception to indicate an invalid command has been sent to the media driver.
        /// </summary>
        /// <param name="code">      for the type of error. </param>
        /// <param name="msg">       providing more detail. </param>
        /// <param name="rootCause"> of the error. </param>
        public ControlProtocolException(ErrorCode code, string msg, Exception rootCause)
            : base(msg, rootCause)
        {
            _code = code;
        }

        /// <summary>
        /// The <seealso cref="ErrorCode"/> indicating more specific issue experienced by the media driver.
        /// </summary>
        /// <returns> <seealso cref="Adaptive.Aeron.ErrorCode"/> indicating more specific issue experienced by the media
        /// driver. </returns>
        public ErrorCode ErrorCode()
        {
            return _code;
        }
    }
}
