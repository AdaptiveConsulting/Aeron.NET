﻿/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Caused when a error occurs during addition or release of <seealso cref="Publication"/>s
    /// or <seealso cref="Subscription"/>s
    /// </summary>
    public class RegistrationException : AeronException
    {
        private readonly int _errorCodeValue;
        private readonly ErrorCode _code;

        public RegistrationException(int errorCodeValue, ErrorCode code, string msg) : base(msg)
        {
            _errorCodeValue = errorCodeValue;
            _code = code;
        }

        /// <summary>
        /// Get the <seealso cref="ErrorCode"/> for the specific exception.
        /// </summary>
        /// <returns> the <seealso cref="ErrorCode"/> for the specific exception. </returns>
        public ErrorCode ErrorCode()
        {
            return _code;
        }

        /// <summary>
        /// Value of the errorCode encoded. This can provide additional information when a
        /// <seealso cref="Adaptive.Aeron.ErrorCode.UNKNOWN_CODE_VALUE"/> is returned.
        /// </summary>
        /// <returns> value of the errorCode encoded as an int. </returns>
        public int ErrorCodeValue()
        {
            return _errorCodeValue;
        }

        public override string Message => "errorCodeValue=" + _errorCodeValue + " " + base.Message;
    }
}