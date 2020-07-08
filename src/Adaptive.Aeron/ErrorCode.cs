/*
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

namespace Adaptive.Aeron
{
    /// <summary>
    /// Error codes between media driver and client and the on-wire protocol.
    /// </summary>
    public enum ErrorCode
    {
        /// <summary>
        /// Aeron encountered an error condition. 
        /// </summary>
        GENERIC_ERROR = 0,

        /// <summary>
        /// A failure occurred creating a new channel or parsing the channel string. 
        /// </summary>
        INVALID_CHANNEL = 1,

        /// <summary>
        /// Attempted to reference a subscription, but it was not found.
        /// </summary>
        UNKNOWN_SUBSCRIPTION = 2,

        /// <summary>
        /// Attempted to reference a publication, but it was not found.
        /// </summary>
        UNKNOWN_PUBLICATION = 3,

        /// <summary>
        /// Channel Endpoint could not be successfully opened.
        /// </summary>
        CHANNEL_ENDPOINT_ERROR = 4,

        /// <summary>
        /// Attempted to reference a counter, but it was not found.
        /// </summary>
        UNKNOWN_COUNTER = 5,

        /// <summary>
        /// Attempted to send a command unknown by the driver.
        /// </summary>
        UNKNOWN_COMMAND_TYPE_ID = 6,

        /// <summary>
        /// Attempted to send a command that is malformed. Typically, too short.
        /// </summary>
        MALFORMED_COMMAND = 7,

        /// <summary>
        /// Attempted to send a command known by the driver, but not currently supported.
        /// </summary>
        NOT_SUPPORTED = 8,

        /// <summary>
        /// Attempted to send a command that had a host name the could be resolved.
        /// </summary>
        UNKNOWN_HOST = 9,

        /// <summary>
        /// Attempted to send a command that referred to a resource that was unavailable.
        /// </summary>
        RESOURCE_TEMPORARILY_UNAVAILABLE = 10,

        /// <summary>
        /// A code value returned was not known.
        /// </summary>
        UNKNOWN_CODE_VALUE = -1
    }
}