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

namespace Adaptive.Aeron.Command
{
    /// <summary>
    /// List of event types used in the control protocol between the
    /// media driver and the core.
    /// </summary>
    public class ControlProtocolEvents
    {
        // Clients to Media Driver

        /// <summary>
        /// Add Publication </summary>
        public const int ADD_PUBLICATION = 0x01;
        /// <summary>
        /// Remove Publication </summary>
        public const int REMOVE_PUBLICATION = 0x02;
        /// <summary>
        /// Add Subscriber </summary>
        public const int ADD_SUBSCRIPTION = 0x04;
        /// <summary>
        /// Remove Subscriber </summary>
        public const int REMOVE_SUBSCRIPTION = 0x05;
        /// <summary>
        /// Keepalive from Client </summary>
        public const int CLIENT_KEEPALIVE = 0x06;
        /// <summary>
        /// Add Destination </summary>
        public const int ADD_DESTINATION = 0x07;
        /// <summary>
        /// Remove Destination </summary>
        public const int REMOVE_DESTINATION = 0x08;

        // Media Driver to Clients

        /// <summary>
        /// Error Response </summary>
        public const int ON_ERROR = 0x0F01;
        /// <summary>
        /// New subscription Buffer Notification </summary>
        public const int ON_AVAILABLE_IMAGE = 0x0F02;
        /// <summary>
        /// New publication Buffer Notification </summary>
        public const int ON_PUBLICATION_READY = 0x0F03;
        /// <summary>
        /// Operation Succeeded </summary>
        public const int ON_OPERATION_SUCCESS = 0x0F04;
        /// <summary>
        /// Inform client of timeout and removal of inactive image </summary>
        public const int ON_UNAVAILABLE_IMAGE = 0x0F05;
    }

}