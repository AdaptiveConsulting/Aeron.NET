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
    /// List of events used in the control protocol between client and the media driver.
    /// </summary>
    public class ControlProtocolEvents
    {
        // Clients to Media Driver

        /// <summary>
        /// Add Publication.
        /// </summary>
        public const int ADD_PUBLICATION = 0x01;

        /// <summary>
        /// Remove Publication.
        /// </summary>
        public const int REMOVE_PUBLICATION = 0x02;

        /// <summary>
        /// Add an Exclusive Publication.
        /// </summary>
        public const int ADD_EXCLUSIVE_PUBLICATION = 0x03;

        /// <summary>
        /// Add a Subscriber.
        /// </summary>
        public const int ADD_SUBSCRIPTION = 0x04;

        /// <summary>
        /// Remove a Subscriber.
        /// </summary>
        public const int REMOVE_SUBSCRIPTION = 0x05;

        /// <summary>
        /// Keepalive from Client.
        /// </summary>
        public const int CLIENT_KEEPALIVE = 0x06;

        /// <summary>
        /// Add Destination to an existing Publication.
        /// </summary>
        public const int ADD_DESTINATION = 0x07;

        /// <summary>
        /// Remove Destination from an existing Publication.
        /// </summary>
        public const int REMOVE_DESTINATION = 0x08;
        
        /// <summary>
        /// Add a Counter to the counters-manager.
        /// </summary>
        public const int ADD_COUNTER = 0x09;

        /// <summary>
        /// Remove a Counter from the counters-manager.
        /// </summary>
        public const int REMOVE_COUNTER = 0x0A;

        /// <summary>
        /// Close indication from Client.
        /// </summary>
        public const int CLIENT_CLOSE = 0x0B;
        
        /// <summary>
        /// Add Destination for existing Subscription.
        /// </summary>
        public const int ADD_RCV_DESTINATION = 0x0C;

        /// <summary>
        /// Remove Destination for existing Subscription.
        /// </summary>
        public const int REMOVE_RCV_DESTINATION = 0x0D;

        /// <summary>
        /// Request the driver to terminate.
        /// </summary>
        public const int TERMINATE_DRIVER = 0x0E;
        
        // Media Driver to Clients

        /// <summary>
        /// Error Response as a result of attempting to process a client command operation.
        /// </summary>
        public const int ON_ERROR = 0x0F01;

        /// <summary>
        /// Subscribed Image buffers are available notification.
        /// </summary>
        public const int ON_AVAILABLE_IMAGE = 0x0F02;
        
        /// <summary>
        /// New Publication buffers are ready notification.
        /// </summary>
        public const int ON_PUBLICATION_READY = 0x0F03;

        /// <summary>
        /// Operation has succeeded.
        /// </summary>
        public const int ON_OPERATION_SUCCESS = 0x0F04;

        /// <summary>
        /// Inform client of timeout and removal of an inactive Image.
        /// </summary>
        public const int ON_UNAVAILABLE_IMAGE = 0x0F05;

        /// <summary>
        /// New Exclusive Publication buffers are ready notification.
        /// </summary>
        public const int ON_EXCLUSIVE_PUBLICATION_READY = 0x0F06;
        
        /// <summary>
        /// New Subscription is ready notification.
        /// </summary>
        public const int ON_SUBSCRIPTION_READY = 0x0F07;

        /// <summary>
        /// New counter is ready notification.
        /// </summary>
        public const int ON_COUNTER_READY = 0x0F08;

        /// <summary>
        /// Inform clients of removal of counter.
        /// </summary>
        public const int ON_UNAVAILABLE_COUNTER = 0x0F09;
        
        /// <summary>
        /// Inform clients of client timeout.
        /// </summary>
        public const int ON_CLIENT_TIMEOUT = 0x0F0A;
    }

}