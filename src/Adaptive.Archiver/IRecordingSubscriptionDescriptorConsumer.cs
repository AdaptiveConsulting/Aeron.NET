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

using Adaptive.Aeron;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Consumer for descriptors of active archive recording <seealso cref="Subscription"/> s.
    /// </summary>
    public interface IRecordingSubscriptionDescriptorConsumer
    {
        /// <summary>
        /// Descriptor for an active recording subscription on the archive.
        /// </summary>
        /// <param name="controlSessionId"> for the request. </param>
        /// <param name="correlationId">    for the request. </param>
        /// <param name="subscriptionId">   that can be used to stop the recording subscription. </param>
        /// <param name="streamId">         the subscription was registered with. </param>
        /// <param name="strippedChannel">  the subscription was registered with. </param>
        void OnSubscriptionDescriptor(
            long controlSessionId,
            long correlationId,
            long subscriptionId,
            int streamId,
            string strippedChannel
        );
    }
}
