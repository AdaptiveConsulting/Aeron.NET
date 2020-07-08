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
    /// Callback interface for dispatching command responses from the driver on the control protocol.
    /// </summary>
    internal interface IDriverEventsListener
    {
        void OnError(long correlationId, int codeValue, ErrorCode errorCode, string message);

        void OnAsyncError(long correlationId, int codeValue, ErrorCode errorCode, string message);
        
        void OnAvailableImage(
            long correlationId,
            int sessionId,
            long subscriberRegistrationId,
            int subscriberPositionId,
            string logFileName,
            string sourceIdentity);

        void OnNewPublication(
            long correlationId,
            long registrationId,
            int streamId,
            int sessionId,
            int publicationLimitId,
            int statusIndicatorId,
            string logFileName);

        void OnNewSubscription(long correlationId, int statusIndicatorId);

        void OnUnavailableImage(long correlationId, long subscriptionRegistrationId);

        void OnNewExclusivePublication(
            long correlationId,
            long registrationid,
            int streamId,
            int sessionId,
            int publicationLimitId,
            int statusIndicatorId,
            string logFileName);

        void OnChannelEndpointError(int statusIndicatorId, string message);

        void OnNewCounter(long correlationId, int counterId);

        void OnAvailableCounter(long correlationId, int counterId);

        void OnUnavailableCounter(long correlationId, int counterId);

        void OnClientTimeout();
    }
}