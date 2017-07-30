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

using System.Collections.Generic;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Callback interface for dispatching command responses from the driver on the control protocol.
    /// </summary>
    internal interface IDriverListener
    {
        void OnError(ErrorCode errorCode, string message, long correlationId);

        void OnAvailableImage(
            int streamId, 
            int sessionId, 
            IDictionary<long, long> subscriberPositionMap, 
            string logFileName, 
            string sourceIdentity, 
            long correlationId);

        void OnNewPublication(
            string channel, 
            int streamId, 
            int sessionId, 
            int publicationLimitId, 
            string logFileName, 
            long correlationId);

        void OnUnavailableImage(int streamId, long correlationId);

        void OnNewExclusivePublication(
            string channel, 
            int streamId, 
            int sessionId, 
            int publicationLimitId, 
            string logFileName, 
            long correlationId);
    }

}