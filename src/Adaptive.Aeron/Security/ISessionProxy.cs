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

namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Representation of a session during the authentication process from the perspective of an
    /// <seealso cref="IAuthenticator"/>.
    /// </summary>
    /// <seealso cref="IAuthenticator"/>
    public interface ISessionProxy
    {
        /// <summary>
        /// The identity of the potential session assigned by the system.
        /// </summary>
        /// <returns> identity for the potential session. </returns>
        long SessionId();

        /// <summary>
        /// Inform the system that the session requires a challenge by sending the provided encoded challenge.
        /// </summary>
        /// <param name="encodedChallenge"> to be sent to the client. </param>
        /// <returns> true if challenge was accepted to be sent at present time or false if it will be retried later.
        /// </returns>
        bool Challenge(byte[] encodedChallenge);

        /// <summary>
        /// Inform the system that the session has met authentication requirements.
        /// </summary>
        /// <param name="encodedPrincipal"> that has passed authentication. </param>
        /// <returns> true if authentication was accepted at present time or false if it will be retried later.
        /// </returns>
        bool Authenticate(byte[] encodedPrincipal);

        /// <summary>
        /// Inform the system that the session should be rejected.
        /// </summary>
        void Reject();
    }
}
