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
    /// Supplier of credentials for authentication with a system.
    ///
    /// Implement this interface to supply credentials for clients. If no credentials are required then the
    /// <seealso cref="NullCredentialsSupplier"/> can be used.
    /// </summary>
    public interface ICredentialsSupplier
    {
        /// <summary>
        /// Provide a credential to be included in Session Connect message to a system.
        /// </summary>
        /// <returns> a credential in binary form to be included in the Session Connect message to system. </returns>
        byte[] EncodedCredentials();

        /// <summary>
        /// Given some encoded challenge data, provide the credentials to be included in a Challenge Response as part of
        /// authentication with a system.
        /// </summary>
        /// <param name="endcodedChallenge"> from the cluster to use in providing a credential. </param>
        /// <returns> encoded credentials in binary form to be included in the Challenge Response to the system.
        /// </returns>
        byte[] OnChallenge(byte[] endcodedChallenge);
    }
}
