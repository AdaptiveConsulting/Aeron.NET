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

namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Null provider of credentials when no authentication is required.
    /// </summary>
    public class NullCredentialsSupplier : ICredentialsSupplier
    {
        /// <summary>
        /// Null credentials are an empty array of bytes.
        /// </summary>
        public static readonly byte[] NULL_CREDENTIAL = Array.Empty<byte>();

        /// <inheritdoc />
        public byte[] EncodedCredentials()
        {
            return NULL_CREDENTIAL;
        }

        /// <inheritdoc />
        public byte[] OnChallenge(byte[] endcodedChallenge)
        {
            return NULL_CREDENTIAL;
        }
    }
}
