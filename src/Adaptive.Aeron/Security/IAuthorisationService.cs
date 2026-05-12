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
    /// Interface for an authorisation service to handle authorisation checks on clients performing actions to a system.
    /// </summary>
    /// <seealso cref="IAuthorisationServiceSupplier"/>
    public interface IAuthorisationService
    {
        /// <summary>
        /// Checks if the client with authenticated credentials is allowed to perform an action indicated by the given
        /// {@code actionId} .
        /// </summary>
        /// <param name="protocolId">       of the protocol to which the action belongs, e.g. a SBE schema id. </param>
        /// <param name="actionId">         of the command being checked, e.g. a SBE message template id. </param>
        /// <param name="type"> optional type for the action being checked, may be {@code null}. For example for
        /// an admin request in the cluster it will contain {@code AdminRequestType} value which denotes the exact kind
        /// of the request. </param>
        /// <param name="encodedPrincipal"> that has been authenticated. </param>
        /// <returns> {@code true} if the client is authorised to execute the action or {@code false} otherwise.
        /// </returns>
        bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal);
    }

    /// <summary>
    /// An <seealso cref="IAuthorisationService"/> instance that allows every action.
    /// </summary>
    public class AllowAllAuthorisationService : IAuthorisationService
    {
        public static readonly IAuthorisationService INSTANCE = new AllowAllAuthorisationService();

        /// <summary>
        /// Special case token for authorisation service supplier that allow all requests.
        /// </summary>
        public static readonly string ALLOW_ALL_NAME = "ALLOW_ALL";

        public bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal)
        {
            return true;
        }
    }

    /// <summary>
    /// An <seealso cref="IAuthorisationService"/> instance that forbids all actions.
    /// </summary>
    public class DenyAllAuthorisationService : IAuthorisationService
    {
        public static readonly IAuthorisationService INSTANCE = new DenyAllAuthorisationService();

        /// <summary>
        /// Special case token for authorisation service supplier that will deny all requests.
        /// </summary>
        public static readonly string DENY_ALL_NAME = "DENY_ALL";

        public bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal)
        {
            return false;
        }
    }
}
