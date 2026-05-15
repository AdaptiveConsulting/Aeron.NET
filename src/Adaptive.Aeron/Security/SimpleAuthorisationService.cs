/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;

// Direct port from io.aeron.security.SimpleAuthorisationService. Internal field naming
// kept PascalCase for parity; line lengths kept to Java structure.
#pragma warning disable IDE1006, IDE0041, S103
namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Authorisation service that supports setting general and per-principal rules scoped to protocol,
    /// action, and type. Uses a fluent <see cref="Builder"/> API to add authorisation rules.
    /// </summary>
    public sealed class SimpleAuthorisationService : IAuthorisationService
    {
        private readonly IAuthorisationService _defaultAuthorisation;
        private readonly Dictionary<ByteArrayKey, Principal> _principalByKeyMap;
        private readonly Principal _defaultPrincipal;

        private SimpleAuthorisationService(Builder builder)
        {
            _defaultAuthorisation = builder.DefaultAuthorisation;
            _principalByKeyMap = new Dictionary<ByteArrayKey, Principal>(builder.PrincipalByKeyMap);
            _defaultPrincipal = builder.DefaultPrincipal;
        }

        public bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal)
        {
            _principalByKeyMap.TryGetValue(new ByteArrayKey(encodedPrincipal), out var principal);
            var result = IsAuthorised(principal, protocolId, actionId, type);
            if (result.HasValue)
            {
                return result.Value;
            }

            result = IsAuthorised(_defaultPrincipal, protocolId, actionId, type);
            if (result.HasValue)
            {
                return result.Value;
            }

            return _defaultAuthorisation.IsAuthorised(protocolId, actionId, type, encodedPrincipal);
        }

        private static bool? IsAuthorised(Principal principal, int protocolId, int actionId, object type)
        {
            return principal?.IsAuthorised(protocolId, actionId, type);
        }

        /// <summary>
        /// Builder to create the authorisation service.
        /// </summary>
        public sealed class Builder
        {
            internal IAuthorisationService DefaultAuthorisation = DenyAllAuthorisationService.INSTANCE;
            internal readonly Dictionary<ByteArrayKey, Principal> PrincipalByKeyMap =
                new Dictionary<ByteArrayKey, Principal>();
            internal readonly Principal DefaultPrincipal = new Principal(Array.Empty<byte>());

            public Builder WithDefaultAuthorisation(IAuthorisationService defaultAuthorisation)
            {
                DefaultAuthorisation = defaultAuthorisation;
                return this;
            }

            public Builder AddPrincipalRule(
                int protocolId, int actionId, object type, byte[] encodedPrincipal, bool isAllowed)
            {
                var principal = GetOrAddPrincipal(encodedPrincipal);
                var byTypeMap = isAllowed
                    ? principal.ByProtocolActionTypeAllowed
                    : principal.ByProtocolActionTypeDenied;
                GetOrAddSet(byTypeMap, protocolId, actionId).Add(type);
                return this;
            }

            public Builder AddPrincipalRule(int protocolId, int actionId, byte[] encodedPrincipal, bool isAllowed)
            {
                GetOrAddPrincipal(encodedPrincipal).ByProtocolAction[(protocolId, actionId)] = isAllowed;
                return this;
            }

            public Builder AddPrincipalRule(int protocolId, byte[] encodedPrincipal, bool isAllowed)
            {
                GetOrAddPrincipal(encodedPrincipal).ByProtocol[protocolId] = isAllowed;
                return this;
            }

            public Builder AddGeneralRule(int protocolId, int actionId, object type, bool isAllowed)
            {
                var byTypeMap = isAllowed
                    ? DefaultPrincipal.ByProtocolActionTypeAllowed
                    : DefaultPrincipal.ByProtocolActionTypeDenied;
                GetOrAddSet(byTypeMap, protocolId, actionId).Add(type);
                return this;
            }

            public Builder AddGeneralRule(int protocolId, int actionId, bool isAllowed)
            {
                DefaultPrincipal.ByProtocolAction[(protocolId, actionId)] = isAllowed;
                return this;
            }

            public Builder AddGeneralRule(int protocolId, bool isAllowed)
            {
                DefaultPrincipal.ByProtocol[protocolId] = isAllowed;
                return this;
            }

            public SimpleAuthorisationService NewInstance()
            {
                return new SimpleAuthorisationService(this);
            }

            private Principal GetOrAddPrincipal(byte[] encodedPrincipal)
            {
                var key = new ByteArrayKey(encodedPrincipal);
                if (!PrincipalByKeyMap.TryGetValue(key, out var principal))
                {
                    principal = new Principal(encodedPrincipal);
                    PrincipalByKeyMap[key] = principal;
                }
                return principal;
            }

            private static HashSet<object> GetOrAddSet(
                Dictionary<(int, int), HashSet<object>> map, int protocolId, int actionId)
            {
                var key = (protocolId, actionId);
                if (!map.TryGetValue(key, out var set))
                {
                    set = new HashSet<object>();
                    map[key] = set;
                }
                return set;
            }
        }

        internal sealed class Principal
        {
            internal readonly Dictionary<int, bool> ByProtocol = new Dictionary<int, bool>();
            internal readonly Dictionary<(int, int), bool> ByProtocolAction = new Dictionary<(int, int), bool>();
            internal readonly Dictionary<(int, int), HashSet<object>> ByProtocolActionTypeAllowed =
                new Dictionary<(int, int), HashSet<object>>();
            internal readonly Dictionary<(int, int), HashSet<object>> ByProtocolActionTypeDenied =
                new Dictionary<(int, int), HashSet<object>>();
            internal readonly byte[] EncodedPrincipal;

            internal Principal(byte[] encodedPrincipal)
            {
                EncodedPrincipal = encodedPrincipal;
            }

            internal bool? IsAuthorised(int protocolId, int actionId, object type)
            {
                if (ByProtocolActionTypeAllowed.TryGetValue((protocolId, actionId), out var allowed)
                    && allowed.Contains(type))
                {
                    return true;
                }

                if (ByProtocolActionTypeDenied.TryGetValue((protocolId, actionId), out var denied)
                    && denied.Contains(type))
                {
                    return false;
                }

                if (ByProtocolAction.TryGetValue((protocolId, actionId), out var authorised))
                {
                    return authorised;
                }

                return ByProtocol.TryGetValue(protocolId, out var byProtocol) ? byProtocol : (bool?)null;
            }
        }

        internal sealed class ByteArrayKey : IEquatable<ByteArrayKey>
        {
            internal readonly byte[] Data;
            private readonly int _hashCode;

            internal ByteArrayKey(byte[] data)
            {
                Data = data ?? Array.Empty<byte>();
                _hashCode = ComputeHash(Data);
            }

            public bool Equals(ByteArrayKey other)
            {
                if (ReferenceEquals(other, null))
                {
                    return false;
                }
                if (Data.Length != other.Data.Length)
                {
                    return false;
                }
                for (var i = 0; i < Data.Length; i++)
                {
                    if (Data[i] != other.Data[i])
                    {
                        return false;
                    }
                }
                return true;
            }

            public override bool Equals(object obj)
            {
                return obj is ByteArrayKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            private static int ComputeHash(byte[] bytes)
            {
                var h = 1;
                foreach (var b in bytes)
                {
                    h = 31 * h + b;
                }
                return h;
            }
        }
    }
}
