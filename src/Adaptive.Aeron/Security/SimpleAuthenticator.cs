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

// Direct port from io.aeron.security.SimpleAuthenticator. Internal field naming kept
// PascalCase for parity; line lengths kept to Java structure.
#pragma warning disable IDE1006, IDE0041, S103
namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// An authenticator that works off a simple principal/credential pair constructed by a builder.
    /// It only supports simple authentication, not challenge/response.
    /// </summary>
    public sealed class SimpleAuthenticator : IAuthenticator
    {
        private readonly Dictionary<Credentials, Principal> _principalsByCredentialsMap;
        private readonly Dictionary<long, Principal> _authenticatedSessionIdToPrincipalMap = new Dictionary<long, Principal>();

        private SimpleAuthenticator(Dictionary<Credentials, Principal> principalsByCredentials)
        {
            _principalsByCredentialsMap = principalsByCredentials;
        }

        public void OnConnectRequest(long sessionId, byte[] encodedCredentials, long nowMs)
        {
            if (_principalsByCredentialsMap.TryGetValue(new Credentials(encodedCredentials), out var principal)
                && principal.CredentialsMatch(encodedCredentials))
            {
                _authenticatedSessionIdToPrincipalMap[sessionId] = principal;
            }
        }

        public void OnChallengeResponse(long sessionId, byte[] encodedCredentials, long nowMs)
        {
            throw new NotSupportedException();
        }

        public void OnConnectedSession(ISessionProxy sessionProxy, long nowMs)
        {
            var sessionId = sessionProxy.SessionId();

            if (_authenticatedSessionIdToPrincipalMap.TryGetValue(sessionId, out var principal))
            {
                if (sessionProxy.Authenticate(principal.EncodedPrincipal))
                {
                    _authenticatedSessionIdToPrincipalMap.Remove(sessionId);
                }
            }
            else
            {
                sessionProxy.Reject();
            }
        }

        public void OnChallengedSession(ISessionProxy sessionProxy, long nowMs)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Builder to create instances of <see cref="SimpleAuthenticator"/>.
        /// </summary>
        public sealed class Builder
        {
            private readonly Dictionary<Credentials, Principal> _principalsByCredentials = new Dictionary<Credentials, Principal>();

            /// <summary>
            /// Add a principal/credentials pair to the list supported by this authenticator.
            /// <para>
            /// <see cref="SimpleAuthenticator"/> keys principals by credentials, so encoded credentials
            /// should include the encoded principal. The associated <see cref="ICredentialsSupplier"/> used
            /// on the client should encode credentials matching this form.
            /// </para>
            /// </summary>
            public Builder Principal(byte[] encodedPrincipal, byte[] encodedCredentials)
            {
                var principal = new Principal(encodedPrincipal, encodedCredentials);
                _principalsByCredentials[principal.Credentials] = principal;
                return this;
            }

            /// <summary>
            /// Construct a new instance of the <see cref="SimpleAuthenticator"/>.
            /// </summary>
            public SimpleAuthenticator NewInstance()
            {
                return new SimpleAuthenticator(new Dictionary<Credentials, Principal>(_principalsByCredentials));
            }
        }

        private sealed class Principal
        {
            internal readonly byte[] EncodedPrincipal;
            internal readonly Credentials Credentials;

            internal Principal(byte[] encodedPrincipal, byte[] encodedCredentials)
            {
                EncodedPrincipal = encodedPrincipal;
                Credentials = new Credentials(encodedCredentials);
            }

            internal bool CredentialsMatch(byte[] encodedCredentials)
            {
                return ByteArrayEquals(Credentials.EncodedCredentials, encodedCredentials);
            }
        }

        private sealed class Credentials : IEquatable<Credentials>
        {
            internal readonly byte[] EncodedCredentials;
            private readonly int _hashCode;

            internal Credentials(byte[] encodedCredentials)
            {
                EncodedCredentials = encodedCredentials;
                _hashCode = ComputeHash(encodedCredentials);
            }

            public bool Equals(Credentials other)
            {
                return !ReferenceEquals(other, null) && ByteArrayEquals(EncodedCredentials, other.EncodedCredentials);
            }

            public override bool Equals(object obj)
            {
                return obj is Credentials other && Equals(other);
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            private static int ComputeHash(byte[] bytes)
            {
                if (bytes == null)
                {
                    return 0;
                }
                int h = 1;
                foreach (var b in bytes)
                {
                    h = 31 * h + b;
                }
                return h;
            }
        }

        private static bool ByteArrayEquals(byte[] a, byte[] b)
        {
            if (ReferenceEquals(a, b))
            {
                return true;
            }
            if (a == null || b == null || a.Length != b.Length)
            {
                return false;
            }
            for (var i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                {
                    return false;
                }
            }
            return true;
        }
    }
}
