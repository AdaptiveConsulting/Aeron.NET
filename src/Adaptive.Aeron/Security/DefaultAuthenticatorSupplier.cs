using System;

namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Default Authenticator that authenticates all connection requests immediately.
    /// </summary>
    public class DefaultAuthenticatorSupplier : IAuthenticatorSupplier
    {
        public static readonly byte[] NULL_ENCODED_PRINCIPAL = new byte[0];
        public static readonly IAuthenticator DEFAULT_AUTHENTICATOR = new DefaultAuthenticator();

        public IAuthenticator Get()
        {
            return DEFAULT_AUTHENTICATOR;
        }

        private class DefaultAuthenticator : IAuthenticator
        {
            public void OnConnectRequest(long sessionId, byte[] encodedCredentials, long nowMs)
            {
            }

            public void OnChallengeResponse(long sessionId, byte[] encodedCredentials, long nowMs)
            {
            }

            public void OnConnectedSession(ISessionProxy sessionProxy, long nowMs)
            {
                sessionProxy.Authenticate(NULL_ENCODED_PRINCIPAL);
            }

            public void OnChallengedSession(ISessionProxy sessionProxy, long nowMs)
            {
                sessionProxy.Authenticate(NULL_ENCODED_PRINCIPAL);
            }
        }
    }
}