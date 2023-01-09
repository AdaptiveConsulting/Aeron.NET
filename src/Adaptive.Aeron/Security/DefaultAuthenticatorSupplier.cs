namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Default Authenticator which authenticates all connection requests immediately.
    /// </summary>
    public class DefaultAuthenticatorSupplier : IAuthenticatorSupplier
    {
        /// <summary>
        /// Singleton instance.
        /// </summary>
        public static readonly DefaultAuthenticatorSupplier INSTANCE = new DefaultAuthenticatorSupplier();
        
        /// <summary>
        /// The null encoded principal is an empty array of bytes.
        /// </summary>
        public static readonly byte[] NULL_ENCODED_PRINCIPAL = new byte[0];

        /// <summary>
        /// Singleton instance which can be used to avoid allocation.
        /// </summary>
        public static readonly IAuthenticator DEFAULT_AUTHENTICATOR = new DefaultAuthenticator();

        /// <summary>
        /// Gets the singleton instance <seealso cref="DEFAULT_AUTHENTICATOR"/> which authenticates all connection requests
        /// immediately.
        /// </summary>
        /// <returns> <seealso cref="DEFAULT_AUTHENTICATOR"/> which authenticates all connection requests immediately. </returns>
        public IAuthenticator Get()
        {
            return DEFAULT_AUTHENTICATOR;
        }

        sealed class DefaultAuthenticator : IAuthenticator
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