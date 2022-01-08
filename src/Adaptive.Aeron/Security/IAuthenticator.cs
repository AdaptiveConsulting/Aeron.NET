namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Interface for an Authenticator to handle authentication of clients to a system.
    /// <para>
    /// The session-id refers to the authentication session and not the Aeron transport session assigned to a publication.
    /// </para>
    ///
    /// <seealso cref="ISessionProxy"/>
    /// <see cref="IAuthenticatorSupplier"/>
    /// </summary>
    public interface IAuthenticator
    {
        /// <summary>
        /// Called upon reception of a Connect Request and will be followed up by multiple calls to <seealso cref="OnConnectedSession"/>
        /// one the response channel is connected.
        /// </summary>
        /// <param name="sessionId">          to identify the client session connecting. </param>
        /// <param name="encodedCredentials"> from the Connect Request. Will not be null, but may be 0 length. </param>
        /// <param name="nowMs">              current epoch time in milliseconds. </param>
        void OnConnectRequest(long sessionId, byte[] encodedCredentials, long nowMs);

        /// <summary>
        /// Called upon reception of a Challenge Response from an unauthenticated client.
        /// </summary>
        /// <param name="sessionId">          to identify the client session connecting. </param>
        /// <param name="encodedCredentials"> from the Challenge Response. Will not be null, but may be 0 length. </param>
        /// <param name="nowMs">              current epoch time in milliseconds. </param>
        void OnChallengeResponse(long sessionId, byte[] encodedCredentials, long nowMs);

        /// <summary>
        /// Called when a client's response channel has been connected. This method may be called multiple times until the
        /// session timeouts, is challenged, authenticated, or rejected.
        /// </summary>
        /// <param name="sessionProxy"> to use to update authentication status. Proxy is only valid for the duration of the call. </param>
        /// <param name="nowMs">        current epoch time in milliseconds. </param>
        /// <seealso cref="ISessionProxy"/>
        void OnConnectedSession(ISessionProxy sessionProxy, long nowMs);

        /// <summary>
        /// Called when a challenged client should be able to accept a response from the authenticator.
        /// <para>
        /// When this is called, there is no assumption that a Challenge Response has been received, plus this method
        /// may be called multiple times.
        /// </para>
        /// <para>
        /// It is up to the concrete class to provide any timeout management.
        /// 
        /// </para>
        /// </summary>
        /// <param name="sessionProxy"> to use to update authentication status. Proxy is only valid for the duration of the call. </param>
        /// <param name="nowMs">        current epoch time in milliseconds. </param>
        /// <seealso cref="ISessionProxy"/>
        void OnChallengedSession(ISessionProxy sessionProxy, long nowMs);
    }
}