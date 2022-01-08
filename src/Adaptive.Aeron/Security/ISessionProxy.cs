namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Representation of a session during the authentication process from the perspective of an <seealso cref="IAuthenticator"/>.
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
        /// <returns> true if challenge was accepted to be sent at present time or false if it will be retried later. </returns>
        bool Challenge(byte[] encodedChallenge);

        /// <summary>
        /// Inform the system that the session has met authentication requirements.
        /// </summary>
        /// <param name="encodedPrincipal"> that has passed authentication. </param>
        /// <returns> true if authentication was accepted at present time or false if it will be retried later. </returns>
        bool Authenticate(byte[] encodedPrincipal);

        /// <summary>
        /// Inform the system that the session should be rejected.
        /// </summary>
        void Reject();
    }
}