namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Representation for a session which is going through the authentication process.
    /// </summary>
    public interface ISessionProxy
    {
        /// <summary>
        /// The session Id of the potential session assigned by the system.
        /// </summary>
        /// <returns> session id for the potential session </returns>
        long SessionId();

        /// <summary>
        /// Inform the system that the session requires a challenge and to send the provided encoded challenge.
        /// </summary>
        /// <param name="encodedChallenge"> to send to the client. </param>
        /// <returns> true if challenge was sent or false if challenge could not be sent. </returns>
        bool Challenge(byte[] encodedChallenge);

        /// <summary>
        /// Inform the system that the session has met authentication requirements.
        /// </summary>
        /// <param name="encodedPrincipal"> that has passed authentication. </param>
        /// <returns> true if success event was sent or false if success event could not be sent. </returns>
        bool Authenticate(byte[] encodedPrincipal);

        /// <summary>
        /// Inform the system that the session has NOT met authentication requirements and should be rejected.
        /// </summary>
        void Reject();
    }
}