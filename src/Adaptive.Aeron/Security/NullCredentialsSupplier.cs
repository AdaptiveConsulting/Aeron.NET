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
        public static readonly byte[] NULL_CREDENTIAL = new byte[0];


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