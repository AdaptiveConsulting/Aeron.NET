namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Null provider of credentials when no authentication is required.
    /// </summary>
    public class NullCredentialsSupplier : ICredentialsSupplier
    {
        public static readonly byte[] NULL_CREDENTIAL = new byte[0];

        public byte[] ConnectRequestCredentialData()
        {
            return NULL_CREDENTIAL;
        }

        public byte[] OnChallenge(byte[] challengeData)
        {
            return NULL_CREDENTIAL;
        }
    }
}