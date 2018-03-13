namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Supplier of credentials for authentication with a cluster leader.
    /// 
    /// Implement this interface to supply credentials for clients. If no credentials are required then the
    /// <seealso cref="NullCredentialsSupplier"/> can be used.
    /// </summary>
    public interface ICredentialsSupplier
    {
        /// <summary>
        /// Provide a credential to be included in Session Connect message to the cluster.
        /// </summary>
        /// <returns> a credential in binary form to be included in the Session Connect message to the cluster. </returns>
        byte[] EncodedCredentials();

        /// <summary>
        /// Given some encoded challenge data, provide the credentials to be included in a Challenge Response as part of
        /// authentication with a cluster.
        /// </summary>
        /// <param name="endcodedChallenge"> from the cluster to use in providing a credential. </param>
        /// <returns> encoded credentials in binary form to be included in the Challenge Response to the cluster. </returns>
        byte[] OnChallenge(byte[] endcodedChallenge);
    }
}