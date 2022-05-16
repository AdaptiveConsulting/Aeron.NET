namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Interface for an authorisation service to handle authorisation checks of clients performing actions on a system.
    /// </summary>
    /// <seealso cref="IAuthorisationServiceSupplier"/>
    public interface IAuthorisationService
    {
        /// <summary>
        /// Checks if the client with authenticated credentials is allowed to perform an operation indicated by the
        /// given {@code actionId}.
        /// </summary>
        /// <param name="protocolId">       of the protocol to which the action belongs, e.g. a SBE schema id. </param>
        /// <param name="actionId">         of the command being checked, e.g. a SBE message template id. </param>
        /// <param name="type">             optional type for the command being checked, may be {@code null}. For example for
        ///                         an admin request in the cluster it will contain {@code AdminRequestType} value which
        ///                         denotes the exact kind of the request. </param>
        /// <param name="encodedPrincipal"> that has been authenticated. </param>
        /// <returns> {@code true} if the client is authorised to execute the action or {@code false} otherwise. </returns>
        bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal);
    }

    /// <summary>
    /// An <seealso cref="IAuthorisationService"/> instance that allows every action.
    /// </summary>
    public class AllowAllAuthorisationService : IAuthorisationService
    {
        public static IAuthorisationService INSTANCE = new AllowAllAuthorisationService();
        
        public bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal)
        {
            return true;
        }
    }
    
    /// <summary>
    /// An <seealso cref="IAuthorisationService"/> instance that forbids all actions.
    /// </summary>
    public class DenyAllAuthorisationService : IAuthorisationService
    {
        public static IAuthorisationService INSTANCE = new DenyAllAuthorisationService();
        
        public bool IsAuthorised(int protocolId, int actionId, object type, byte[] encodedPrincipal)
        {
            return false;
        }
    }
}