namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Used to supply instances of <see cref="IAuthenticator"/>
    /// </summary>
    public interface IAuthenticatorSupplier
    {
        IAuthenticator Get();
    }
}