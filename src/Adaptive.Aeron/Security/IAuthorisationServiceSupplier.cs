namespace Adaptive.Aeron.Security
{
    /// <summary>
    /// Used to supply instances of <seealso cref="IAuthorisationService"/>.
    /// </summary>
    public interface IAuthorisationServiceSupplier
    {
        IAuthorisationService Get();
    }
}