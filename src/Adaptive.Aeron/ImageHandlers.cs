namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for delivery of new image events to a <seealso cref="Aeron"/> instance.
    /// </summary>
    public delegate void AvailableImageHandler(Image image);

    /// <summary>
    /// Interface for delivery of inactive image events to a <seealso cref="Subscription"/>.
    /// </summary>
    public delegate void UnavailableImageHandler(Image image);
}