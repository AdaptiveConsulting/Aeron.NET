namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for delivery of inactive image events to a <seealso cref="Subscription"/>.
    /// </summary>
    public interface IUnavailableImageHandler
    {
        /// <summary>
        /// Method called by Aeron to deliver notification that an Image is no longer available for polling.
        /// </summary>
        /// <param name="image"> the image that has become unavailable. </param>
        void OnUnavailableImage(Image image);
    }
}