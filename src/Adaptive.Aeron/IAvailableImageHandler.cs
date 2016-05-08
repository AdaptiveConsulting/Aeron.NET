namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for delivery of new image events to a <seealso cref="Aeron"/> instance.
    /// </summary>
    public interface IAvailableImageHandler
    {
        /// <summary>
        /// Method called by Aeron to deliver notification of a new <seealso cref="Image"/> being available for polling.
        /// </summary>
        /// <param name="image"> that is now available </param>
        void OnAvailableImage(Image image);
    }
}