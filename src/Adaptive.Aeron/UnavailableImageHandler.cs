namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for delivery of inactive image notification to a <seealso cref="Subscription"/>.
    /// 
    /// Method called by Aeron to deliver notification that an <see cref="Image"/> is no longer available for polling.
    ///
    /// Within this callback reentrant calls to the <see cref="Aeron"/> client are not permitted and
    /// will result in undefined behaviour.
    /// </summary>
    /// <param name="image"> that is no longer available for polling.</param>
    public delegate void UnavailableImageHandler(Image image);
}