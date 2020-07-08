namespace Adaptive.Archiver
{
    /// <summary>
    /// Interface for listening to events from the archive in response to requests.
    /// </summary>
    public interface IControlResponseListener : IRecordingDescriptorConsumer, IControlEventListener
    {
    }
}