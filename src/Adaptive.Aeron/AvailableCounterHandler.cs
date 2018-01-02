using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for notification of<seealso cref="Counter"/>s becoming available via a <seealso cref="Aeron"/> client.
    /// </summary>
    public delegate void AvailableCounterHandler(CountersReader countersReader, long registrationId, int counterId);
}