using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for notification of <seealso cref="Counter"/>s being removed via an <seealso cref="Aeron"/> client.
    /// </summary>
    public delegate void UnavailableCounterHandler(CountersReader countersReader, long registrationId, int counterId);
}