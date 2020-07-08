using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for notification of<seealso cref="Counter"/>s becoming available via a <seealso cref="Aeron"/> client.
    ///
    /// Method called by Aeron to deliver notification of a {@link Counter} being available.
    /// 
    /// Within this callback reentrant calls to the <see cref="Aeron"/> client are not permitted and
    /// will result in undefined behaviour.
    /// 
    /// </summary>
    /// <param name="countersReader"> for more detail on the counter. </param>
    /// <param name="registrationId"> for the counter. </param>
    /// <param name="counterId">      that is available. </param>
    public delegate void AvailableCounterHandler(CountersReader countersReader, long registrationId, int counterId);
}