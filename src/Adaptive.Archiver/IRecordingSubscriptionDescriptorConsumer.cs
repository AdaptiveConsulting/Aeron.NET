using Adaptive.Aeron;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Consumer for descriptors of active archive recording <seealso cref="Subscription"/>s.
    /// </summary>
    public interface IRecordingSubscriptionDescriptorConsumer
    {
        /// <summary>
        /// Descriptor for an active recording subscription on the archive.
        /// </summary>
        /// <param name="controlSessionId"> for the request. </param>
        /// <param name="correlationId">    for the request. </param>
        /// <param name="subscriptionId">   that can be used to stop the recording subscription. </param>
        /// <param name="streamId">         the subscription was registered with. </param>
        /// <param name="strippedChannel">  the subscription was registered with. </param>
        void OnSubscriptionDescriptor(
            long controlSessionId,
            long correlationId,
            long subscriptionId,
            int streamId,
            string strippedChannel);
    }

}