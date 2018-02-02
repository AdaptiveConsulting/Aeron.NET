using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Listens for events that can be bi-directional between the consensus module and services.
    /// <para>
    /// The relevant handlers can be implemented and others ignored with the default implementations.
    /// </para>
    /// </summary>
    public interface IServiceControlListener
    {
        /// <summary>
        /// Request from a service to schedule a timer.
        /// </summary>
        /// <param name="correlationId"> that must be unique across services for the timer. </param>
        /// <param name="deadline">      after which the timer will expire and then fire. </param>
        void OnScheduleTimer(long correlationId, long deadline);

        /// <summary>
        /// Request from a service to cancel a previously scheduled timer.
        /// </summary>
        /// <param name="correlationId"> of the previously scheduled timer. </param>
        void OnCancelTimer(long correlationId);

        /// <summary>
        /// Acknowledgement from a service that it has undertaken the a requested <seealso cref="ClusterAction"/>.
        /// </summary>
        /// <param name="logPosition">      of the service after undertaking the action. </param>
        /// <param name="leadershipTermId"> within which the action has taken place. </param>
        /// <param name="serviceId">        that has undertaken the action. </param>
        /// <param name="action">           undertaken. </param>
        void OnServiceAck(long logPosition, long leadershipTermId, int serviceId, ClusterAction action);

        /// <summary>
        /// Request that the services join to a log for replay or live stream.
        /// </summary>
        /// <param name="leadershipTermId"> for the log. </param>
        /// <param name="commitPositionId"> for counter that gives the bound for consumption of the log. </param>
        /// <param name="logSessionId">     for the log to confirm subscription. </param>
        /// <param name="logStreamId">      to subscribe to for the log. </param>
        /// <param name="logChannel">       to subscribe to for the log. </param>
        void OnJoinLog(long leadershipTermId, int commitPositionId, int logSessionId, int logStreamId, string logChannel);
    }
}