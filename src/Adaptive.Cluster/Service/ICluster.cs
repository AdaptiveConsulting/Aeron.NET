namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Interface for a <seealso cref="IClusteredService"/> to interact with cluster hosting it.
    /// </summary>
    public interface ICluster
    {
        /// <summary>
        /// The role the cluster node is playing.
        /// </summary>
        /// <returns> the role the cluster node is playing. </returns>
        ClusterRole Role();

        /// <summary>
        /// Is the node currently in replay of existing messages.
        /// </summary>
        /// <returns> true if the node currently in replay of existing messages otherwise false. </returns>
        bool IsReplay();

        /// <summary>
        /// Get the <seealso cref="Aeron"/> client used by the cluster.
        /// </summary>
        /// <returns> the <seealso cref="Aeron"/> client used by the cluster. </returns>
        Aeron.Aeron Aeron();

        /// <summary>
        /// Get the <seealso cref="ClientSession"/> for a given cluster session id.
        /// </summary>
        /// <param name="clusterSessionId"> to be looked up. </param>
        /// <returns> the <seealso cref="ClientSession"/> that matches the clusterSessionId. </returns>
        ClientSession GetClientSession(long clusterSessionId);

        /// <summary>
        /// Current Epoch time in milliseconds.
        /// </summary>
        /// <returns> Epoch time in milliseconds. </returns>
        long TimeMs();

        /// <summary>
        /// Schedule a timer for a given deadline and provide a correlation id to identify the timer when it expires.
        /// <para>
        /// If the correlationId is for an existing scheduled timer then it will be reschedule to the new deadline.
        /// 
        /// </para>
        /// </summary>
        /// <param name="correlationId"> to identify the timer when it expires. </param>
        /// <param name="deadlineMs"> after which the timer will fire. </param>
        void ScheduleTimer(long correlationId, long deadlineMs);

        /// <summary>
        /// Cancel a previous scheduled timer.
        /// </summary>
        /// <param name="correlationId"> for the scheduled timer. </param>
        void CancelTimer(long correlationId);
    }
}