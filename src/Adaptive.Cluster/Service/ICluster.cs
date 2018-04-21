using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

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
        /// Get all <seealso cref="ClientSession"/>s.
        /// </summary>
        /// <returns> the <seealso cref="ClientSession"/>s. </returns>
        ICollection<ClientSession> GetClientSessions();
        
        /// <summary>
        /// Request the close of a <seealso cref="ClientSession"/> by sending the request to the consensus module.
        /// </summary>
        /// <param name="clusterSessionId"> to be closed. </param>
        /// <returns> true if the event to close a session was sent or false if back pressure was applied. </returns>
        /// <exception cref="ArgumentException"> if the clusterSessionId is not recognised. </exception>
        bool CloseSession(long clusterSessionId);

        /// <summary>
        /// Current Epoch time in milliseconds.
        /// </summary>
        /// <returns> Epoch time in milliseconds. </returns>
        long TimeMs();

        /// <summary>
        /// Schedule a timer for a given deadline and provide a correlation id to identify the timer when it expires or
        /// for cancellation.
        /// 
        /// If the correlationId is for an existing scheduled timer then it will be reschedule to the new deadline.
        ///    
        /// </summary>
        /// <param name="correlationId"> to identify the timer when it expires. </param>
        /// <param name="deadlineMs"> after which the timer will fire. </param>
        /// <returns> true if the event to schedule a timer has been sent or false if back pressure is applied. </returns>
        /// <see cref="CancelTimer(long)"/>
        bool ScheduleTimer(long correlationId, long deadlineMs);

        /// <summary>
        /// Cancel a previous scheduled timer.
        /// </summary>
        /// <param name="correlationId"> for the timer provided when it was scheduled. </param>
        /// <returns> true if the event to cancel a scheduled timer has been sent or false if back pressure is applied. </returns>
        /// <see cref="ScheduleTimer(long, long)"/>
        bool CancelTimer(long correlationId);

    }
}