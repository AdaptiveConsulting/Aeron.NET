using System;
using System.Collections.Generic;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Interface for a <seealso cref="IClusteredService"/> to interact with cluster hosting it.
    /// <para>
    /// This object should only be used to send messages to the cluster or schedule timers in response to other messages
    /// and timers. Sending messages and timers should not happen from cluster lifecycle methods like
    /// <seealso cref="IClusteredService.OnStart(ICluster, Image)"/>, <seealso cref="IClusteredService.OnRoleChange(ClusterRole)"/> or
    /// <seealso cref="IClusteredService.OnTakeSnapshot(ExclusivePublication)"/>, or <seealso cref="IClusteredService.OnTerminate(ICluster)"/>,
    /// with the exception of the session lifecycle methods <seealso cref="IClusteredService.OnSessionOpen(IClientSession, long)"/> and
    /// <seealso cref="IClusteredService.OnSessionClose(IClientSession, long, CloseReason)"/> and <seealso cref="IClusteredService.OnNewLeadershipTermEvent"/>.
    /// </para>
    /// </summary>

    public interface ICluster
    {
        /// <summary>
        /// The unique id for the hosting member of the cluster. Useful only for debugging purposes.
        /// </summary>
        /// <returns> unique id for the hosting member of the cluster. </returns>
        int MemberId { get; }

        /// <summary>
        /// Position the log has reached in bytes as of the current message.
        /// </summary>
        /// <returns> position the log has reached in bytes as of the current message. </returns>
        long LogPosition();
        
        /// <summary>
        /// The role the cluster node is playing.
        /// </summary>
        /// <returns> the role the cluster node is playing. </returns>
        ClusterRole Role { get; }

        /// <summary>
        /// Get the <seealso cref="Aeron"/> client used by the cluster.
        /// </summary>
        /// <returns> the <seealso cref="Aeron"/> client used by the cluster. </returns>
        Aeron.Aeron Aeron { get; }

        /// <summary>
        /// Get the <seealso cref="ClusteredServiceContainer.Context"/> under which the container is running.
        /// </summary>
        /// <returns> the <seealso cref="ClusteredServiceContainer.Context"/> under which the container is running. </returns>
        ClusteredServiceContainer.Context Context { get; }

        /// <summary>
        /// Get the <seealso cref="IClientSession"/> for a given cluster session id.
        /// </summary>
        /// <param name="clusterSessionId"> to be looked up. </param>
        /// <returns> the <seealso cref="IClientSession"/> that matches the clusterSessionId. </returns>
        IClientSession GetClientSession(long clusterSessionId);

        /// <summary>
        /// Get all <seealso cref="IClientSession"/>s.
        /// </summary>
        /// <returns> the <seealso cref="IClientSession"/>s. </returns>
        ICollection<IClientSession> ClientSessions { get; }
        
        /// <summary>
        /// For each iterator over <seealso cref="IClientSession"/>s using the most efficient method possible.
        /// </summary>
        /// <param name="action"> to be taken for each <seealso cref="IClientSession"/> in turn. </param>
        void ForEachClientSession(Action<IClientSession> action);

        /// <summary>
        /// Request the close of a <seealso cref="IClientSession"/> by sending the request to the consensus module.
        /// </summary>
        /// <param name="clusterSessionId"> to be closed. </param>
        /// <returns> true if the event to close a session was sent or false if back pressure was applied. </returns>
        /// <exception cref="ClusterException"> if the clusterSessionId is not recognised. </exception>
        bool CloseClientSession(long clusterSessionId);

        /// <summary>
        /// Cluster time as <seealso cref="TimeUnit()"/>s since 1 Jan 1970 UTC.
        /// </summary>
        /// <returns> time as <seealso cref="TimeUnit()"/>s since 1 Jan 1970 UTC. </returns>

        long Time { get; }

        /// <summary>
        /// The unit of time applied when timestamping and <seealso cref="Time()"/> operations.
        /// </summary>
        /// <returns> the unit of time applied when timestamping and <seealso cref="Time()"/> operations. </returns>
        ClusterTimeUnit TimeUnit();

        /// <summary>
        /// Schedule a timer for a given deadline and provide a correlation id to identify the timer when it expires or
        /// for cancellation. This action is asynchronous and will race with the timer expiring.
        /// <para>
        /// If the correlationId is for an existing scheduled timer then it will be rescheduled to the new deadline. However
        /// it is best to generate correlationIds in a monotonic fashion and be aware of potential clashes with other
        /// services in the same cluster. Service isolation can be achieved by using the upper bits for service id.
        /// </para>
        /// <para>
        /// Timers should only be scheduled or cancelled in the context of processing a
        /// <seealso cref="IClusteredService.OnSessionMessage(IClientSession, long, IDirectBuffer, int, int, Header)"/>,
        /// <seealso cref="IClusteredService.OnTimerEvent(long, long)"/>,
        /// <seealso cref="IClusteredService.OnSessionOpen(IClientSession, long)"/>, or
        /// <seealso cref="IClusteredService.OnSessionClose(IClientSession, long, CloseReason)"/>.
        /// If applied to other events then they are not guaranteed to be reliable.
        ///   
        /// </para>
        /// </summary>
        /// <param name="correlationId"> to identify the timer when it expires. <seealso cref="long.MaxValue"/> not supported. </param>
        /// <param name="deadline">      time after which the timer will fire. <seealso cref="long.MaxValue"/> not supported. </param>
        /// <returns> true if the event to schedule a timer request has been sent or false if back pressure is applied. </returns>
        /// <seealso cref="CancelTimer(long)"/>
        bool ScheduleTimer(long correlationId, long deadline);

        /// <summary>
        /// Cancel a previously scheduled timer. This action is asynchronous and will race with the timer expiring.
        /// <para>
        /// Timers should only be scheduled or cancelled in the context of processing a
        /// <seealso cref="IClusteredService.OnSessionMessage(IClientSession, long, IDirectBuffer, int, int, Header)"/>,
        /// <seealso cref="IClusteredService.OnTimerEvent(long, long)"/>,
        /// <seealso cref="IClusteredService.OnSessionOpen(IClientSession, long)"/>, or
        /// <seealso cref="IClusteredService.OnSessionClose(IClientSession, long, CloseReason)"/>.
        /// If applied to other events then they are not guaranteed to be reliable.
        ///    
        /// </para>
        /// </summary>
        /// <param name="correlationId"> for the timer provided when it was scheduled. <seealso cref="long.MaxValue"/> not supported. </param>
        /// <returns> true if the event to cancel request has been sent or false if back-pressure is applied. </returns>
        /// <seealso cref="ScheduleTimer"/>
        bool CancelTimer(long correlationId);

        /// <summary>
        /// Offer a message as ingress to the cluster for sequencing. This will happen efficiently over IPC to the
        /// consensus module and have the cluster session of as the negative value of the
        /// <seealso cref="ClusteredServiceContainer.Configuration.SERVICE_ID_PROP_NAME"/>.
        /// </summary>
        /// <param name="buffer"> containing the message to be offered. </param>
        /// <param name="offset"> in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in the buffer of the encoded message. </param>
        /// <returns> positive value if successful. </returns>
        long Offer(IDirectBuffer buffer, int offset, int length);

        /// <summary>
        /// Offer a message as ingress to the cluster for sequencing. This will happen efficiently over IPC to the
        /// consensus module and have the cluster session of as the negative value of the
        /// <seealso cref="ClusteredServiceContainer.Configuration.SERVICE_ID_PROP_NAME"/>.
        /// <para>
        /// The first vector must be left free to be filled in for the session message header.
        /// 
        /// </para>
        /// </summary>
        /// <param name="vectors"> containing the message parts with the first left to be filled. </param>
        /// <returns> positive value if successful. </returns>
        long Offer(DirectBufferVector[] vectors);

        /// <summary>
        /// Try to claim a range in the publication log into which a message can be written with zero copy semantics.
        /// Once the message has been written then <seealso cref="BufferClaim.Commit()"/> should be called thus making it available.
        /// <para>
        /// On successful claim, the Cluster egress header will be written to the start of the claimed buffer section.
        /// Clients <b>MUST</b> write into the claimed buffer region at offset + <seealso cref="AeronCluster.SESSION_HEADER_LENGTH"/>.
        /// <pre>{@code
        ///     final IDirectBuffer srcBuffer = AcquireMessage();
        ///    
        ///     if (cluster.TryClaim(length, bufferClaim) > 0L)
        ///     {
        ///         try
        ///         {
        ///              final IMutableDirectBuffer buffer = bufferClaim.Buffer;
        ///              final int offset = bufferClaim.Offset;
        ///              // ensure that data is written at the correct offset
        ///              buffer.PutBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
        ///         }
        ///         finally
        ///         {
        ///             bufferClaim.Commit();
        ///         }
        ///     }
        /// }</pre>
        ///    
        /// </para>
        /// </summary>
        /// <param name="length">      of the range to claim, in bytes. </param>
        /// <param name="bufferClaim"> to be populated if the claim succeeds. </param>
        /// <returns> The new stream position, otherwise a negative error value as specified in
        ///         <seealso cref="Publication.TryClaim(int, BufferClaim)"/>. </returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="Publication.MaxPayloadLength"/>. </exception>
        /// <seealso cref="Publication.TryClaim(int, BufferClaim)"/>
        /// <seealso cref="BufferClaim.Commit()"/>
        /// <seealso cref="BufferClaim.Abort()"/>
        long TryClaim(int length, BufferClaim bufferClaim);

        /// <summary>
        /// <seealso cref="IdleStrategy"/> which should be used by the service when it experiences back-pressure on egress,
        /// closing sessions, making timer requests, or any long-running actions.
        /// </summary>
        /// <returns> the <seealso cref="IdleStrategy"/> which should be used by the service when it experiences back-pressure. </returns>
        IIdleStrategy IdleStrategy();
    }
}