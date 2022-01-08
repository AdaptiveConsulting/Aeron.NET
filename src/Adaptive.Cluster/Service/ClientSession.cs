using System;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Session representing a connected client to the cluster.
    /// </summary>
    public interface IClientSession
    {
        /// <summary>
        /// Cluster session identity uniquely allocated when the session was opened.
        /// </summary>
        /// <returns> the cluster session identity uniquely allocated when the session was opened. </returns>
        long Id { get; }

        /// <summary>
        /// The response channel stream id for responding to the client.
        /// </summary>
        /// <returns> response channel stream id for responding to the client. </returns>
        int ResponseStreamId { get; }

        /// <summary>
        /// The response channel for responding to the client.
        /// </summary>
        /// <returns> response channel for responding to the client. </returns>
        string ResponseChannel { get; }

        /// <summary>
        /// Cluster session encoded principal from when the session was authenticated.
        /// </summary>
        /// <returns> The encoded Principal passed. May be 0 length to indicate none present. </returns>
        byte[] EncodedPrincipal { get; }

        /// <summary>
        /// Close of this <seealso cref="IClientSession"/> by sending the request to the consensus module.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
        void Close();

        /// <summary>
        /// Indicates that a request to close this session has been made.
        /// </summary>
        /// <returns> whether a request to close this session has been made. </returns>
        bool IsClosing { get; }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message to a cluster.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <param name="offset"> offset in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int, ReservedValueSupplier)"/> when in <seealso cref="ClusterRole.Leader"/>,
        /// otherwise <see cref="ClientSessionConstants.MOCKED_OFFER"/> when a follower. </returns>
        long Offer(IDirectBuffer buffer, int offset, int length);

        /// <summary>
        /// Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced by the cluster
        /// egress header so must be left unused.
        /// </summary>
        /// <param name="vectors"> which make up the message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/>. </returns>
        /// <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/> when in <seealso cref="ClusterRole.Leader"/>,
        /// otherwise <seealso cref="ClientSessionConstants.MOCKED_OFFER"/> when a follower.
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
        ///     if (clientSession.TryClaim(length, bufferClaim) > 0L)
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
        /// <param name="length">      of the range to claim in bytes. The additional bytes for the session header will be added. </param>
        /// <param name="bufferClaim"> to be populated if the claim succeeds. </param>
        /// <returns> The new stream position, otherwise a negative error value as specified in
        ///         <seealso cref="Publication.TryClaim(int, BufferClaim)"/>.when in <seealso cref="ClusterRole.Leader"/>,
        ///         otherwise <seealso cref="ClientSessionConstants.MOCKED_OFFER"/> when a follower.</returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="Publication.MaxPayloadLength"/>. </exception>
        /// <seealso cref="Publication.TryClaim(int, BufferClaim)"/>
        /// <seealso cref="BufferClaim.Commit()"/>
        /// <seealso cref="BufferClaim.Abort()"/>
        long TryClaim(int length, BufferClaim bufferClaim);
    }
}