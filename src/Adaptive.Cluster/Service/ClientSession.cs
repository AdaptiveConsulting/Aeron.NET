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
    public class ClientSession
    {
        /// <summary>
        /// Return value to indicate egress to a session is mocked out by the cluster when in follower mode.
        /// </summary>
        public const long MOCKED_OFFER = 1;

        private readonly ClusteredServiceAgent _clusteredServiceAgent;
        private Publication _responsePublication;

        internal ClientSession(
            long sessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal,
            ClusteredServiceAgent clusteredServiceAgent)
        {
            Id = sessionId;
            ResponseStreamId = responseStreamId;
            ResponseChannel = responseChannel;
            EncodedPrincipal = encodedPrincipal;
            _clusteredServiceAgent = clusteredServiceAgent;
        }

        /// <summary>
        /// Cluster session identity uniquely allocated when the session was opened.
        /// </summary>
        /// <returns> the cluster session identity uniquely allocated when the session was opened. </returns>
        public long Id { get; }

        /// <summary>
        /// The response channel stream id for responding to the client.
        /// </summary>
        /// <returns> response channel stream id for responding to the client. </returns>
        public int ResponseStreamId { get; }

        /// <summary>
        /// The response channel for responding to the client.
        /// </summary>
        /// <returns> response channel for responding to the client. </returns>
        public string ResponseChannel { get; }

        /// <summary>
        /// Cluster session encoded principal from when the session was authenticated.
        /// </summary>
        /// <returns> The encoded Principal passed. May be 0 length to indicate none present. </returns>
        public byte[] EncodedPrincipal { get; }

        /// <summary>
        /// Indicates that a request to close this session has been made.
        /// </summary>
        /// <returns> whether a request to close this session has been made. </returns>
        public bool IsClosing { get; private set; }


        /// <summary>
        /// Close of this <seealso cref="ClientSession"/> by sending the request to the consensus module.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
        public void Close()
        {
            if (null != _clusteredServiceAgent.GetClientSession(Id))
            {
                _clusteredServiceAgent.CloseClientSession(Id);
            }
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message to a cluster.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <param name="offset"> offset in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int, ReservedValueSupplier)"/> when in <seealso cref="ClusterRole.Leader"/>,
        /// otherwise <see cref="MOCKED_OFFER"/> when a follower. </returns>
        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            return _clusteredServiceAgent.Offer(Id, _responsePublication, buffer, offset, length);
        }
        
        /// <summary>
        /// Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced by the cluster
        /// egress header so must be left unused.
        /// </summary>
        /// <param name="vectors"> which make up the message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/>. </returns>
        /// <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/> when in <seealso cref="ClusterRole.Leader"/>,
        /// otherwise <seealso cref="MOCKED_OFFER"/> when a follower.
        public long Offer(DirectBufferVector[] vectors)
        {
            return _clusteredServiceAgent.Offer(Id, _responsePublication, vectors);
        }
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
        ///         otherwise <seealso cref="MOCKED_OFFER"/> when a follower.</returns>
        /// <exception cref="ArgumentException"> if the length is greater than <seealso cref="Publication.MaxPayloadLength"/>. </exception>
        /// <seealso cref="Publication.TryClaim(int, BufferClaim)"/>
        /// <seealso cref="BufferClaim.Commit()"/>
        /// <seealso cref="BufferClaim.Abort()"/>
        public long TryClaim(int length, BufferClaim bufferClaim)
        {
            return _clusteredServiceAgent.TryClaim(Id, _responsePublication, length, bufferClaim);
        }
        
        internal void Connect(Aeron.Aeron aeron)
        {
            if (null == _responsePublication)
            {
                try
                {
                    _responsePublication = aeron.AddPublication(ResponseChannel, ResponseStreamId);
                }
                catch (RegistrationException ex)
                {
                    _clusteredServiceAgent.HandleError(ex);
                }
            }
        }

        internal void MarkClosing()
        {
            IsClosing = true;
        }

        internal void ResetClosing()
        {
            IsClosing = false;
        }

        internal void Disconnect(ErrorHandler errorHandler)
        {
            CloseHelper.Dispose(errorHandler, _responsePublication);
            _responsePublication = null;
        }

        public override string ToString()
        {
            return "ClientSession{" +
                   "id=" + Id +
                   ", responseStreamId=" + ResponseStreamId +
                   ", responseChannel='" + ResponseChannel + '\'' +
                   ", encodedPrincipal=" + EncodedPrincipal +
                   ", clusteredServiceAgent=" + _clusteredServiceAgent +
                   ", responsePublication=" + _responsePublication +
                   ", isClosing=" + IsClosing +
                   '}';
        }
    }
}