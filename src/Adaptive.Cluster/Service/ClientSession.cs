using System;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;

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

        private readonly ClusteredServiceAgent _cluster;
        private Publication _responsePublication;

        internal ClientSession(
            long sessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal,
            ClusteredServiceAgent cluster)
        {
            Id = sessionId;
            ResponseStreamId = responseStreamId;
            ResponseChannel = responseChannel;
            EncodedPrincipal = encodedPrincipal;
            _cluster = cluster;
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
            if (null != _cluster.GetClientSession(Id))
            {
                _cluster.CloseSession(Id);
            }
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message to a cluster.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <param name="offset"> offset in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int, ReservedValueSupplier)"/> when in <seealso cref="ClusterRole.Leader"/>
        /// otherwise <see cref="MOCKED_OFFER"/>. </returns>
        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            return _cluster.Offer(Id, _responsePublication, buffer, offset, length);
        }
        
        /// <summary>
        /// Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced cluster
        /// egress header so must be left unused.
        /// </summary>
        /// <param name="vectors"> which make up the message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/>. </returns>
        /// <seealso cref="Publication.Offer(DirectBufferVector[], ReservedValueSupplier)"/> when in <seealso cref="ClusterRole.Leader"/>
        /// otherwise <seealso cref="MOCKED_OFFER"/>.
        public long Offer(DirectBufferVector[] vectors)
        {
            return _cluster.Offer(Id, _responsePublication, vectors);
        }
        
        internal void Connect(Aeron.Aeron aeron)
        {
            if (null == _responsePublication)
            {
                try
                {
                    _responsePublication = aeron.AddExclusivePublication(ResponseChannel, ResponseStreamId);
                }
                catch (RegistrationException)
                {
                    // ignore
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

        internal void Disconnect()
        {
            _responsePublication?.Dispose();
            _responsePublication = null;
        }
    }
}