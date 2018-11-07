﻿using Adaptive.Aeron;
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

        internal void Connect(Aeron.Aeron aeron)
        {
            if (null == _responsePublication)
            {
                _responsePublication = aeron.AddExclusivePublication(ResponseChannel, ResponseStreamId);
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