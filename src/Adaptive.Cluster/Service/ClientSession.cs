using Adaptive.Aeron;
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

        private readonly long _id;
        private readonly int _responseStreamId;
        private readonly string _responseChannel;
        private readonly byte[] _encodedPrincipal;

        private readonly ClusteredServiceAgent _cluster;
        private Publication _responsePublication;
        private bool _isClosing;

        internal ClientSession(
            long sessionId,
            int responseStreamId,
            string responseChannel,
            byte[] encodedPrincipal,
            ClusteredServiceAgent cluster)
        {
            _id = sessionId;
            _responseStreamId = responseStreamId;
            _responseChannel = responseChannel;
            _encodedPrincipal = encodedPrincipal;
            _cluster = cluster;
        }

        /// <summary>
        /// Cluster session identity uniquely allocated when the session was opened.
        /// </summary>
        /// <returns> the cluster session identity uniquely allocated when the session was opened. </returns>
        public long Id()
        {
            return _id;
        }

        /// <summary>
        /// The response channel stream id for responding to the client.
        /// </summary>
        /// <returns> response channel stream id for responding to the client. </returns>
        public int ResponseStreamId()
        {
            return _responseStreamId;
        }

        /// <summary>
        /// The response channel for responding to the client.
        /// </summary>
        /// <returns> response channel for responding to the client. </returns>
        public string ResponseChannel()
        {
            return _responseChannel;
        }

        /// <summary>
        /// Cluster session encoded principal from when the session was authenticated.
        /// </summary>
        /// <returns> The encoded Principal passed. May be 0 length to indicate none present. </returns>
        public byte[] EncodedPrincipal()
        {
            return _encodedPrincipal;
        }

        /// <summary>
        /// Indicates that a request to close this session has been made.
        /// </summary>
        /// <returns> whether a request to close this session has been made. </returns>
        public bool IsClosing => _isClosing;

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message to a cluster.
        /// </summary>
        /// <param name="buffer"> containing message. </param>
        /// <param name="offset"> offset in the buffer at which the encoded message begins. </param>
        /// <param name="length"> in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int)"/> when in <seealso cref="ClusterRole.Leader"/>
        /// otherwise <see cref="MOCKED_OFFER"/>. </returns>
        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            return _cluster.Offer(_id, _responsePublication, buffer, offset, length);
        }

        internal void Connect(Aeron.Aeron aeron)
        {
            if (null == _responsePublication)
            {
                _responsePublication = aeron.AddExclusivePublication(_responseChannel, _responseStreamId);
            }
        }

        internal void MarkClosing()
        {
            _isClosing = true;
        }

        internal void ResetClosing()
        {
            _isClosing = false;
        }

        internal void Disconnect()
        {
            _responsePublication?.Dispose();
            _responsePublication = null;
        }
    }
}