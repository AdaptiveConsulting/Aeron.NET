using System;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Session representing a connected client to the cluster.
    /// </summary>
    public class ClientSession
    {
        /// <summary>
        /// Length of the session header that will be prepended to the message.
        /// </summary>
        public static readonly int SESSION_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;

        private readonly long _id;
        private readonly int _responseStreamId;
        private readonly string _responseChannel;
        private Publication _responsePublication;
        private readonly byte[] _principalData;
        private readonly DirectBufferVector[] _vectors = new DirectBufferVector[2];
        private readonly DirectBufferVector _messageBuffer = new DirectBufferVector();
        private readonly SessionHeaderEncoder _sessionHeaderEncoder = new SessionHeaderEncoder();
        private readonly ICluster _cluster;

        internal ClientSession(long sessionId, int responseStreamId, string responseChannel, byte[] principalData, ICluster cluster)
        {
            _id = sessionId;
            _responseStreamId = responseStreamId;
            _responseChannel = responseChannel;
            _principalData = principalData;
            _cluster = cluster;

            UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
            _sessionHeaderEncoder.WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder()).ClusterSessionId(sessionId);

            _vectors[0] = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
            _vectors[1] = _messageBuffer;
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
        /// Cluster session principal data passed from <seealso cref="io.aeron.cluster.Authenticator"/>
        /// when the session was authenticated.
        /// </summary>
        /// <returns> The Principal data passed. May be 0 length to indicate no data. </returns>
        public byte[] PrincipalData()
        {
            return _principalData;
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message to a cluster.
        /// </summary>
        /// <param name="correlationId"> to be used to identify the message to the cluster. </param>
        /// <param name="buffer">        containing message. </param>
        /// <param name="offset">        offset in the buffer at which the encoded message begins. </param>
        /// <param name="length">        in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication#offer(DirectBuffer, int, int)"/> when in <seealso cref="Cluster.Role#LEADER"/>
        /// otherwise 1. </returns>
        public long Offer(long correlationId, IDirectBuffer buffer, int offset, int length)
        {
            if (_cluster.Role() != ClusterRole.Leader)
            {
                return 1;
            }

            _sessionHeaderEncoder.CorrelationId(correlationId);
            _sessionHeaderEncoder.Timestamp(_cluster.TimeMs());
            _messageBuffer.Reset(buffer, offset, length);

            return _responsePublication.Offer(_vectors);
        }

        internal void Connect(Aeron.Aeron aeron)
        {
            if (null != _responsePublication)
            {
                throw new InvalidOperationException("Response publication already present");
            }

            _responsePublication = aeron.AddExclusivePublication(_responseChannel, _responseStreamId);
        }

        internal void Disconnect()
        {
            _responsePublication?.Dispose();
            _responsePublication = null;
        }
    }
}