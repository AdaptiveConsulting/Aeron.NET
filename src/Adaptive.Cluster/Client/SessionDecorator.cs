using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Encapsulate applying the cluster session header.
    /// <para>
    /// The session header is applied by a vectored offer to the <seealso cref="Publication"/>.
    /// </para>
    /// <para>
    /// <b>Note:</b> This class is NOT threadsafe. Each publisher thread requires its own instance.
    /// </para>
    /// </summary>
    public class SessionDecorator
    {
        /// <summary>
        /// Length of the session header that will be prepended to the message.
        /// </summary>
        public static readonly int SESSION_HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;

        private long lastCorrelationId;
        private readonly DirectBufferVector[] vectors = new DirectBufferVector[2];
        private readonly DirectBufferVector messageBuffer = new DirectBufferVector();
        private readonly SessionHeaderEncoder sessionHeaderEncoder = new SessionHeaderEncoder();

        /// <summary>
        /// Construct a new session header wrapper.
        /// </summary>
        /// <param name="clusterSessionId"> that has been allocated by the cluster. </param>
        /// <param name="lastCorrelationId"> the last correlation id that was sent to the cluster with this session.</param>
        public SessionDecorator(long clusterSessionId, long lastCorrelationId = Aeron.Aeron.NULL_VALUE)
        {
            UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
            sessionHeaderEncoder
                .WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder())
                .ClusterSessionId(clusterSessionId)
                .Timestamp(Aeron.Aeron.NULL_VALUE);

            vectors[0] = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
            vectors[1] = messageBuffer;
            
            this.lastCorrelationId = lastCorrelationId;
        }

        /// <summary>
        /// Reset the cluster session id in the header.
        /// </summary>
        /// <param name="clusterSessionId"> to be set in the header. </param>
        public void ClusterSessionId(long clusterSessionId)
        {
            sessionHeaderEncoder.ClusterSessionId(clusterSessionId);
        }

        /// <summary>
        /// Get the last correlation id generated for this session. Starts with <seealso cref="Aeron#NULL_VALUE"/>.
        /// </summary>
        /// <returns> the last correlation id generated for this session. </returns>
        /// <seealso cref="NextCorrelationId"/>
        public long LastCorrelationId()
        {
            return lastCorrelationId;
        }

        /// <summary>
        /// Generate a new correlation id to be used for this session. This is not threadsafe. If you require a threadsafe
        /// correlation id generation then use <seealso cref="Aeron#NextCorrelationId()"/>.
        /// </summary>
        /// <returns>  a new correlation id to be used for this session. </returns>
        /// <seealso cref="LastCorrelationId"/>
        public long NextCorrelationId()
        {
            return ++lastCorrelationId;
        }
        
        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
        /// <para>
        /// This version of the method will set the timestamp value in the header to zero.
        /// 
        /// </para>
        /// </summary>
        /// <param name="publication">   to be offer to. </param>
        /// <param name="correlationId"> to be used to identify the message to the cluster. </param>
        /// <param name="buffer">        containing message. </param>
        /// <param name="offset">        offset in the buffer at which the encoded message begins. </param>
        /// <param name="length">        in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(UnsafeBuffer, int, int)"/>. </returns>
        public long Offer(Publication publication, long correlationId, IDirectBuffer buffer, int offset, int length)
        {
            sessionHeaderEncoder.CorrelationId(correlationId);
            messageBuffer.Reset(buffer, offset, length);

            return publication.Offer(vectors, null);
        }
    }
}