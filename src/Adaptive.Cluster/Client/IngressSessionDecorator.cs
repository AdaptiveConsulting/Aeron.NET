using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Codecs;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Encapsulate applying a client message header for ingress to the cluster.
    /// 
    /// The client message header is applied by a vectored offer to the <seealso cref="Publication"/>.
    /// 
    /// <b>Note:</b> This class is NOT threadsafe. Each publisher thread requires its own instance.
    /// 
    /// </summary>
    public class IngressSessionDecorator
    {
        /// <summary>
        /// Length of the session header that will be prepended to the message.
        ///
        /// The client message header is applied to the <see cref="Publication"/> before the offered buffer.
        ///
        /// Note: This class is NOT threadsafe for updating <see cref="ClusterSessionId"/> or
        /// <see cref="LeadershipTermId"/>. Each publisher thread requires its own instance.
        /// 
        /// </summary>
        public static readonly int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + SessionMessageHeaderEncoder.BLOCK_LENGTH;

        private readonly SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private readonly UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[HEADER_LENGTH]);

        /// <summary>
        /// Construct a new ingress session header wrapper that defaults all fields to the <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>
        /// </summary>
        public IngressSessionDecorator() : this(Aeron.Aeron.NULL_VALUE, Aeron.Aeron.NULL_VALUE)
        {
        }

        /// <summary>
        /// Construct a new session header wrapper.
        /// </summary>
        /// <param name="clusterSessionId"> that has been allocated by the cluster. </param>
        /// <param name="leadershipTermId"> of the current leader.</param>
        public IngressSessionDecorator(long clusterSessionId, long leadershipTermId)
        {
            sessionMessageHeaderEncoder
                .WrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder())
                .LeadershipTermId(leadershipTermId)
                .ClusterSessionId(clusterSessionId)
                .Timestamp(Aeron.Aeron.NULL_VALUE);
        }

        /// <summary>
        /// Reset the cluster session id in the header.
        /// </summary>
        /// <param name="clusterSessionId"> to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        public IngressSessionDecorator ClusterSessionId(long clusterSessionId)
        {
            sessionMessageHeaderEncoder.ClusterSessionId(clusterSessionId);
            return this;
        }

        /// <summary>
        /// Reset the leadership term id in the header.
        /// </summary>
        /// <param name="leadershipTermId"> to be set in the header. </param>
        /// <returns> this for a fluent API. </returns>
        public IngressSessionDecorator LeadershipTermId(long leadershipTermId)
        {
            sessionMessageHeaderEncoder.LeadershipTermId(leadershipTermId);
            return this;
        }

        /// <summary>
        /// Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
        /// <para>
        /// This version of the method will set the timestamp value in the header to <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="publication">   to be offer to. </param>
        /// <param name="buffer">        containing message. </param>
        /// <param name="offset">        offset in the buffer at which the encoded message begins. </param>
        /// <param name="length">        in bytes of the encoded message. </param>
        /// <returns> the same as <seealso cref="Publication.Offer(IDirectBuffer, int, int, ReservedValueSupplier)"/>. </returns>
        public long Offer(Publication publication, IDirectBuffer buffer, int offset, int length)
        {
            return publication.Offer(headerBuffer, 0, HEADER_LENGTH, buffer, offset, length);
        }
    }
}