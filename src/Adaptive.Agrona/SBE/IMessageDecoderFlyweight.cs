namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// A flyweight for decoding an SBE message from a buffer.
    /// </summary>
    public interface IMessageDecoderFlyweight : IMessageFlyweight, IDecoderFlyweight
    {
        /// <summary>
        /// Wrap a buffer containing an encoded message for decoding.
        /// </summary>
        /// <param name="buffer">            containing the encoded message. </param>
        /// <param name="offset">            in the buffer at which the decoding should begin. </param>
        /// <param name="actingBlockLength"> the root block length the decoder should act on. </param>
        /// <param name="actingVersion">     the version of the encoded message. </param>
        /// <returns> the <seealso cref="IMessageDecoderFlyweight"/> for fluent API design. </returns>
        IMessageDecoderFlyweight Wrap(IDirectBuffer buffer, int offset, int actingBlockLength, int actingVersion);
    }
}