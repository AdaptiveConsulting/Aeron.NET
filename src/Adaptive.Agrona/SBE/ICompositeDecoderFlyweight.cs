namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// A flyweight for decoding an SBE Composite type.
    /// </summary>
    public interface ICompositeDecoderFlyweight : IDecoderFlyweight
    {
        /// <summary>
        /// Wrap a buffer for decoding at a given offset.
        /// </summary>
        /// <param name="buffer"> containing the encoded SBE Composite type. </param>
        /// <param name="offset"> at which the encoded SBE Composite type begins. </param>
        /// <returns> the <seealso cref="ICompositeDecoderFlyweight"/> for fluent API design. </returns>
        ICompositeDecoderFlyweight Wrap(IDirectBuffer buffer, int offset);
    }
}
