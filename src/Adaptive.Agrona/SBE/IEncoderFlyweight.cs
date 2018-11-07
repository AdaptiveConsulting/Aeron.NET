namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// A flyweight for encoding an SBE type.
    /// </summary>
    public interface IEncoderFlyweight : IFlyweight
    {
        /// <summary>
        /// Wrap a buffer for encoding at a given offset.
        /// </summary>
        /// <param name="buffer"> to be wrapped and into which the type will be encoded. </param>
        /// <param name="offset"> at which the encoded object will be begin. </param>
        /// <returns> the <seealso cref="IEncoderFlyweight"/> for fluent API design. </returns>
        IEncoderFlyweight Wrap(IMutableDirectBuffer buffer, int offset);
    }
}