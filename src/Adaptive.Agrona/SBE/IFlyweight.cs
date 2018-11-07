namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// An SBE (Simple Binary Encoding) flyweight object.
    /// </summary>
    public interface IFlyweight
    {
        /// <summary>
        /// The length of the encoded type in bytes.
        /// </summary>
        /// <returns> the length of the encoded type in bytes. </returns>
        int EncodedLength();
    }
}