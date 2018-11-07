namespace Adaptive.Agrona.SBE
{
    /// <summary>
    /// Common behaviour to SBE Message encoder and decoder flyweights.
    /// </summary>
    public interface IMessageFlyweight : IFlyweight
    {
        /// <summary>
        /// The length of the root block in bytes.
        /// </summary>
        /// <returns> the length of the root block in bytes. </returns>
        int SbeBlockLength();

        /// <summary>
        /// The SBE template identifier for the message.
        /// </summary>
        /// <returns> the SBE template identifier for the message. </returns>
        int SbeTemplateId();

        /// <summary>
        /// The SBE Schema identifier containing the message declaration.
        /// </summary>
        /// <returns> the SBE Schema identifier containing the message declaration. </returns>
        int SbeSchemaId();

        /// <summary>
        /// The version number of the SBE Schema containing the message.
        /// </summary>
        /// <returns> the version number of the SBE Schema containing the message. </returns>
        int SbeSchemaVersion();

        /// <summary>
        /// The semantic type of the message which is typically the semantic equivalent in the FIX repository.
        /// </summary>
        /// <returns> the semantic type of the message which is typically the semantic equivalent in the FIX repository. </returns>
        string SbeSemanticType();

        /// <summary>
        /// The current offset in the buffer from which the message is being encoded or decoded.
        /// </summary>
        /// <returns> the current offset in the buffer from which the message is being encoded or decoded. </returns>
        int Offset();
    }
}