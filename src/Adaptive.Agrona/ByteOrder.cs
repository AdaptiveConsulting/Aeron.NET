namespace Adaptive.Agrona
{
    public enum ByteOrder
    {
        /// <summary>
        /// Constant denoting big-endian byte order.  In this order, the bytes of a
        /// multibyte value are ordered from most significant to least significant.
        /// </summary>
        BigEndian,

        /// <summary>
        /// Constant denoting little-endian byte order.  In this order, the bytes of
        /// a multibyte value are ordered from least significant to most
        /// significant.
        /// </summary>
        LittleEndian
    }
}