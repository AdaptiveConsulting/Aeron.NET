using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for encapsulating the strategy of mapping <seealso cref="LogBuffers"/> at a giving file location.
    /// </summary>
    internal interface ILogBuffersFactory
    {
        /// <summary>
        /// Map a log file into memory and wrap each section with a <seealso cref="UnsafeBuffer"/>.
        /// </summary>
        /// <param name="logFileName"> to be mapped into memory. </param>
        /// <returns> a representation of the mapped log buffer. </returns>
        LogBuffers Map(string logFileName);
    }
}