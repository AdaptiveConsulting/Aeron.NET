using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Interface for encapsulating the strategy of mapping <seealso cref="LogBuffers"/> at a giving file location.
    /// </summary>
    public interface ILogBuffersFactory
    {
        /// <summary>
        /// Map a log file into memory and wrap each section with a <seealso cref="UnsafeBuffer"/>.
        /// </summary>
        /// <param name="logFileName"> to be mapped into memory. </param>
        /// <param name="mapMode"> the mode to be used for the file.</param>
        /// <returns> a representation of the mapped log buffer. </returns>
        LogBuffers Map(string logFileName, MapMode mapMode);
    }
}