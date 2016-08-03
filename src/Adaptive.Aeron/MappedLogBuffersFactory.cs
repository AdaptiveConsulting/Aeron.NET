using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Default mapping byteBuffer lifecycle strategy for the client
    /// </summary>
    internal class MappedLogBuffersFactory : ILogBuffersFactory
    {
        public LogBuffers Map(string logFileName, MapMode mapMode)
        {
            return new LogBuffers(logFileName, mapMode);
        }
    }

}