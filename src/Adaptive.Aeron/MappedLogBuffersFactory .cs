namespace Adaptive.Aeron
{
    /// <summary>
    /// Default mapping byteBuffer lifecycle strategy for the client
    /// </summary>
    internal class MappedLogBuffersFactory : ILogBuffersFactory
    {
        public virtual LogBuffers Map(string logFileName)
        {
            return new LogBuffers(logFileName);
        }
    }

}