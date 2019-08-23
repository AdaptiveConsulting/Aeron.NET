namespace Adaptive.Aeron.Exceptions
{
    /// <summary>
    /// Conclude has been called concurrently on a Context. The caller that receives this should not close the
    /// concluded context as it will be owned by another caller.
    /// </summary>
    public class ConcurrentConcludeException : AeronException
    {
        
    }
}