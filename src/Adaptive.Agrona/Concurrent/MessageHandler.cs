namespace Adaptive.Agrona.Concurrent
{
    /// <summary>
    /// Callback interface for processing of messages that are read from a buffer.
    /// Called for the processing of each message read from a buffer in turn.
    /// </summary>
    /// <param name="msgTypeId"> type of the encoded message.</param>
    /// <param name="buffer"> containing the encoded message.</param>
    /// <param name="index"> at which the encoded message begins.</param>
    /// <param name="length"> in bytes of the encoded message.</param>
    public delegate void MessageHandler(int msgTypeId, UnsafeBuffer buffer, int index, int length);
}