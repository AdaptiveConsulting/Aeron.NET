namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Receiver that copies messages that have been broadcast to enable a simpler API for the client.
    /// </summary>
    public class CopyBroadcastReceiver
    {
        private const int ScratchBufferSize = 4096;

        private readonly BroadcastReceiver _receiver;
        private readonly IMutableDirectBuffer _scratchBuffer;

        /// <summary>
        /// Wrap a <seealso cref="BroadcastReceiver"/> to simplify the API for receiving messages.
        /// </summary>
        /// <param name="receiver"> to be wrapped. </param>
        public CopyBroadcastReceiver(BroadcastReceiver receiver)
        {
            _receiver = receiver;
            _scratchBuffer = new UnsafeBuffer(new sbyte[ScratchBufferSize]);

            while (receiver.ReceiveNext())
            {
                // If we're reconnecting to a broadcast buffer then we need to
                // scan ourselves up to date, otherwise we risk "falling behind"
                // the buffer due to the time taken to catchup.
            }
        }

        /// <summary>
        /// Receive one message from the broadcast buffer.
        /// </summary>
        /// <param name="handler"> to be called for each message received. </param>
        /// <returns> the number of messages that have been received. </returns>
        public virtual int Receive(IMessageHandler handler)
        {
            var messagesReceived = 0;
            var receiver = _receiver;
            var lastSeenLappedCount = receiver.LappedCount();

            if (receiver.ReceiveNext())
            {
                if (lastSeenLappedCount != receiver.LappedCount())
                {
                    throw new System.InvalidOperationException("Unable to keep up with broadcast buffer");
                }

                var length = receiver.Length();
                var capacity = _scratchBuffer.Capacity;
                if (length > capacity)
                {
                    throw new System.InvalidOperationException($"Buffer required size {length:D} but only has {capacity:D}");
                }

                var msgTypeId = receiver.TypeId();
                _scratchBuffer.PutBytes(0, receiver.Buffer(), receiver.Offset(), length);

                if (!receiver.Validate())
                {
                    throw new System.InvalidOperationException("Unable to keep up with broadcast buffer");
                }

                handler.OnMessage(msgTypeId, _scratchBuffer, 0, length);

                messagesReceived = 1;
            }

            return messagesReceived;
        }
    }
}