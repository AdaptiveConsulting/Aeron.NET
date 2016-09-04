using System;

namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Receiver that copies messages that have been broadcast to enable a simpler API for the client.
    /// </summary>
    public class CopyBroadcastReceiver
    {
        /// <summary>
        /// Default length for the scratch buffer for copying messages into.
        /// </summary>
        private const int ScratchBufferSize = 4096;

        private readonly BroadcastReceiver _receiver;
        private readonly UnsafeBuffer _scratchBuffer;

        public CopyBroadcastReceiver()
        {
            
        }

        /// <summary>
        /// Wrap a <seealso cref="BroadcastReceiver"/> to simplify the API for receiving messages.
        /// </summary>
        /// <param name="receiver"> to be wrapped. </param>
        /// <param name="scratchBufferLength">  is the maximum length of a message to be copied when receiving.</param>
        public CopyBroadcastReceiver(BroadcastReceiver receiver, int scratchBufferLength)
        {
            _receiver = receiver;
            _scratchBuffer = new UnsafeBuffer(new byte[scratchBufferLength]);

            while (receiver.ReceiveNext())
            {
                // If we're reconnecting to a broadcast buffer then we need to
                // scan ourselves up to date, otherwise we risk "falling behind"
                // the buffer due to the time taken to catchup.
            }
        }

        /// <summary>
        /// Wrap a <seealso cref="BroadcastReceiver"/> to simplify the API for receiving messages.
        /// </summary>
        /// <param name="receiver"> to be wrapped. </param>
        public CopyBroadcastReceiver(BroadcastReceiver receiver)
        {
            _receiver = receiver;
            _scratchBuffer = new UnsafeBuffer(new byte[ScratchBufferSize]);

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
#if DEBUG
        public virtual int Receive(MessageHandler handler)
#else
        public  int Receive(MessageHandler handler)
#endif
        {
            var messagesReceived = 0;
            var receiver = _receiver;
            var lastSeenLappedCount = receiver.LappedCount();

            if (receiver.ReceiveNext())
            {
                if (lastSeenLappedCount != receiver.LappedCount())
                {
                    throw new InvalidOperationException("Unable to keep up with broadcast buffer");
                }

                var length = receiver.Length();
                var capacity = _scratchBuffer.Capacity;
                if (length > capacity)
                {
                    throw new InvalidOperationException($"Buffer required length of {length:D} but only has {capacity:D}");
                }

                var msgTypeId = receiver.TypeId();
                _scratchBuffer.PutBytes(0, receiver.Buffer(), receiver.Offset(), length);

                if (!receiver.Validate())
                {
                    throw new InvalidOperationException("Unable to keep up with broadcast buffer");
                }

                handler(msgTypeId, _scratchBuffer, 0, length);

                messagesReceived = 1;
            }

            return messagesReceived;
        }
    }
}