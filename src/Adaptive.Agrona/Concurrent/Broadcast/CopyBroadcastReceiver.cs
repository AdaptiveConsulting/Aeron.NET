/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;

namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Receiver that copies messages which have been broadcast to enable a simpler API for the client.
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
            _scratchBuffer = new UnsafeBuffer(BufferUtil.AllocateDirect(scratchBufferLength));
        }

        /// <summary>
        /// Wrap a <seealso cref="BroadcastReceiver"/> to simplify the API for receiving messages.
        /// </summary>
        /// <param name="receiver"> to be wrapped. </param>
        public CopyBroadcastReceiver(BroadcastReceiver receiver)
        {
            _receiver = receiver;
            _scratchBuffer = new UnsafeBuffer(BufferUtil.AllocateDirect(ScratchBufferSize));
        }

        /// <summary>
        /// Receive one message from the broadcast buffer.
        /// </summary>
        /// <param name="handler"> to be called for each message received. </param>
        /// <returns> the number of messages that have been received. </returns>
        public int Receive(MessageHandler handler)
        {
            var messagesReceived = 0;
            var receiver = _receiver;
            var lastSeenLappedCount = receiver.LappedCount();

            if (receiver.ReceiveNext())
            {
                if (lastSeenLappedCount != receiver.LappedCount())
                {
                    throw new InvalidOperationException("Unable to keep up with broadcast");
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
                    throw new InvalidOperationException("Unable to keep up with broadcast");
                }

                handler(msgTypeId, _scratchBuffer, 0, length);

                messagesReceived = 1;
            }

            return messagesReceived;
        }
    }
}