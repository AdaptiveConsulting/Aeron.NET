/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Archiver.IntegrationTests.Helpers
{
    /// <summary>
    /// Verifies the message stream produced by <see cref="BackgroundPublisher"/>: each
    /// message must carry a monotonically increasing id in both the first and last 8 bytes.
    /// Throws if a fragment is missing, duplicated, or corrupted.
    /// </summary>
    internal sealed class MessageVerifier : IFragmentHandler
    {
        public long ExpectedMessageId { get; private set; }
        public long Position { get; private set; }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (length < 2 * sizeof(long))
            {
                throw new InvalidOperationException("length was " + length);
            }
            var messageId1 = buffer.GetLong(offset);
            var messageId2 = buffer.GetLong(offset + length - sizeof(long));
            if (messageId1 != messageId2)
            {
                throw new InvalidOperationException(
                    "message had different ids " + messageId1 + " and " + messageId2);
            }
            if (messageId1 != ExpectedMessageId)
            {
                throw new InvalidOperationException(
                    "expected id " + ExpectedMessageId + ", but got " + messageId1);
            }
            ExpectedMessageId = messageId1 + 1;
            Position = header.Position;
        }
    }
}
