/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;

namespace Adaptive.Archiver.IntegrationTests.Helpers
{
    internal sealed class BufferingFragmentHandler : IFragmentHandler
    {
        public List<byte[]> ReceivedPayloads { get; } = new();
        public long Position { get; private set; }

        public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
        {
            Position = header.Position;
            var bytes = new byte[length];
            buffer.GetBytes(offset, bytes);
            ReceivedPayloads.Add(bytes);
        }

        public bool HasReceivedPayloads(int numberOfPayloads) => ReceivedPayloads.Count >= numberOfPayloads;
    }
}
