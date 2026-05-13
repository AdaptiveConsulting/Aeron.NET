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

using Adaptive.Aeron.Command;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Command
{
    public class CounterMessageFlyweightTest
    {
        private readonly UnsafeBuffer _buffer = new UnsafeBuffer(new byte[128]);
        private readonly CounterMessageFlyweight _flyweight = new CounterMessageFlyweight();

        [Test]
        public void KeyBuffer()
        {
            const int offset = 24;
            _buffer.SetMemory(0, offset, 15);
            _flyweight.Wrap(_buffer, offset);

            _flyweight.KeyBuffer(NewBuffer(16), 4, 8);

            Assert.AreEqual(8, _flyweight.KeyBufferLength());
            Assert.AreEqual(CounterMessageFlyweight.KeyBufferFieldOffset, _flyweight.KeyBufferOffset());
        }

        [Test]
        public void LabelBuffer()
        {
            const int offset = 40;
            _buffer.SetMemory(0, offset, 0xFF);
            _flyweight.Wrap(_buffer, offset);
            _flyweight.KeyBuffer(NewBuffer(16), 6, 9);

            _flyweight.LabelBuffer(NewBuffer(32), 2, 21);

            Assert.AreEqual(21, _flyweight.LabelBufferLength());
            Assert.AreEqual(CounterMessageFlyweight.KeyBufferFieldOffset + 16, _flyweight.LabelBufferOffset());
            Assert.AreEqual(CounterMessageFlyweight.KeyBufferFieldOffset + 37, _flyweight.Length());
        }

        private static IDirectBuffer NewBuffer(int length)
        {
            var bytes = new byte[length];
            for (int i = 0; i < length; i++)
            {
                bytes[i] = 1;
            }

            var buffer = new UnsafeBuffer(new byte[4 + length]);
            buffer.PutBytes(4, bytes);
            return buffer;
        }
    }
}
