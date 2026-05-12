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
using Adaptive.Aeron.Command;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Command
{
    public class TerminateDriverFlyweightTest
    {
        [Test]
        public void TokenBuffer()
        {
            const int offset = 24;
            var buffer = new UnsafeBuffer(new byte[128]);
            buffer.SetMemory(0, offset, 15);
            var flyweight = new TerminateDriverFlyweight();
            flyweight.Wrap(buffer, offset);

            flyweight.TokenBuffer(NewBuffer(16), 4, 8);

            Assert.AreEqual(8, flyweight.TokenBufferLength());
            Assert.AreEqual(TerminateDriverFlyweight.TokenBufferFieldOffset, flyweight.TokenBufferOffset());
            Assert.AreEqual(TerminateDriverFlyweight.TokenBufferFieldOffset + 8, flyweight.Length());
        }

        private static IDirectBuffer NewBuffer(int length)
        {
            var bytes = new byte[length];
            Array.Fill(bytes, (byte)1);
            var buffer = new UnsafeBuffer(new byte[4 + length]);
            buffer.PutBytes(4, bytes);
            return buffer;
        }
    }
}
