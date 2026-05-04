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
            Assert.AreEqual(TerminateDriverFlyweight.TOKEN_BUFFER_OFFSET, flyweight.TokenBufferOffset());
            Assert.AreEqual(TerminateDriverFlyweight.TOKEN_BUFFER_OFFSET + 8, flyweight.Length());
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
