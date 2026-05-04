using Adaptive.Aeron.Command;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.Command
{
    public class CounterMessageFlyweightTest
    {
        private readonly UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        private readonly CounterMessageFlyweight flyweight = new CounterMessageFlyweight();

        [Test]
        public void KeyBuffer()
        {
            const int offset = 24;
            buffer.SetMemory(0, offset, 15);
            flyweight.Wrap(buffer, offset);

            flyweight.KeyBuffer(NewBuffer(16), 4, 8);

            Assert.AreEqual(8, flyweight.KeyBufferLength());
            Assert.AreEqual(CounterMessageFlyweight.KEY_BUFFER_OFFSET, flyweight.KeyBufferOffset());
        }

        [Test]
        public void LabelBuffer()
        {
            const int offset = 40;
            buffer.SetMemory(0, offset, 0xFF);
            flyweight.Wrap(buffer, offset);
            flyweight.KeyBuffer(NewBuffer(16), 6, 9);

            flyweight.LabelBuffer(NewBuffer(32), 2, 21);

            Assert.AreEqual(21, flyweight.LabelBufferLength());
            Assert.AreEqual(CounterMessageFlyweight.KEY_BUFFER_OFFSET + 16, flyweight.LabelBufferOffset());
            Assert.AreEqual(CounterMessageFlyweight.KEY_BUFFER_OFFSET + 37, flyweight.Length());
        }

        private static IDirectBuffer NewBuffer(int length)
        {
            var bytes = new byte[length];
            for (int i = 0; i < length; i++) bytes[i] = 1;
            var buffer = new UnsafeBuffer(new byte[4 + length]);
            buffer.PutBytes(4, bytes);
            return buffer;
        }
    }
}
