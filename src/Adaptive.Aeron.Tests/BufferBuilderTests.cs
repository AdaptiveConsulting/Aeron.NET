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

using System.Text;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    [TestFixture]
    public class BufferBuilderTests
    {
        [SetUp]
        public void Setup()
        {
            _bufferBuilder = new BufferBuilder();
        }

        private BufferBuilder _bufferBuilder;

        [Test]
        public void ShouldFindMaxCapacityWhenRequested()
        {
            Assert.AreEqual(BufferBuilder.MAX_CAPACITY,
                BufferBuilder.FindSuitableCapacity(0, BufferBuilder.MAX_CAPACITY));
        }
        
        [Test]
        public void ShouldInitialiseToDefaultValues()
        {
            Assert.AreEqual(0, _bufferBuilder.Capacity());
            Assert.AreEqual(0, _bufferBuilder.Buffer().Capacity);
            Assert.AreEqual(0, _bufferBuilder.Limit());
        }

        [Test]
        public void ShouldAppendNothingForZeroLength()
        {
            var srcBuffer = new UnsafeBuffer(new byte[BufferBuilder.INIT_MIN_CAPACITY]);

            _bufferBuilder.Append(srcBuffer, 0, 0);

            Assert.AreEqual(0, _bufferBuilder.Limit());
        }

        [Test]
        public void ShouldGrowToMultipleOfInitialCapacity()
        {
            var srcCapacity = BufferBuilder.INIT_MIN_CAPACITY * 5;
            var srcBuffer = new UnsafeBuffer(new byte[srcCapacity]);

            _bufferBuilder.Append(srcBuffer, 0, srcBuffer.Capacity);

            Assert.AreEqual(srcCapacity, _bufferBuilder.Limit());
            Assert.That(_bufferBuilder.Capacity, Is.GreaterThanOrEqualTo(srcCapacity));
        }

        [Test]
        public void ShouldAppendThenReset()
        {
            var srcBuffer = new UnsafeBuffer(new byte[BufferBuilder.INIT_MIN_CAPACITY]);

            _bufferBuilder.Append(srcBuffer, 0, srcBuffer.Capacity);

            Assert.AreEqual(srcBuffer.Capacity, _bufferBuilder.Limit());

            _bufferBuilder.Reset();

            Assert.AreEqual(0, _bufferBuilder.Limit());
        }

        [Test]
        public void ShouldAppendOneBufferWithoutResizing()
        {
            var srcBuffer = new UnsafeBuffer(new byte[BufferBuilder.INIT_MIN_CAPACITY]);
            var bytes = Encoding.UTF8.GetBytes("Hello World");
            srcBuffer.PutBytes(0, bytes, 0, bytes.Length);

            _bufferBuilder.Append(srcBuffer, 0, bytes.Length);

            var temp = new byte[bytes.Length];
            _bufferBuilder.Buffer().GetBytes(0, temp, 0, bytes.Length);

            Assert.AreEqual(bytes.Length, _bufferBuilder.Limit());
            Assert.AreEqual(BufferBuilder.INIT_MIN_CAPACITY, _bufferBuilder.Capacity());
            Assert.AreEqual(bytes, temp);
        }

        [Test]
        public void ShouldAppendTwoBuffersWithoutResizing()
        {
            var srcBuffer = new UnsafeBuffer(new byte[BufferBuilder.INIT_MIN_CAPACITY]);
            var bytes = Encoding.UTF8.GetBytes("1111111122222222");
            srcBuffer.PutBytes(0, bytes, 0, bytes.Length);

            _bufferBuilder.Append(srcBuffer, 0, bytes.Length / 2);
            _bufferBuilder.Append(srcBuffer, bytes.Length / 2, bytes.Length / 2);

            var temp = new byte[bytes.Length];
            _bufferBuilder.Buffer().GetBytes(0, temp, 0, bytes.Length);

            Assert.AreEqual(bytes.Length, _bufferBuilder.Limit());
            Assert.AreEqual(BufferBuilder.INIT_MIN_CAPACITY, _bufferBuilder.Capacity());
            Assert.AreEqual(bytes, temp);
        }

        [Test]
        public void ShouldFillBufferWithoutResizing()
        {
            const int bufferLength = 128;
            var buffer = new byte[bufferLength];
            Arrays.Fill(buffer, (byte)7);
            var srcBuffer = new UnsafeBuffer(buffer);

            var bufferBuilder = new BufferBuilder(bufferLength);

            bufferBuilder.Append(srcBuffer, 0, bufferLength);

            var temp = new byte[bufferLength];
            bufferBuilder.Buffer().GetBytes(0, temp, 0, bufferLength);

            Assert.AreEqual(bufferLength, bufferBuilder.Limit());
            Assert.AreEqual(bufferLength, bufferBuilder.Capacity());
            Assert.AreEqual(buffer, temp);
        }

        [Test]
        public void ShouldResizeWhenBufferJustDoesNotFit()
        {
            const int bufferLength = 128;
            var buffer = new byte[bufferLength + 1];
            Arrays.Fill(buffer, (byte)7);
            var srcBuffer = new UnsafeBuffer(buffer);

            var bufferBuilder = new BufferBuilder(bufferLength);

            bufferBuilder.Append(srcBuffer, 0, buffer.Length);

            var temp = new byte[buffer.Length];
            bufferBuilder.Buffer().GetBytes(0, temp, 0, buffer.Length);

            Assert.AreEqual(buffer.Length, bufferBuilder.Limit());
            Assert.That(bufferBuilder.Capacity(), Is.GreaterThan(bufferLength));
            Assert.AreEqual(buffer, temp);
        }

        [Test]
        public void ShouldAppendTwoBuffersAndResize()
        {
            const int bufferLength = 128;
            var buffer = new byte[bufferLength];
            var firstLength = buffer.Length / 4;
            var secondLength = buffer.Length / 2;
            Arrays.Fill(buffer, 0, firstLength + secondLength, (byte)7);
            var srcBuffer = new UnsafeBuffer(buffer);

            var bufferBuilder = new BufferBuilder(bufferLength / 2);

            bufferBuilder.Append(srcBuffer, 0, firstLength);
            bufferBuilder.Append(srcBuffer, firstLength, secondLength);

            var temp = new byte[buffer.Length];
            bufferBuilder.Buffer().GetBytes(0, temp, 0, secondLength + firstLength);

            Assert.AreEqual(firstLength + secondLength, bufferBuilder.Limit());
            Assert.That(bufferBuilder.Capacity(), Is.GreaterThanOrEqualTo(firstLength + secondLength));
            Assert.AreEqual(buffer, temp);
        }

        [Test]
        public void ShouldCompactBufferToLowerLimit()
        {
            var bufferLength = BufferBuilder.INIT_MIN_CAPACITY / 2;
            var buffer = new byte[bufferLength];
            var srcBuffer = new UnsafeBuffer(buffer);

            var bufferBuilder = new BufferBuilder();

            const int bufferCount = 5;
            for (var i = 0; i < bufferCount; i++) bufferBuilder.Append(srcBuffer, 0, buffer.Length);

            var expectedLimit = buffer.Length * bufferCount;
            Assert.AreEqual(expectedLimit, bufferBuilder.Limit());
            var expandedCapacity = bufferBuilder.Capacity();
            Assert.That(expandedCapacity, Is.GreaterThan(expectedLimit));

            bufferBuilder.Reset();

            bufferBuilder.Append(srcBuffer, 0, buffer.Length);
            bufferBuilder.Append(srcBuffer, 0, buffer.Length);
            bufferBuilder.Append(srcBuffer, 0, buffer.Length);

            bufferBuilder.Compact();

            Assert.AreEqual(buffer.Length * 3, bufferBuilder.Limit());
            Assert.That(bufferBuilder.Capacity(), Is.LessThan(expandedCapacity));
        }

        private static class Arrays
        {
            internal static void Fill<T>(T[] array, T value)
            {
                for (var i = 0; i < array.Length; i++) array[i] = value;
            }

            internal static void Fill<T>(T[] array, int fromIndex, int toIndex, T value)
            {
                for (var i = fromIndex; i < toIndex; i++) array[i] = value;
            }
        }
    }
}