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
        private BufferBuilder _bufferBuilder;

        [SetUp]
        public void Setup()
        {
            _bufferBuilder = new BufferBuilder();
        }

        [Test]
        public void ShouldInitialiseToDefaultValues()
        {
            Assert.That(_bufferBuilder.Capacity(), Is.EqualTo(0));
            Assert.That(_bufferBuilder.Buffer().Capacity, Is.EqualTo(0));
            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(0));
        }

        [Test]
        public void ShouldAppendNothingForZeroLength()
        {
            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[BufferBuilderUtil.MIN_ALLOCATED_CAPACITY]);

            _bufferBuilder.Append(srcBuffer, 0, 0);

            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(0));
        }

        [Test]
        public void ShouldGrowToMultipleOfInitialCapaity()
        {
            int srcCapacity = BufferBuilderUtil.MIN_ALLOCATED_CAPACITY * 5;
            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[srcCapacity]);

            _bufferBuilder.Append(srcBuffer, 0, srcBuffer.Capacity);

            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(srcCapacity));
            Assert.That(_bufferBuilder.Capacity, Is.GreaterThanOrEqualTo(srcCapacity));
        }

        [Test]
        public void ShouldAppendThenReset()
        {
            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[BufferBuilderUtil.MIN_ALLOCATED_CAPACITY]);

            _bufferBuilder.Append(srcBuffer, 0, srcBuffer.Capacity);

            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(srcBuffer.Capacity));

            _bufferBuilder.Reset();

            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(0));
        }

        [Test]
        public void ShouldAppendOneBufferWithoutResizing()
        {
            var srcBuffer = new UnsafeBuffer(new byte[BufferBuilderUtil.MIN_ALLOCATED_CAPACITY]);
            var bytes = Encoding.UTF8.GetBytes("Hello World");
            srcBuffer.PutBytes(0, bytes, 0, bytes.Length);

            _bufferBuilder.Append(srcBuffer, 0, bytes.Length);

            byte[] temp = new byte[bytes.Length];
            _bufferBuilder.Buffer().GetBytes(0, temp, 0, bytes.Length);

            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(bytes.Length));
            Assert.That(_bufferBuilder.Capacity(), Is.EqualTo(BufferBuilderUtil.MIN_ALLOCATED_CAPACITY));
            Assert.That(temp, Is.EqualTo(bytes));
        }

        [Test]
        public void ShouldAppendTwoBuffersWithoutResizing()
        {
            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[BufferBuilderUtil.MIN_ALLOCATED_CAPACITY]);
            byte[] bytes = Encoding.UTF8.GetBytes("1111111122222222");
            srcBuffer.PutBytes(0, bytes, 0, bytes.Length);

            _bufferBuilder.Append(srcBuffer, 0, bytes.Length / 2);
            _bufferBuilder.Append(srcBuffer, bytes.Length / 2, bytes.Length / 2);

            byte[] temp = new byte[bytes.Length];
            _bufferBuilder.Buffer().GetBytes(0, temp, 0, bytes.Length);

            Assert.That(_bufferBuilder.Limit(), Is.EqualTo(bytes.Length));
            Assert.That(_bufferBuilder.Capacity(), Is.EqualTo(BufferBuilderUtil.MIN_ALLOCATED_CAPACITY));
            Assert.That(temp, Is.EqualTo(bytes));
        }

        [Test]
        public void ShouldFillBufferWithoutResizing()
        {
            const int bufferLength = 128;
            byte[] buffer = new byte[bufferLength];
            Arrays.Fill(buffer, (byte)7);
            UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

            BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);

            bufferBuilder.Append(srcBuffer, 0, bufferLength);

            byte[] temp = new byte[bufferLength];
            bufferBuilder.Buffer().GetBytes(0, temp, 0, bufferLength);

            Assert.That(bufferBuilder.Limit(), Is.EqualTo(bufferLength));
            Assert.That(bufferBuilder.Capacity(), Is.EqualTo(bufferLength));
            Assert.That(temp, Is.EqualTo(buffer));
        }

        [Test]
        public void ShouldResizeWhenBufferJustDoesNotFit()
        {
            const int bufferLength = 128;
            byte[] buffer = new byte[bufferLength + 1];
            Arrays.Fill(buffer, (byte)7);
            UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

            BufferBuilder bufferBuilder = new BufferBuilder(bufferLength);

            bufferBuilder.Append(srcBuffer, 0, buffer.Length);

            byte[] temp = new byte[buffer.Length];
            bufferBuilder.Buffer().GetBytes(0, temp, 0, buffer.Length);

            Assert.That(bufferBuilder.Limit(), Is.EqualTo(buffer.Length));
            Assert.That(bufferBuilder.Capacity(), Is.GreaterThan(bufferLength));
            Assert.That(temp, Is.EqualTo(buffer));
        }

        [Test]
        public void ShouldAppendTwoBuffersAndResize()
        {
            const int bufferLength = 128;
            byte[] buffer = new byte[bufferLength];
            int firstLength = buffer.Length / 4;
            int secondLength = buffer.Length / 2;
            Arrays.Fill(buffer, 0, firstLength + secondLength, (byte)7);
            UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

            BufferBuilder bufferBuilder = new BufferBuilder(bufferLength / 2);

            bufferBuilder.Append(srcBuffer, 0, firstLength);
            bufferBuilder.Append(srcBuffer, firstLength, secondLength);

            byte[] temp = new byte[buffer.Length];
            bufferBuilder.Buffer().GetBytes(0, temp, 0, secondLength + firstLength);

            Assert.That(bufferBuilder.Limit(), Is.EqualTo(firstLength + secondLength));
            Assert.That(bufferBuilder.Capacity(), Is.GreaterThanOrEqualTo(firstLength + secondLength));
            Assert.That(temp, Is.EqualTo(buffer));
        }

        [Test]
        public void ShouldCompactBufferToLowerLimit()
        {
            int bufferLength = BufferBuilderUtil.MIN_ALLOCATED_CAPACITY / 2;
            byte[] buffer = new byte[bufferLength];
            UnsafeBuffer srcBuffer = new UnsafeBuffer(buffer);

            BufferBuilder bufferBuilder = new BufferBuilder();

            const int bufferCount = 5;
            for (int i = 0; i < bufferCount; i++)
            {
                bufferBuilder.Append(srcBuffer, 0, buffer.Length);
            }

            int expectedLimit = buffer.Length * bufferCount;
            Assert.That(bufferBuilder.Limit(), Is.EqualTo(expectedLimit));
            int expandedCapacity = bufferBuilder.Capacity();
            Assert.That(expandedCapacity, Is.GreaterThan(expectedLimit));

            bufferBuilder.Reset();

            bufferBuilder.Append(srcBuffer, 0, buffer.Length);
            bufferBuilder.Append(srcBuffer, 0, buffer.Length);
            bufferBuilder.Append(srcBuffer, 0, buffer.Length);

            bufferBuilder.Compact();

            Assert.That(bufferBuilder.Limit(), Is.EqualTo(buffer.Length * 3));
            Assert.That(bufferBuilder.Capacity(), Is.LessThan(expandedCapacity));
        }

        internal static class Arrays
        {
            internal static void Fill<T>(T[] array, T value)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    array[i] = value;
                }
            }

            internal static void Fill<T>(T[] array, int fromIndex, int toIndex, T value)
            {
                for (int i = fromIndex; i < toIndex; i++)
                {
                    array[i] = value;
                }
            }
        }
    }
}