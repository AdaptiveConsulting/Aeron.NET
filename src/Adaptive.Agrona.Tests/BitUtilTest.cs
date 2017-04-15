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

using NUnit.Framework;

namespace Adaptive.Agrona.Tests
{
    [TestFixture]
    public class BitUtilTest
    {
        [Test]
        public void ShouldReturnNextPositivePowerOfTwo()
        {
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(int.MinValue), Is.EqualTo(int.MinValue));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(int.MinValue + 1), Is.EqualTo(1));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(-1), Is.EqualTo(1));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(0), Is.EqualTo(1));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(1), Is.EqualTo(1));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(2), Is.EqualTo(2));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(3), Is.EqualTo(4));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(4), Is.EqualTo(4));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(31), Is.EqualTo(32));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(32), Is.EqualTo(32));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo(1 << 30), Is.EqualTo(1 << 30));
            Assert.That(BitUtil.FindNextPositivePowerOfTwo((1 << 30) + 1), Is.EqualTo(int.MinValue));
        }

        [Test]
        public void ShouldAlignValueToNextMultipleOfAlignment()
        {
            int alignment = BitUtil.CACHE_LINE_LENGTH;

            Assert.That(BitUtil.Align(0, alignment), Is.EqualTo(0));
            Assert.That(BitUtil.Align(1, alignment), Is.EqualTo(alignment));
            Assert.That(BitUtil.Align(alignment, alignment), Is.EqualTo(alignment));
            Assert.That(BitUtil.Align(alignment + 1, alignment), Is.EqualTo(alignment * 2));

            int remainder = int.MaxValue % alignment;
            int maxMultiple = int.MaxValue - remainder;

            Assert.That(BitUtil.Align(maxMultiple, alignment), Is.EqualTo(maxMultiple));
            Assert.That(BitUtil.Align(int.MaxValue, alignment), Is.EqualTo(int.MinValue));
        }

        [Test]
        public void ShouldConvertToHexCorrectly()
        {
            byte[] buffer =  { 0x01, 0x23, 0x45, 0x69, 0x78, 0xBC, 0xDA, 0xEF, 0x5F };
            byte[] converted = BitUtil.ToHexByteArray(buffer);
            string hexStr = BitUtil.ToHex(buffer);

            Assert.That(converted[0], Is.EqualTo((sbyte)'0'));
            Assert.That(converted[1], Is.EqualTo((sbyte)'1'));
            Assert.That(converted[2], Is.EqualTo((sbyte)'2'));
            Assert.That(converted[3], Is.EqualTo((sbyte)'3'));
            Assert.That(hexStr, Is.EqualTo("0123456978bcdaef5f"));
        }

        [Test]
        public void ShouldDetectEvenAndOddNumbers()
        {
            Assert.IsTrue(BitUtil.IsEven(0));
            Assert.IsTrue(BitUtil.IsEven(2));
            Assert.IsTrue(BitUtil.IsEven(int.MinValue));

            Assert.IsFalse(BitUtil.IsEven(1));
            Assert.IsFalse(BitUtil.IsEven(-1));
            Assert.IsFalse(BitUtil.IsEven(int.MaxValue));
        }
    }
}