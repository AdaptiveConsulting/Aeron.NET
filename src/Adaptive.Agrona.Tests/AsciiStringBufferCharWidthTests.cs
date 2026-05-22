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

using System.Text;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests
{
    /// <summary>
    /// Regression tests for a Java-to-.NET porting bug in <see cref="UnsafeBuffer"/> and
    /// <see cref="ExpandableArrayBuffer"/> where ASCII string helpers used
    /// <c>*(char*)(buf + i) = c</c> in a byte-indexed loop. Because a .NET <c>char</c>
    /// is 2 bytes, that:
    ///   (1) overran the bounds-checked region by one byte in
    ///       <c>PutStringWithoutLengthAscii</c>, and
    ///   (2) read the same 2 bytes every iteration in
    ///       <c>GetStringAscii(int, StringBuilder)</c> because the offset
    ///       expression mistakenly used <c>index</c> instead of the loop cursor.
    /// </summary>
    [TestFixture]
    public class AsciiStringBufferCharWidthTests
    {
        // ---- UnsafeBuffer ----

        [Test]
        public void UnsafeBuffer_PutStringWithoutLengthAscii_DoesNotOverrunBoundsByOneByte()
        {
            byte[] backing = { 0xAA, 0xAA, 0xAA, 0xAA };
            var buffer = new UnsafeBuffer(backing);

            int written = buffer.PutStringWithoutLengthAscii(0, "abc");

            Assert.AreEqual(3, written);
            Assert.AreEqual((byte)'a', backing[0]);
            Assert.AreEqual((byte)'b', backing[1]);
            Assert.AreEqual((byte)'c', backing[2]);
            // With the pre-fix code this byte was clobbered to 0x00 by the
            // trailing high-byte of the final 2-byte char write.
            Assert.AreEqual(0xAA, backing[3], "sentinel byte at offset==length must not be overwritten");
        }

        [Test]
        public void UnsafeBuffer_PutStringWithoutLengthAscii_WithValueOffsetAndLength_DoesNotOverrun()
        {
            byte[] backing = { 0xAA, 0xAA, 0xAA, 0xAA, 0xAA };
            var buffer = new UnsafeBuffer(backing);

            int written = buffer.PutStringWithoutLengthAscii(0, "xabcdx", 1, 4);

            Assert.AreEqual(4, written);
            Assert.AreEqual((byte)'a', backing[0]);
            Assert.AreEqual((byte)'b', backing[1]);
            Assert.AreEqual((byte)'c', backing[2]);
            Assert.AreEqual((byte)'d', backing[3]);
            Assert.AreEqual(0xAA, backing[4], "sentinel byte at offset==length must not be overwritten");
        }

        [Test]
        public void UnsafeBuffer_GetStringAscii_StringBuilder_AdvancesReadOffset()
        {
            byte[] backing = new byte[16];
            var buffer = new UnsafeBuffer(backing);

            buffer.PutStringAscii(0, "abcd");

            var sb = new StringBuilder();
            buffer.GetStringAscii(0, sb);

            // With the pre-fix code, every loop iteration read from `index`
            // (the length prefix), so the result was 4 copies of ''.
            Assert.AreEqual("abcd", sb.ToString());
        }

        // ---- ExpandableArrayBuffer ----

        [Test]
        public void ExpandableArrayBuffer_PutStringWithoutLengthAscii_DoesNotOverrunBoundsByOneByte()
        {
            var buffer = new ExpandableArrayBuffer(16);
            byte[] backing = buffer.ByteArray;
            for (int i = 0; i < backing.Length; i++)
            {
                backing[i] = 0xAA;
            }

            int written = buffer.PutStringWithoutLengthAscii(0, "abc");

            Assert.AreEqual(3, written);
            Assert.AreEqual((byte)'a', backing[0]);
            Assert.AreEqual((byte)'b', backing[1]);
            Assert.AreEqual((byte)'c', backing[2]);
            Assert.AreEqual(0xAA, backing[3], "sentinel byte at offset==length must not be overwritten");
        }

        [Test]
        public void ExpandableArrayBuffer_PutStringWithoutLengthAscii_WithValueOffsetAndLength_DoesNotOverrun()
        {
            var buffer = new ExpandableArrayBuffer(16);
            byte[] backing = buffer.ByteArray;
            for (int i = 0; i < backing.Length; i++)
            {
                backing[i] = 0xAA;
            }

            int written = buffer.PutStringWithoutLengthAscii(0, "xabcdx", 1, 4);

            Assert.AreEqual(4, written);
            Assert.AreEqual((byte)'a', backing[0]);
            Assert.AreEqual((byte)'b', backing[1]);
            Assert.AreEqual((byte)'c', backing[2]);
            Assert.AreEqual((byte)'d', backing[3]);
            Assert.AreEqual(0xAA, backing[4], "sentinel byte at offset==length must not be overwritten");
        }

        [Test]
        public void ExpandableArrayBuffer_GetStringAscii_StringBuilder_AdvancesReadOffset()
        {
            var buffer = new ExpandableArrayBuffer(16);

            buffer.PutStringAscii(0, "abcd");

            var sb = new StringBuilder();
            buffer.GetStringAscii(0, sb);

            Assert.AreEqual("abcd", sb.ToString());
        }
    }
}
