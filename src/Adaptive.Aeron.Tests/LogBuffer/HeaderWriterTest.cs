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

using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    public class HeaderWriterTest
    {
        private readonly UnsafeBuffer _defaultHeaderBuffer = new UnsafeBuffer(new byte[32]);
        private readonly UnsafeBuffer _termBuffer = new UnsafeBuffer(new byte[1024]);

        [SetUp]
        public void Before()
        {
            _defaultHeaderBuffer.SetMemory(0, _defaultHeaderBuffer.Capacity, 0xFF);
            _termBuffer.SetMemory(0, _termBuffer.Capacity, 0xFF);
        }

        // Java byte values are signed (-128..127); .NET byte is unsigned (0..255).
        // Bit patterns map: Java -2 -> 0xFE -> 254; Java -128 -> 0x80 -> 128.
        [TestCase(100, (byte)8, (byte)5, (short)9, 352, -777, -1000, -33)]
        [TestCase(-99, (byte)254, (byte)7, (short)1, 8, 42, 3, 89)]
        [TestCase(123, (byte)0, (byte)0, (short)0, 0, 0, 0, 0)]
        [TestCase(32, (byte)1, (byte)128, (short)4, 96, int.MaxValue, int.MinValue, int.MinValue)]
        public void ShouldEncodeHeaderUsingLittleEndianByteOrder(
            int frameLength,
            byte version,
            byte flags,
            short headerType,
            int termOffset,
            int sessionId,
            int streamId,
            int termId
        )
        {
            Assume.That(BitConverter.IsLittleEndian, "LE-only test");

            _defaultHeaderBuffer.PutByte(HeaderFlyweight.VERSION_FIELD_OFFSET, version);
            _defaultHeaderBuffer.PutByte(HeaderFlyweight.FLAGS_FIELD_OFFSET, flags);
            PutShort(_defaultHeaderBuffer, HeaderFlyweight.TYPE_FIELD_OFFSET, headerType, ByteOrder.LittleEndian);
            PutInt(
                _defaultHeaderBuffer,
                DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET,
                sessionId,
                ByteOrder.LittleEndian
            );
            PutInt(_defaultHeaderBuffer, DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.LittleEndian);

            var headerWriter = HeaderWriter.NewInstance(_defaultHeaderBuffer);
            Assert.AreEqual(typeof(HeaderWriter), headerWriter.GetType());
            headerWriter.Write(_termBuffer, termOffset, frameLength, termId);

            Assert.AreEqual(
                -frameLength,
                GetInt(_termBuffer, termOffset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LittleEndian)
            );
            Assert.AreEqual(version, _termBuffer.GetByte(termOffset + HeaderFlyweight.VERSION_FIELD_OFFSET));
            Assert.AreEqual(flags, _termBuffer.GetByte(termOffset + HeaderFlyweight.FLAGS_FIELD_OFFSET));
            Assert.AreEqual(
                headerType,
                GetShort(_termBuffer, termOffset + HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.LittleEndian)
            );
            Assert.AreEqual(
                termOffset,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, ByteOrder.LittleEndian)
            );
            Assert.AreEqual(
                sessionId,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET, ByteOrder.LittleEndian)
            );
            Assert.AreEqual(
                streamId,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, ByteOrder.LittleEndian)
            );
            Assert.AreEqual(
                termId,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET, ByteOrder.LittleEndian)
            );
        }

        [TestCase(100, (byte)8, (byte)5, (short)9, 352, -777, -1000, -33)]
        [TestCase(-99, (byte)254, (byte)7, (short)1, 8, 42, 3, 89)]
        [TestCase(123, (byte)0, (byte)0, (short)0, 0, 0, 0, 0)]
        [TestCase(32, (byte)1, (byte)128, (short)4, 96, int.MaxValue, int.MinValue, int.MinValue)]
        public void ShouldEncodeHeaderUsingBigEndianByteOrder(
            int frameLength,
            byte version,
            byte flags,
            short headerType,
            int termOffset,
            int sessionId,
            int streamId,
            int termId
        )
        {
            if (BitConverter.IsLittleEndian)
            {
                Assert.Ignore("BE-only test; .NET has no BE host. Kept for Java parity.");
            }

            _defaultHeaderBuffer.PutByte(HeaderFlyweight.VERSION_FIELD_OFFSET, version);
            _defaultHeaderBuffer.PutByte(HeaderFlyweight.FLAGS_FIELD_OFFSET, flags);
            PutShort(_defaultHeaderBuffer, HeaderFlyweight.TYPE_FIELD_OFFSET, headerType, ByteOrder.BigEndian);
            PutInt(_defaultHeaderBuffer, DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET, sessionId, ByteOrder.BigEndian);
            PutInt(_defaultHeaderBuffer, DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, streamId, ByteOrder.BigEndian);

            var headerWriter = HeaderWriter.NewInstance(_defaultHeaderBuffer);
            Assert.AreNotEqual(typeof(HeaderWriter), headerWriter.GetType());
            headerWriter.Write(_termBuffer, termOffset, frameLength, termId);

            Assert.AreEqual(
                -frameLength,
                GetInt(_termBuffer, termOffset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, ByteOrder.BigEndian)
            );
            Assert.AreEqual(version, _termBuffer.GetByte(termOffset + HeaderFlyweight.VERSION_FIELD_OFFSET));
            Assert.AreEqual(flags, _termBuffer.GetByte(termOffset + HeaderFlyweight.FLAGS_FIELD_OFFSET));
            Assert.AreEqual(
                headerType,
                GetShort(_termBuffer, termOffset + HeaderFlyweight.TYPE_FIELD_OFFSET, ByteOrder.BigEndian)
            );
            Assert.AreEqual(
                termOffset,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, ByteOrder.BigEndian)
            );
            Assert.AreEqual(
                sessionId,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET, ByteOrder.BigEndian)
            );
            Assert.AreEqual(
                streamId,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, ByteOrder.BigEndian)
            );
            Assert.AreEqual(
                termId,
                GetInt(_termBuffer, termOffset + DataHeaderFlyweight.TERM_ID_FIELD_OFFSET, ByteOrder.BigEndian)
            );
        }

        // Native UnsafeBuffer reads/writes use host order. These helpers apply the requested
        // byte order via EndianessConverter so the test reads match Java's putShort/getShort
        // overloads that take a ByteOrder argument.
        private static void PutShort(UnsafeBuffer buffer, int index, short value, ByteOrder byteOrder) =>
            buffer.PutShort(index, EndianessConverter.ApplyInt16(byteOrder, value));

        private static void PutInt(UnsafeBuffer buffer, int index, int value, ByteOrder byteOrder) =>
            buffer.PutInt(index, EndianessConverter.ApplyInt32(byteOrder, value));

        private static short GetShort(UnsafeBuffer buffer, int index, ByteOrder byteOrder) =>
            EndianessConverter.ApplyInt16(byteOrder, buffer.GetShort(index));

        private static int GetInt(UnsafeBuffer buffer, int index, ByteOrder byteOrder) =>
            EndianessConverter.ApplyInt32(byteOrder, buffer.GetInt(index));
    }
}
