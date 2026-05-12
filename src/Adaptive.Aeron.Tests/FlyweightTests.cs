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

using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class FlyweightTest
    {
        private bool _instanceFieldsInitialized = false;

        public FlyweightTest()
        {
            if (!_instanceFieldsInitialized)
            {
                InitializeInstanceFields();
                _instanceFieldsInitialized = true;
            }
        }

        private void InitializeInstanceFields()
        {
            _aBuff = new UnsafeBuffer(_buffer);
        }

        private readonly ByteBuffer _buffer = BufferUtil.Allocate(512);

        private UnsafeBuffer _aBuff;
        private readonly HeaderFlyweight _encodeHeader = new HeaderFlyweight();
        private readonly HeaderFlyweight _decodeHeader = new HeaderFlyweight();
        private readonly DataHeaderFlyweight _encodeDataHeader = new DataHeaderFlyweight();
        private readonly DataHeaderFlyweight _decodeDataHeader = new DataHeaderFlyweight();

        [Test]
        public void ShouldWriteCorrectValuesForGenericHeaderFields()
        {
            _encodeHeader.Wrap(_aBuff);

            _encodeHeader.Version((short)1);
            _encodeHeader.Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
            _encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            _encodeHeader.FrameLength(8);

            // little endian
            Assert.AreEqual((byte)0x08, _buffer.Get(0));
            Assert.AreEqual((byte)0x00, _buffer.Get(1));
            Assert.AreEqual((byte)0x00, _buffer.Get(2));
            Assert.AreEqual((byte)0x00, _buffer.Get(3));
            Assert.AreEqual((byte)0x01, _buffer.Get(4));
            Assert.AreEqual((byte)0xC0, _buffer.Get(5));
            Assert.AreEqual((byte)HeaderFlyweight.HDR_TYPE_DATA, _buffer.Get(6));
            Assert.AreEqual((byte)0x00, _buffer.Get(7));
        }

        [Test]
        public void ShouldReadWhatIsWrittenToGenericHeaderFields()
        {
            _encodeHeader.Wrap(_aBuff);

            _encodeHeader.Version(1);
            _encodeHeader.Flags(0);
            _encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            _encodeHeader.FrameLength(8);

            _decodeHeader.Wrap(_aBuff);
            Assert.AreEqual((short)1, _decodeHeader.Version());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_DATA, _decodeHeader.HeaderType());
            Assert.AreEqual(8, _decodeHeader.FrameLength());
        }

        [Test]
        public void ShouldWriteAndReadMultipleFramesCorrectly()
        {
            _encodeHeader.Wrap(_aBuff);

            _encodeHeader.Version(1);
            _encodeHeader.Flags(0);
            _encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            _encodeHeader.FrameLength(8);

            _encodeHeader.Wrap(_aBuff, 8, _aBuff.Capacity - 8);
            _encodeHeader.Version(2);
            _encodeHeader.Flags(0x01);
            _encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_SM);
            _encodeHeader.FrameLength(8);

            _decodeHeader.Wrap(_aBuff);
            Assert.AreEqual((short)1, _decodeHeader.Version());
            Assert.AreEqual((short)0, _decodeHeader.Flags());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_DATA, _decodeHeader.HeaderType());
            Assert.AreEqual(8, _decodeHeader.FrameLength());

            _decodeHeader.Wrap(_aBuff, 8, _aBuff.Capacity - 8);
            Assert.AreEqual((short)2, _decodeHeader.Version());
            Assert.AreEqual((short)0x01, _decodeHeader.Flags());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_SM, _decodeHeader.HeaderType());
            Assert.AreEqual(8, _decodeHeader.FrameLength());
        }

        [Test]
        public void ShouldReadAndWriteDataHeaderCorrectly()
        {
            _encodeDataHeader.Wrap(_aBuff);

            _encodeDataHeader.Version(1);
            _encodeDataHeader.Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
            _encodeDataHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            _encodeDataHeader.FrameLength(DataHeaderFlyweight.HEADER_LENGTH);
            _encodeDataHeader.SessionId(12345);
            _encodeDataHeader.StreamId(0x44332211);
            _encodeDataHeader.TermId(99887766);

            _decodeDataHeader.Wrap(_aBuff);
            Assert.AreEqual((short)1, _decodeDataHeader.Version());
            Assert.AreEqual(DataHeaderFlyweight.BEGIN_AND_END_FLAGS, _decodeDataHeader.Flags());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_DATA, _decodeDataHeader.HeaderType());
            Assert.AreEqual(DataHeaderFlyweight.HEADER_LENGTH, _decodeDataHeader.FrameLength());
            Assert.AreEqual(12345, _decodeDataHeader.SessionId());
            Assert.AreEqual(0x44332211, _decodeDataHeader.StreamId());
            Assert.AreEqual(99887766, _decodeDataHeader.TermId());
            Assert.AreEqual(DataHeaderFlyweight.HEADER_LENGTH, _decodeDataHeader.DataOffset());
        }
    }
}
