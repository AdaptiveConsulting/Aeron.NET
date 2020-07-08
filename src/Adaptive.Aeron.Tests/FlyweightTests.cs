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
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class FlyweightTest
    {
        private bool InstanceFieldsInitialized = false;

        public FlyweightTest()
        {
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }
        }

        private void InitializeInstanceFields()
        {
            aBuff = new UnsafeBuffer(buffer);
        }

        private readonly byte[] buffer = new byte[512];

        private UnsafeBuffer aBuff;
        private readonly HeaderFlyweight encodeHeader = new HeaderFlyweight();
        private readonly HeaderFlyweight decodeHeader = new HeaderFlyweight();
        private readonly DataHeaderFlyweight encodeDataHeader = new DataHeaderFlyweight();
        private readonly DataHeaderFlyweight decodeDataHeader = new DataHeaderFlyweight();

        [Test]
        public void ShouldWriteCorrectValuesForGenericHeaderFields()
        {
            encodeHeader.Wrap(aBuff);

            encodeHeader.Version((short)1);
            encodeHeader.Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
            encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            encodeHeader.FrameLength(8);

            // little endian
            Assert.AreEqual((byte)0x08, buffer[0]);
            Assert.AreEqual((byte)0x00, buffer[1]);
            Assert.AreEqual((byte)0x00, buffer[2]);
            Assert.AreEqual((byte)0x00, buffer[3]);
            Assert.AreEqual((byte)0x01, buffer[4]);
            Assert.AreEqual((byte)0xC0, buffer[5]);
            Assert.AreEqual((byte)HeaderFlyweight.HDR_TYPE_DATA, buffer[6]);
            Assert.AreEqual((byte)0x00, buffer[7]);
        }

        [Test]
        public void ShouldReadWhatIsWrittenToGenericHeaderFields()
        {
            encodeHeader.Wrap(aBuff);

            encodeHeader.Version(1);
            encodeHeader.Flags(0);
            encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            encodeHeader.FrameLength(8);

            decodeHeader.Wrap(aBuff);
            Assert.AreEqual((short)1, decodeHeader.Version());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_DATA, decodeHeader.HeaderType());
            Assert.AreEqual(8, decodeHeader.FrameLength());
        }

        [Test]
        public void ShouldWriteAndReadMultipleFramesCorrectly()
        {
            encodeHeader.Wrap(aBuff);

            encodeHeader.Version(1);
            encodeHeader.Flags(0);
            encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            encodeHeader.FrameLength(8);

            encodeHeader.Wrap(aBuff, 8, aBuff.Capacity - 8);
            encodeHeader.Version(2);
            encodeHeader.Flags(0x01);
            encodeHeader.HeaderType(HeaderFlyweight.HDR_TYPE_SM);
            encodeHeader.FrameLength(8);

            decodeHeader.Wrap(aBuff);
            Assert.AreEqual((short)1, decodeHeader.Version());
            Assert.AreEqual((short)0, decodeHeader.Flags());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_DATA, decodeHeader.HeaderType());
            Assert.AreEqual(8, decodeHeader.FrameLength());

            decodeHeader.Wrap(aBuff, 8, aBuff.Capacity - 8);
            Assert.AreEqual((short)2, decodeHeader.Version());
            Assert.AreEqual((short)0x01, decodeHeader.Flags());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_SM, decodeHeader.HeaderType());
            Assert.AreEqual(8, decodeHeader.FrameLength());
        }

        [Test]
        public void ShouldReadAndWriteDataHeaderCorrectly()
        {
            encodeDataHeader.Wrap(aBuff);

            encodeDataHeader.Version(1);
            encodeDataHeader.Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS);
            encodeDataHeader.HeaderType(HeaderFlyweight.HDR_TYPE_DATA);
            encodeDataHeader.FrameLength(DataHeaderFlyweight.HEADER_LENGTH);
            encodeDataHeader.SessionId(12345);
            encodeDataHeader.StreamId(0x44332211);
            encodeDataHeader.TermId(99887766);

            decodeDataHeader.Wrap(aBuff);
            Assert.AreEqual((short)1, decodeDataHeader.Version());
            Assert.AreEqual(DataHeaderFlyweight.BEGIN_AND_END_FLAGS, decodeDataHeader.Flags());
            Assert.AreEqual(HeaderFlyweight.HDR_TYPE_DATA, decodeDataHeader.HeaderType());
            Assert.AreEqual(DataHeaderFlyweight.HEADER_LENGTH, decodeDataHeader.FrameLength());
            Assert.AreEqual(12345, decodeDataHeader.SessionId());
            Assert.AreEqual(0x44332211, decodeDataHeader.StreamId());
            Assert.AreEqual(99887766, decodeDataHeader.TermId());
            Assert.AreEqual(DataHeaderFlyweight.HEADER_LENGTH, decodeDataHeader.DataOffset());
        }
    }
}