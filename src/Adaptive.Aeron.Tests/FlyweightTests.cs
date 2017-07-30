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
            Assert.That(buffer[0], Is.EqualTo((byte)0x08));
            Assert.That(buffer[1], Is.EqualTo((byte)0x00));
            Assert.That(buffer[2], Is.EqualTo((byte)0x00));
            Assert.That(buffer[3], Is.EqualTo((byte)0x00));
            Assert.That(buffer[4], Is.EqualTo((byte)0x01));
            Assert.That(buffer[5], Is.EqualTo(unchecked((byte)0xC0)));
            Assert.That(buffer[6], Is.EqualTo((byte)HeaderFlyweight.HDR_TYPE_DATA));
            Assert.That(buffer[7], Is.EqualTo((byte)0x00));
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
            Assert.That(decodeHeader.Version(), Is.EqualTo((short)1));
            Assert.That(decodeHeader.HeaderType(), Is.EqualTo(HeaderFlyweight.HDR_TYPE_DATA));
            Assert.That(decodeHeader.FrameLength(), Is.EqualTo(8));
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
            Assert.That(decodeHeader.Version(), Is.EqualTo((short)1));
            Assert.That(decodeHeader.Flags(), Is.EqualTo((short)0));
            Assert.That(decodeHeader.HeaderType(), Is.EqualTo(HeaderFlyweight.HDR_TYPE_DATA));
            Assert.That(decodeHeader.FrameLength(), Is.EqualTo(8));

            decodeHeader.Wrap(aBuff, 8, aBuff.Capacity - 8);
            Assert.That(decodeHeader.Version(), Is.EqualTo((short)2));
            Assert.That(decodeHeader.Flags(), Is.EqualTo((short)0x01));
            Assert.That(decodeHeader.HeaderType(), Is.EqualTo(HeaderFlyweight.HDR_TYPE_SM));
            Assert.That(decodeHeader.FrameLength(), Is.EqualTo(8));
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
            Assert.That(decodeDataHeader.Version(), Is.EqualTo((short)1));
            Assert.That(decodeDataHeader.Flags(), Is.EqualTo(DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
            Assert.That(decodeDataHeader.HeaderType(), Is.EqualTo(HeaderFlyweight.HDR_TYPE_DATA));
            Assert.That(decodeDataHeader.FrameLength(), Is.EqualTo(DataHeaderFlyweight.HEADER_LENGTH));
            Assert.That(decodeDataHeader.SessionId(), Is.EqualTo(12345));
            Assert.That(decodeDataHeader.StreamId(), Is.EqualTo(0x44332211));
            Assert.That(decodeDataHeader.TermId(), Is.EqualTo(99887766));
            Assert.That(decodeDataHeader.DataOffset(), Is.EqualTo(DataHeaderFlyweight.HEADER_LENGTH));
        }
    }
}