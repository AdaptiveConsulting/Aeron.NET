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

using System;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class FragmentAssemblerTest
    {
        private const int SESSION_ID = 777;
        private const int INITIAL_TERM_ID = 3;

        private IFragmentHandler delegateFragmentHandler;
        private Header header;
        private FragmentAssembler assembler;
        private DataHeaderFlyweight headerFlyweight;

        [SetUp]
        public void SetUp()
        {
            delegateFragmentHandler = A.Fake<IFragmentHandler>();
            assembler = new FragmentAssembler(delegateFragmentHandler);
            header = A.Fake<Header>(x => x.Wrapping(new Header(INITIAL_TERM_ID, LogBufferDescriptor.TERM_MIN_LENGTH)));

            UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
            headerFlyweight = new DataHeaderFlyweight();
            headerFlyweight.Wrap(buffer, 16, DataHeaderFlyweight.HEADER_LENGTH);
            header.SetBuffer(buffer, 16);
            headerFlyweight.SessionId(SESSION_ID);
        }

        [Test]
        public void ShouldPassThroughUnfragmentedMessage()
        {
            headerFlyweight.Flags(FrameDescriptor.UNFRAGMENTED);
            
            var srcBuffer = new UnsafeBuffer(new byte[128]);
            const int offset = 8;
            const int length = 32;

            assembler.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler.OnFragment(srcBuffer, offset, length, header))
                .MustHaveHappened(1, Times.Exactly);
        }

        [Test]
        public void ShouldAssembleTwoPartMessage()
        {
            A.CallTo(() => header.Flags).ReturnsNextFromSequence(FrameDescriptor.BEGIN_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG);

            var srcBuffer = new UnsafeBuffer(new byte[1024 + (2 * DataHeaderFlyweight.HEADER_LENGTH)]);
            var length = 512;

            int offset = DataHeaderFlyweight.HEADER_LENGTH;
            headerFlyweight.Flags(FrameDescriptor.BEGIN_FRAG_FLAG);
            assembler.OnFragment(srcBuffer, offset, length, header);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            headerFlyweight.Flags(FrameDescriptor.END_FRAG_FLAG);
            assembler.OnFragment(srcBuffer, offset, length, header);

            Func<Header, bool> headerAssertion = capturedHeader => capturedHeader.SessionId == SESSION_ID &&
                                                                   capturedHeader.Flags == FrameDescriptor.UNFRAGMENTED;
            
            A.CallTo(() => delegateFragmentHandler.OnFragment(
                A<IDirectBuffer>._,
                0,
                length * 2,
                A<Header>.That.Matches(headerAssertion, "header")))
                .MustHaveHappened(1, Times.Exactly);
        }

        [Test]
        public void ShouldAssembleFourPartMessage()
        {
            var srcBuffer = new UnsafeBuffer(new byte[1024 + + (4 * DataHeaderFlyweight.HEADER_LENGTH)]);
            var length = 256;

            int offset = DataHeaderFlyweight.HEADER_LENGTH;
            headerFlyweight.Flags(FrameDescriptor.BEGIN_FRAG_FLAG);
            assembler.OnFragment(srcBuffer, offset, length, header);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            headerFlyweight.Flags(0);
            assembler.OnFragment(srcBuffer, offset, length, header);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            headerFlyweight.Flags(0);
            assembler.OnFragment(srcBuffer, offset, length, header);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            headerFlyweight.Flags(FrameDescriptor.END_FRAG_FLAG);
            assembler.OnFragment(srcBuffer, offset, length, header);

            Func<Header, bool> headerAssertion = capturedHeader => capturedHeader.SessionId == SESSION_ID &&
                                                                   capturedHeader.Flags == FrameDescriptor.UNFRAGMENTED;

            A.CallTo(() => delegateFragmentHandler.OnFragment(
                A<IDirectBuffer>._,
                0,
                length * 4,
                A<Header>.That.Matches(headerAssertion, "header")))
                .MustHaveHappened(1, Times.Exactly);
        }

        [Test]
        public void ShouldFreeSessionBuffer()
        {
            A.CallTo(() => header.Flags).ReturnsNextFromSequence(FrameDescriptor.BEGIN_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            srcBuffer.SetMemory(0, length, 65);
            srcBuffer.SetMemory(length, length, 66);

            Assert.False(assembler.FreeSessionBuffer(SESSION_ID));

            assembler.OnFragment(srcBuffer, offset, length, header);
            assembler.OnFragment(srcBuffer, length, length, header);

            Assert.True(assembler.FreeSessionBuffer(SESSION_ID));
            Assert.False(assembler.FreeSessionBuffer(SESSION_ID));
        }

        [Test]
        public void ShouldDoNotingIfEndArrivesWithoutBegin()
        {
            A.CallTo(() => header.Flags).Returns(FrameDescriptor.END_FRAG_FLAG);
            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            assembler.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldDoNotingIfMidArrivesWithoutBegin()
        {
            A.CallTo(() => header.Flags).Returns(FrameDescriptor.END_FRAG_FLAG);
            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            assembler.OnFragment(srcBuffer, offset, length, header);
            assembler.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldSkipOverMessagesWithLoss()
        {
            var srcBuffer = new UnsafeBuffer(new byte[2048]);
            var length = 256;

            int offset = DataHeaderFlyweight.HEADER_LENGTH;
            headerFlyweight.Flags(FrameDescriptor.BEGIN_FRAG_FLAG);
            assembler.OnFragment(srcBuffer, offset, length, header);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            headerFlyweight.Flags(FrameDescriptor.END_FRAG_FLAG);
            assembler.OnFragment(srcBuffer, offset, length, header);
            offset = BitUtil.Align(offset + length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
            headerFlyweight.Flags(FrameDescriptor.UNFRAGMENTED);
            assembler.OnFragment(srcBuffer, offset, length, header);

            Func<Header, bool> headerAssertion = capturedHeader => capturedHeader.SessionId == SESSION_ID &&
                                                                   capturedHeader.Flags ==
                                                                   FrameDescriptor.UNFRAGMENTED;

            A.CallTo(() => delegateFragmentHandler.OnFragment(
                    A<IDirectBuffer>._,
                    offset,
                    length,
                    A<Header>.That.Matches(headerAssertion, "header")))
                .MustHaveHappened(1, Times.Exactly);
        }
    }
}