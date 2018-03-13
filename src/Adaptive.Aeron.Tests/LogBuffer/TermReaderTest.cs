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

using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests.LogBuffer
{
    [TestFixture]
    public class TermReaderTest
    {
        private static readonly int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
        private static readonly int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
        private const int INITIAL_TERM_ID = 7;

        private Header header;
        private UnsafeBuffer termBuffer;
        private ErrorHandler errorHandler;
        private IFragmentHandler handler;
        private IPosition subscriberPosition;

        [SetUp]
        public void SetUp()
        {
            header = new Header(INITIAL_TERM_ID, TERM_BUFFER_CAPACITY);
            termBuffer = A.Fake<UnsafeBuffer>();
            errorHandler = A.Fake<ErrorHandler>();
            handler = A.Fake<IFragmentHandler>();
            subscriberPosition = A.Fake<IPosition>();

            A.CallTo(() => termBuffer.Capacity).Returns(TERM_BUFFER_CAPACITY);
        }

        [Test]
        public void ShouldReadFirstMessage()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => termBuffer.GetIntVolatile(0))
                .Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(FrameDescriptor.TypeOffset(0)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(termBuffer, termOffset, handler, int.MaxValue, header, errorHandler, 0, subscriberPosition);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(1));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => subscriberPosition.SetOrdered(alignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldNotReadPastTail()
        {
            const int termOffset = 0;

            int readOutcome = TermReader.Read(termBuffer, termOffset, handler, int.MaxValue, header, errorHandler, 0, subscriberPosition);
            Assert.That(TermReader.FragmentsRead(readOutcome), Is.EqualTo(0));
            Assert.That(TermReader.Offset(readOutcome), Is.EqualTo(termOffset));

            A.CallTo(() => subscriberPosition.SetOrdered(A<long>._)).MustNotHaveHappened();
            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened();
            A.CallTo(() => handler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldReadOneLimitedMessage()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => termBuffer.GetIntVolatile(A<int>._))
                .Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(A<int>._))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(termBuffer, termOffset, handler, 1, header, errorHandler, 0, subscriberPosition);
            Assert.That(readOutcome, Is.EqualTo(1));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => subscriberPosition.SetOrdered(alignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldReadMultipleMessages()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => termBuffer.GetIntVolatile(0)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetIntVolatile(alignedFrameLength)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(A<int>._)).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(termBuffer, termOffset, handler, int.MaxValue, header, errorHandler, 0, subscriberPosition);
            Assert.That(readOutcome, Is.EqualTo(2));

            A.CallTo(() => termBuffer.GetIntVolatile(0)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => termBuffer.GetIntVolatile(alignedFrameLength)).MustHaveHappened())
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, alignedFrameLength + HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => subscriberPosition.SetOrdered(alignedFrameLength * 2)).MustHaveHappened());
        }

        [Test]
        public void ShouldReadLastMessage()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;
            
            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(FrameDescriptor.TypeOffset(frameOffset))).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(termBuffer, frameOffset, handler, int.MaxValue, header, errorHandler, 0, subscriberPosition);
            Assert.That(readOutcome, Is.EqualTo(1));
            
            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).MustHaveHappened()
                .Then(A.CallTo(() => handler.OnFragment(termBuffer, frameOffset + HEADER_LENGTH, msgLength, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => subscriberPosition.SetOrdered(alignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldNotReadLastMessageWhenPadding()
        {
            const int msgLength = 1;
            int frameLength = HEADER_LENGTH + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).Returns(frameLength);
            A.CallTo(() => termBuffer.GetShort(FrameDescriptor.TypeOffset(frameOffset))).Returns((short)FrameDescriptor.PADDING_FRAME_TYPE);
            
            int readOutcome = TermReader.Read(termBuffer, frameOffset, handler, int.MaxValue, header, errorHandler, 0, subscriberPosition);
            Assert.That(readOutcome, Is.EqualTo(0));

            A.CallTo(() => termBuffer.GetIntVolatile(frameOffset)).MustHaveHappened()
                .Then(A.CallTo(() => subscriberPosition.SetOrdered(alignedFrameLength)).MustHaveHappened());

            A.CallTo(() => handler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }
    }
}