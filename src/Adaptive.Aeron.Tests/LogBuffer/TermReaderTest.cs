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
        private static readonly int TermBufferCapacity = LogBufferDescriptor.TERM_MIN_LENGTH;
        private static readonly int HeaderLength = DataHeaderFlyweight.HEADER_LENGTH;
        private const int InitialTermId = 7;
        private static readonly int PositionBitsToShift = LogBufferDescriptor.PositionBitsToShift(TermBufferCapacity);

        private Header _header;
        private UnsafeBuffer _termBuffer;
        private IErrorHandler _errorHandler;
        private IFragmentHandler _handler;
        private IPosition _subscriberPosition;

        [SetUp]
        public void SetUp()
        {
            _header = new Header(InitialTermId, TermBufferCapacity);
            _termBuffer = A.Fake<UnsafeBuffer>();
            _errorHandler = A.Fake<IErrorHandler>();
            _handler = A.Fake<IFragmentHandler>();
            _subscriberPosition = A.Fake<IPosition>();

            A.CallTo(() => _termBuffer.Capacity).Returns(TermBufferCapacity);
        }

        [Test]
        public void ShouldReadFirstMessage()
        {
            const int msgLength = 1;
            int frameLength = HeaderLength + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => _termBuffer.GetIntVolatile(0)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(0)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(
                _termBuffer,
                termOffset,
                _handler,
                int.MaxValue,
                _header,
                _errorHandler,
                0,
                _subscriberPosition
            );
            Assert.AreEqual(1, TermReader.FragmentsRead(readOutcome));

            A.CallTo(() => _termBuffer.GetIntVolatile(0))
                .MustHaveHappened()
                .Then(
                    A.CallTo(() => _handler.OnFragment(_termBuffer, HeaderLength, msgLength, A<Header>._))
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _subscriberPosition.SetRelease(alignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldNotReadPastTail()
        {
            const int termOffset = 0;

            int readOutcome = TermReader.Read(
                _termBuffer,
                termOffset,
                _handler,
                int.MaxValue,
                _header,
                _errorHandler,
                0,
                _subscriberPosition
            );

            Assert.AreEqual(0, readOutcome);

            A.CallTo(() => _subscriberPosition.SetRelease(A<long>._)).MustNotHaveHappened();
            A.CallTo(() => _termBuffer.GetIntVolatile(0)).MustHaveHappened();
            A.CallTo(() => _handler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._))
                .MustNotHaveHappened();
        }

        [Test]
        public void ShouldReadOneLimitedMessage()
        {
            const int msgLength = 1;
            int frameLength = HeaderLength + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => _termBuffer.GetIntVolatile(A<int>._)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetShort(A<int>._)).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(
                _termBuffer,
                termOffset,
                _handler,
                1,
                _header,
                _errorHandler,
                0,
                _subscriberPosition
            );
            Assert.AreEqual(1, readOutcome);

            A.CallTo(() => _termBuffer.GetIntVolatile(0))
                .MustHaveHappened()
                .Then(
                    A.CallTo(() => _handler.OnFragment(_termBuffer, HeaderLength, msgLength, A<Header>._))
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _subscriberPosition.SetRelease(alignedFrameLength)).MustHaveHappened());
        }

        [Test]
        public void ShouldReadMultipleMessages()
        {
            const int msgLength = 1;
            int frameLength = HeaderLength + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            const int termOffset = 0;

            A.CallTo(() => _termBuffer.GetIntVolatile(0)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetIntVolatile(alignedFrameLength)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetShort(A<int>._)).Returns((short)HeaderFlyweight.HDR_TYPE_DATA);

            int readOutcome = TermReader.Read(
                _termBuffer,
                termOffset,
                _handler,
                int.MaxValue,
                _header,
                _errorHandler,
                0,
                _subscriberPosition
            );
            Assert.AreEqual(2, readOutcome);

            A.CallTo(() => _termBuffer.GetIntVolatile(0))
                .MustHaveHappened()
                .Then(
                    A.CallTo(() => _handler.OnFragment(_termBuffer, HeaderLength, msgLength, A<Header>._))
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _termBuffer.GetIntVolatile(alignedFrameLength)).MustHaveHappened())
                .Then(
                    A.CallTo(() =>
                            _handler.OnFragment(_termBuffer, alignedFrameLength + HeaderLength, msgLength, A<Header>._)
                        )
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _subscriberPosition.SetRelease(alignedFrameLength * 2L)).MustHaveHappened());
        }

        [Test]
        public void ShouldReadLastMessage()
        {
            const int msgLength = 1;
            int frameLength = HeaderLength + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int frameOffset = TermBufferCapacity - alignedFrameLength;
            long startingPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                frameOffset,
                PositionBitsToShift,
                InitialTermId
            );

            A.CallTo(() => _termBuffer.GetIntVolatile(frameOffset)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(frameOffset)))
                .Returns((short)HeaderFlyweight.HDR_TYPE_DATA);
            A.CallTo(() => _subscriberPosition.GetVolatile()).Returns(startingPosition);

            int readOutcome = TermReader.Read(
                _termBuffer,
                frameOffset,
                _handler,
                int.MaxValue,
                _header,
                _errorHandler,
                startingPosition,
                _subscriberPosition
            );

            Assert.AreEqual(1, readOutcome);

            A.CallTo(() => _termBuffer.GetIntVolatile(frameOffset))
                .MustHaveHappened()
                .Then(
                    A.CallTo(() => _handler.OnFragment(_termBuffer, frameOffset + HeaderLength, msgLength, A<Header>._))
                        .MustHaveHappened()
                )
                .Then(A.CallTo(() => _subscriberPosition.SetRelease(TermBufferCapacity)).MustHaveHappened());
        }

        [Test]
        public void ShouldNotReadLastMessageWhenPadding()
        {
            const int msgLength = 1;
            int frameLength = HeaderLength + msgLength;
            int alignedFrameLength = BitUtil.Align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            int frameOffset = TermBufferCapacity - alignedFrameLength;
            long startingPosition = LogBufferDescriptor.ComputePosition(
                InitialTermId,
                frameOffset,
                PositionBitsToShift,
                InitialTermId
            );

            A.CallTo(() => _termBuffer.GetIntVolatile(frameOffset)).Returns(frameLength);
            A.CallTo(() => _termBuffer.GetShort(FrameDescriptor.TypeOffset(frameOffset)))
                .Returns((short)FrameDescriptor.PADDING_FRAME_TYPE);
            A.CallTo(() => _subscriberPosition.GetVolatile()).Returns(startingPosition);

            int readOutcome = TermReader.Read(
                _termBuffer,
                frameOffset,
                _handler,
                int.MaxValue,
                _header,
                _errorHandler,
                startingPosition,
                _subscriberPosition
            );

            Assert.AreEqual(0, readOutcome);

            A.CallTo(() => _termBuffer.GetIntVolatile(frameOffset))
                .MustHaveHappened()
                .Then(A.CallTo(() => _subscriberPosition.SetRelease(TermBufferCapacity)).MustHaveHappened());

            A.CallTo(() => _handler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._))
                .MustNotHaveHappened();
        }
    }
}
