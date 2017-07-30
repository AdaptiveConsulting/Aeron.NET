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
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class ImageTest
    {
        private const int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
        private static readonly int POSITION_BITS_TO_SHIFT = IntUtil.NumberOfTrailingZeros(TERM_BUFFER_LENGTH);
        private static readonly byte[] DATA = new byte[36];

        static ImageTest()
        {
            for (var i = 0; i < DATA.Length; i++)
            {
                DATA[i] = (byte) i;
            }
        }

        private const long CORRELATION_ID = 0xC044E1AL;
        private const int SESSION_ID = 0x5E55101D;
        private const int STREAM_ID = 0xC400E;
        private const string SOURCE_IDENTITY = "ipc";
        private const int INITIAL_TERM_ID = 0xEE81D;
        private static readonly int MESSAGE_LENGTH = DataHeaderFlyweight.HEADER_LENGTH + DATA.Length;
        private static readonly int ALIGNED_FRAME_LENGTH = BitUtil.Align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);

        private UnsafeBuffer RcvBuffer;
        private DataHeaderFlyweight DataHeader;
        private FragmentHandler MockFragmentHandler;
        private IControlledFragmentHandler MockControlledFragmentHandler;
        private IPosition Position;
        private LogBuffers LogBuffers;
        private ErrorHandler ErrorHandler;
        private Subscription Subscription;

        private UnsafeBuffer[] TermBuffers;

        [SetUp]
        public void SetUp()
        {
            RcvBuffer = new UnsafeBuffer(new byte[ALIGNED_FRAME_LENGTH]);
            DataHeader = new DataHeaderFlyweight();
            MockFragmentHandler = A.Fake<FragmentHandler>();
            MockControlledFragmentHandler = A.Fake<IControlledFragmentHandler>();
            Position = A.Fake<IPosition>(options => options.Wrapping(new AtomicLongPosition()));
            LogBuffers = A.Fake<LogBuffers>();
            ErrorHandler = A.Fake<ErrorHandler>();
            Subscription = A.Fake<Subscription>();

            TermBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];

            DataHeader.Wrap(RcvBuffer);

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                TermBuffers[i] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
            }

            var logMetaDataBuffer = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);
            
            A.CallTo(() => LogBuffers.TermBuffers()).Returns(TermBuffers);
            A.CallTo(() => LogBuffers.TermLength()).Returns(TERM_BUFFER_LENGTH);
            A.CallTo(() => LogBuffers.MetaDataBuffer()).Returns(logMetaDataBuffer);
        }

        [Test]
        public void ShouldHandleClosedImage()
        {
            var image = CreateImage();

            image.ManagedResource();

            Assert.True(image.Closed);
            Assert.AreEqual(0, image.Poll(MockFragmentHandler, int.MaxValue));
        }

        [Test]
        public void ShouldReportCorrectPositionOnReception()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            var messages = image.Poll(MockFragmentHandler, int.MaxValue);
            Assert.AreEqual(1, messages);

            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();

            A.CallTo(() => Position.SetOrdered(initialPosition)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened()
                );
        }

        [Test]
        public void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId()
        {
            const int initialMessageIndex = 5;
            var initialTermOffset = OffsetForFrame(initialMessageIndex);
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(initialMessageIndex));

            var messages = image.Poll(MockFragmentHandler, int.MaxValue);
            Assert.AreEqual(1, messages);
            
            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, initialTermOffset + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();

            A.CallTo(() => Position.SetOrdered(initialPosition)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened()
                );
        }

        [Test]
        public void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId()
        {
            var activeTermId = INITIAL_TERM_ID + 1;
            const int initialMessageIndex = 5;
            var initialTermOffset = OffsetForFrame(initialMessageIndex);
            var initialPosition = LogBufferDescriptor.ComputePosition(activeTermId, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(activeTermId, OffsetForFrame(initialMessageIndex));

            var messages = image.Poll(MockFragmentHandler, int.MaxValue);
            Assert.AreEqual(1, messages);

            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, initialTermOffset + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();

            A.CallTo(() => Position.SetOrdered(initialPosition)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened()
                );
        }

        [Test]
        public void ShouldPollNoFragmentsToControlledFragmentHandler()
        {
            var image = CreateImage();
            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(0, fragmentsRead);

            A.CallTo(() => Position.SetOrdered(A<long>._)).MustNotHaveHappened();
            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public void ShouldPollOneFragmentToControlledFragmentHandlerOnContinue()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(1, fragmentsRead);
            
            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened());
        }

        [Test]
        public void ShouldUpdatePositionOnRethrownExceptionInControlledPoll()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).Throws(new Exception());

            A.CallTo(ErrorHandler).Throws(new Exception());

            bool thrown = false;

            try
            {
                image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);
            }
            catch (Exception)
            {
                thrown = true;
            }

            Assert.True(thrown);
            Assert.AreEqual(initialPosition + ALIGNED_FRAME_LENGTH, image.Position());

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public void ShouldUpdatePositionOnRethrownExceptionInPoll()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).Throws(new Exception());

            A.CallTo(ErrorHandler).Throws(new Exception());

            bool thrown = false;

            try
            {
                image.Poll(MockFragmentHandler, int.MaxValue);
            }
            catch (Exception)
            {
                thrown = true;
            }

            Assert.True(thrown);
            Assert.AreEqual(initialPosition + ALIGNED_FRAME_LENGTH, image.Position());

            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();
        }


        [Test]
        public void ShouldNotPollOneFragmentToControlledFragmentHandlerOnAbort()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.ABORT);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(0, fragmentsRead);
            Assert.AreEqual(initialPosition, image.Position());

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public void ShouldPollOneFragmentToControlledFragmentHandlerOnBreak()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.BREAK);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(1, fragmentsRead);
            
            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened());
        }

        [Test]
        public void ShouldPollFragmentsToControlledFragmentHandlerOnCommit()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.COMMIT);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(2, fragmentsRead);

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened()
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened())
                .Then(A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, ALIGNED_FRAME_LENGTH + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH*2)).MustHaveHappened());
        }

        [Test]
        public void ShouldUpdatePositionToEndOfCommittedFragmentOnCommit()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(2));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._))
                .ReturnsNextFromSequence(ControlledFragmentHandlerAction.CONTINUE, ControlledFragmentHandlerAction.COMMIT, ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(3, fragmentsRead);

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened()
                .Then(A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, ALIGNED_FRAME_LENGTH + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH * 2)).MustHaveHappened())
                .Then(A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, 2 * ALIGNED_FRAME_LENGTH + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH * 3)).MustHaveHappened());
        }

        [Test]
        public void ShouldPollFragmentsToControlledFragmentHandlerOnContinue()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));
            
            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(2, fragmentsRead);
            
            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened()
                .Then(A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<IDirectBuffer>._, ALIGNED_FRAME_LENGTH + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH * 2)).MustHaveHappened());
        }

        private Image CreateImage()
        {
            return new Image(Subscription, SESSION_ID, Position, LogBuffers, ErrorHandler, SOURCE_IDENTITY, CORRELATION_ID);
        }

        private void InsertDataFrame(int activeTermId, int termOffset)
        {
            DataHeader.TermId(INITIAL_TERM_ID).StreamId(STREAM_ID).SessionId(SESSION_ID).TermOffset(termOffset).FrameLength(DATA.Length + DataHeaderFlyweight.HEADER_LENGTH).HeaderType(HeaderFlyweight.HDR_TYPE_DATA).Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS).Version(HeaderFlyweight.CURRENT_VERSION);

            RcvBuffer.PutBytes(DataHeader.DataOffset(), DATA);

            var activeIndex = LogBufferDescriptor.IndexByTerm(INITIAL_TERM_ID, activeTermId);
            TermRebuilder.Insert(TermBuffers[activeIndex], termOffset, RcvBuffer, ALIGNED_FRAME_LENGTH);
        }

        private static int OffsetForFrame(int index)
        {
            return index*ALIGNED_FRAME_LENGTH;
        }
    }
}