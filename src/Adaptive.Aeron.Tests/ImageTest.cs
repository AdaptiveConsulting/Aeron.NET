/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class ImageTest
    {
        private const int TERM_BUFFER_LENGTH = TERM_MIN_LENGTH;
        private static readonly int POSITION_BITS_TO_SHIFT = Integer.numberOfTrailingZeros(TERM_BUFFER_LENGTH);
        private static readonly sbyte[] DATA = new sbyte[36];

        static ImageTest()
        {
            for (int i = 0; i < DATA.Length; i++)
            {
                DATA[i] = (sbyte) i;
            }
        }

        private const long CORRELATION_ID = 0xC044E1AL;
        private const int SESSION_ID = 0x5E55101D;
        private const int STREAM_ID = 0xC400E;
        private const string SOURCE_IDENTITY = "ipc";
        private const int INITIAL_TERM_ID = 0xEE81D;
        private static readonly int MESSAGE_LENGTH = HEADER_LENGTH + DATA.Length;
        private static readonly int ALIGNED_FRAME_LENGTH = BitUtil.Align(MESSAGE_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);

        private readonly UnsafeBuffer RcvBuffer = new UnsafeBuffer(new byte[ALIGNED_FRAME_LENGTH]);
        private readonly DataHeaderFlyweight DataHeader = new DataHeaderFlyweight();
        private readonly IFragmentHandler MockFragmentHandler = mock(typeof (FragmentHandler));
        private readonly IControlledFragmentHandler MockControlledFragmentHandler = mock(typeof (ControlledFragmentHandler));
        private readonly IPosition Position = spy(new AtomicLongPosition());
        private readonly LogBuffers LogBuffers = mock(typeof (LogBuffers));
        private readonly ErrorHandler ErrorHandler = mock(typeof (ErrorHandler));
        private readonly Subscription Subscription = mock(typeof (Subscription));

        private UnsafeBuffer[] AtomicBuffers = new UnsafeBuffer[(PARTITION_COUNT*2) + 1];
        private UnsafeBuffer[] TermBuffers = new UnsafeBuffer[PARTITION_COUNT];

        [SetUp]
        public virtual void SetUp()
        {
            DataHeader.Wrap(RcvBuffer);

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                AtomicBuffers[i] = new UnsafeBuffer(allocateDirect(TERM_BUFFER_LENGTH));
                TermBuffers[i] = AtomicBuffers[i];

                AtomicBuffers[i + PARTITION_COUNT] = new UnsafeBuffer(allocateDirect(TERM_META_DATA_LENGTH));
            }

            AtomicBuffers[LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(allocateDirect(LOG_META_DATA_LENGTH));

            @when(LogBuffers.AtomicBuffers()).thenReturn(AtomicBuffers);
            @when(LogBuffers.TermLength()).thenReturn(TERM_BUFFER_LENGTH);
        }

        [Test]
        public virtual void ShouldHandleClosedImage()
        {
            Image image = CreateImage();

            image.ManagedResource();

            assertTrue(image.Closed);
            assertThat(image.Poll(MockFragmentHandler, int.MaxValue), @is(0));
        }

        [Test]
        public virtual void ShouldReportCorrectPositionOnReception()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            int messages = image.Poll(MockFragmentHandler, int.MaxValue);
            assertThat(messages, @is(1));

            verify(MockFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));

            InOrder inOrder = Mockito.inOrder(Position);
            inOrder.verify(Position).Ordered = initialPosition;
            inOrder.verify(Position).Ordered = initialPosition + ALIGNED_FRAME_LENGTH;
        }

        [Test]
        public virtual void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId()
        {
            const int initialMessageIndex = 5;
            int initialTermOffset = OffsetForFrame(initialMessageIndex);
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(initialMessageIndex));

            int messages = image.Poll(MockFragmentHandler, int.MaxValue);
            assertThat(messages, @is(1));

            verify(MockFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(initialTermOffset + HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));

            InOrder inOrder = Mockito.inOrder(Position);
            inOrder.verify(Position).Ordered = initialPosition;
            inOrder.verify(Position).Ordered = initialPosition + ALIGNED_FRAME_LENGTH;
        }

        [Test]
        public virtual void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId()
        {
            int activeTermId = INITIAL_TERM_ID + 1;
            const int initialMessageIndex = 5;
            int initialTermOffset = OffsetForFrame(initialMessageIndex);
            long initialPosition = LogBufferDescriptor.ComputePosition(activeTermId, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(activeTermId, OffsetForFrame(initialMessageIndex));

            int messages = image.Poll(MockFragmentHandler, int.MaxValue);
            assertThat(messages, @is(1));

            verify(MockFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(initialTermOffset + HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));

            InOrder inOrder = Mockito.inOrder(Position);
            inOrder.verify(Position).Ordered = initialPosition;
            inOrder.verify(Position).Ordered = initialPosition + ALIGNED_FRAME_LENGTH;
        }

        [Test]
        public virtual void ShouldPollNoFragmentsToControlledFragmentHandler()
        {
            Image image = CreateImage();
            int fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            assertThat(fragmentsRead, @is(0));
            verify(Position, never()).Ordered = anyLong();
            verify(MockControlledFragmentHandler, never()).onFragment(any(typeof (UnsafeBuffer)), anyInt(), anyInt(), any(typeof (Header)));
        }

        [Test]
        public virtual void ShouldPollOneFragmentToControlledFragmentHandlerOnContinue()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            @when(MockControlledFragmentHandler(any(typeof (DirectBuffer)), anyInt(), anyInt(), any(typeof (Header)))).thenReturn(ControlledFragmentHandler_Action.CONTINUE);

            int fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            assertThat(fragmentsRead, @is(1));

            InOrder inOrder = Mockito.inOrder(Position, MockControlledFragmentHandler);
            inOrder.verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
            inOrder.verify(Position).Ordered = initialPosition + ALIGNED_FRAME_LENGTH;
        }

        [Test]
        public virtual void ShouldNotPollOneFragmentToControlledFragmentHandlerOnAbort()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            @when(MockControlledFragmentHandler(any(typeof (DirectBuffer)), anyInt(), anyInt(), any(typeof (Header)))).thenReturn(ControlledFragmentHandler_Action.ABORT);

            int fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            assertThat(fragmentsRead, @is(0));
            assertThat(image.Position(), @is(initialPosition));

            verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
        }

        [Test]
        public virtual void ShouldPollOneFragmentToControlledFragmentHandlerOnBreak()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            @when(MockControlledFragmentHandler(any(typeof (DirectBuffer)), anyInt(), anyInt(), any(typeof (Header)))).thenReturn(ControlledFragmentHandler_Action.BREAK);

            int fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            assertThat(fragmentsRead, @is(1));

            InOrder inOrder = Mockito.inOrder(Position, MockControlledFragmentHandler);
            inOrder.verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
            inOrder.verify(Position).Ordered = initialPosition + ALIGNED_FRAME_LENGTH;
        }

        [Test]
        public virtual void ShouldPollFragmentsToControlledFragmentHandlerOnCommit()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            @when(MockControlledFragmentHandler(any(typeof (DirectBuffer)), anyInt(), anyInt(), any(typeof (Header)))).thenReturn(ControlledFragmentHandler_Action.COMMIT);

            int fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            assertThat(fragmentsRead, @is(2));

            InOrder inOrder = Mockito.inOrder(Position, MockControlledFragmentHandler);
            inOrder.verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
            inOrder.verify(Position).Ordered = initialPosition + ALIGNED_FRAME_LENGTH;

            inOrder.verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
            inOrder.verify(Position).Ordered = initialPosition + (ALIGNED_FRAME_LENGTH*2);
        }


        [Test]
        public virtual void ShouldPollFragmentsToControlledFragmentHandlerOnContinue()
        {
            long initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.Ordered = initialPosition;
            Image image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            @when(MockControlledFragmentHandler(any(typeof (DirectBuffer)), anyInt(), anyInt(), any(typeof (Header)))).thenReturn(ControlledFragmentHandler_Action.CONTINUE);

            int fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            assertThat(fragmentsRead, @is(2));

            InOrder inOrder = Mockito.inOrder(Position, MockControlledFragmentHandler);
            inOrder.verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
            inOrder.verify(MockControlledFragmentHandler).onFragment(any(typeof (UnsafeBuffer)), eq(ALIGNED_FRAME_LENGTH + HEADER_LENGTH), eq(DATA.Length), any(typeof (Header)));
            inOrder.verify(Position).Ordered = initialPosition + (ALIGNED_FRAME_LENGTH*2);
        }

        private Image CreateImage()
        {
            return new Image(Subscription, SESSION_ID, Position, LogBuffers, ErrorHandler, SOURCE_IDENTITY, CORRELATION_ID);
        }

        private void InsertDataFrame(int activeTermId, int termOffset)
        {
            DataHeader.TermId(INITIAL_TERM_ID).StreamId(STREAM_ID).SessionId(SESSION_ID).TermOffset(termOffset).FrameLength(DATA.Length + HEADER_LENGTH).HeaderType(HeaderFlyweight.HDR_TYPE_DATA).Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS).Version(HeaderFlyweight.CURRENT_VERSION);

            RcvBuffer.putBytes(DataHeader.DataOffset(), DATA);

            int activeIndex = LogBufferDescriptor.IndexByTerm(INITIAL_TERM_ID, activeTermId);
            TermRebuilder.Insert(TermBuffers[activeIndex], termOffset, RcvBuffer, ALIGNED_FRAME_LENGTH);
        }

        private static int OffsetForFrame(int index)
        {
            return index*ALIGNED_FRAME_LENGTH;
        }
    }
}