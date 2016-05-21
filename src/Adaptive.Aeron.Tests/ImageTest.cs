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

        private UnsafeBuffer[] AtomicBuffers;
        private UnsafeBuffer[] TermBuffers;

        [SetUp]
        public virtual void SetUp()
        {
            RcvBuffer = new UnsafeBuffer(new byte[ALIGNED_FRAME_LENGTH]);
            DataHeader = new DataHeaderFlyweight();
            MockFragmentHandler = A.Fake<FragmentHandler>();
            MockControlledFragmentHandler = A.Fake<IControlledFragmentHandler>();
            Position = A.Fake<IPosition>(options => options.Wrapping(new AtomicLongPosition()));
            LogBuffers = A.Fake<LogBuffers>();
            ErrorHandler = A.Fake<ErrorHandler>();
            Subscription = A.Fake<Subscription>();

            AtomicBuffers = new UnsafeBuffer[(LogBufferDescriptor.PARTITION_COUNT * 2) + 1];
            TermBuffers = new UnsafeBuffer[LogBufferDescriptor.PARTITION_COUNT];

            DataHeader.Wrap(RcvBuffer);

            for (var i = 0; i < LogBufferDescriptor.PARTITION_COUNT; i++)
            {
                AtomicBuffers[i] = new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]);
                TermBuffers[i] = AtomicBuffers[i];

                AtomicBuffers[i + LogBufferDescriptor.PARTITION_COUNT] = new UnsafeBuffer(new byte[LogBufferDescriptor.TERM_META_DATA_LENGTH]);
            }

            AtomicBuffers[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX] = new UnsafeBuffer(new byte[LogBufferDescriptor.LOG_META_DATA_LENGTH]);

            A.CallTo(() => LogBuffers.AtomicBuffers()).Returns(AtomicBuffers);
            A.CallTo(() => LogBuffers.TermLength()).Returns(TERM_BUFFER_LENGTH);
        }

        [Test]
        public virtual void ShouldHandleClosedImage()
        {
            var image = CreateImage();

            image.ManagedResource();

            Assert.True(image.Closed);
            Assert.AreEqual(image.Poll(MockFragmentHandler, int.MaxValue), 0);
        }

        [Test]
        public virtual void ShouldReportCorrectPositionOnReception()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            var messages = image.Poll(MockFragmentHandler, int.MaxValue);
            Assert.AreEqual(messages, 1);

            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();

            A.CallTo(() => Position.SetOrdered(initialPosition)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened()
                );
        }

        [Test]
        public virtual void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInInitialTermId()
        {
            const int initialMessageIndex = 5;
            var initialTermOffset = OffsetForFrame(initialMessageIndex);
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(initialMessageIndex));

            var messages = image.Poll(MockFragmentHandler, int.MaxValue);
            Assert.AreEqual(messages, 1);


            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, initialTermOffset + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();

            A.CallTo(() => Position.SetOrdered(initialPosition)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened()
                );
        }

        [Test]
        public virtual void ShouldReportCorrectPositionOnReceptionWithNonZeroPositionInNonInitialTermId()
        {
            var activeTermId = INITIAL_TERM_ID + 1;
            const int initialMessageIndex = 5;
            var initialTermOffset = OffsetForFrame(initialMessageIndex);
            var initialPosition = LogBufferDescriptor.ComputePosition(activeTermId, initialTermOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(activeTermId, OffsetForFrame(initialMessageIndex));

            var messages = image.Poll(MockFragmentHandler, int.MaxValue);
            Assert.AreEqual(messages, 1);

            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, initialTermOffset + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();

            A.CallTo(() => Position.SetOrdered(initialPosition)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened()
                );
        }

        [Test]
        public virtual void ShouldPollNoFragmentsToControlledFragmentHandler()
        {
            var image = CreateImage();
            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(fragmentsRead, 0);

            A.CallTo(() => Position.SetOrdered(A<long>._)).MustNotHaveHappened();
            A.CallTo(() => MockFragmentHandler(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public virtual void ShouldPollOneFragmentToControlledFragmentHandlerOnContinue()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(fragmentsRead, 1);
            
            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened());
        }

        [Test]
        public virtual void ShouldNotPollOneFragmentToControlledFragmentHandlerOnAbort()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.ABORT);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(fragmentsRead,
                0);
            Assert.AreEqual(image.Position, initialPosition);

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public virtual void ShouldPollOneFragmentToControlledFragmentHandlerOnBreak()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.BREAK);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(fragmentsRead, 1);


            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened().Then(
                A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened());
        }

        [Test]
        public virtual void ShouldPollFragmentsToControlledFragmentHandlerOnCommit()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));

            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.COMMIT);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(fragmentsRead, 2);


            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened()
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH)).MustHaveHappened())
                .Then(A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, ALIGNED_FRAME_LENGTH + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH*2)).MustHaveHappened());
        }


        [Test]
        public virtual void ShouldPollFragmentsToControlledFragmentHandlerOnContinue()
        {
            var initialPosition = LogBufferDescriptor.ComputePosition(INITIAL_TERM_ID, 0, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);
            Position.SetOrdered(initialPosition);
            var image = CreateImage();

            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(0));
            InsertDataFrame(INITIAL_TERM_ID, OffsetForFrame(1));


            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).Returns(ControlledFragmentHandlerAction.CONTINUE);

            var fragmentsRead = image.ControlledPoll(MockControlledFragmentHandler, int.MaxValue);

            Assert.AreEqual(fragmentsRead, 2);


            A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened()
                .Then(A.CallTo(() => MockControlledFragmentHandler.OnFragment(A<UnsafeBuffer>._, ALIGNED_FRAME_LENGTH + DataHeaderFlyweight.HEADER_LENGTH, DATA.Length, A<Header>._)).MustHaveHappened())
                .Then(A.CallTo(() => Position.SetOrdered(initialPosition + ALIGNED_FRAME_LENGTH * 2)).MustHaveHappened());
        }

        private Image CreateImage()
        {
            return new Image(Subscription, SESSION_ID, Position, LogBuffers, ErrorHandler, SOURCE_IDENTITY, CORRELATION_ID);
        }

        private void InsertDataFrame(int activeTermId, int termOffset)
        {
            DataHeader.TermId(INITIAL_TERM_ID).StreamId(STREAM_ID).SessionId(SESSION_ID).TermOffset(termOffset).Header.FrameLength(DATA.Length + DataHeaderFlyweight.HEADER_LENGTH).HeaderType(HeaderFlyweight.HDR_TYPE_DATA).Flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS).Version(HeaderFlyweight.CURRENT_VERSION);

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