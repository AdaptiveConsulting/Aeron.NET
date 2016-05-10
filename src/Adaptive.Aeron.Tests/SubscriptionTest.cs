using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Protocol;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    public class SubscriptionTest
    {
        private const string CHANNEL = "udp://localhost:40124";
        private const int STREAM_ID_1 = 2;
        private const long SUBSCRIPTION_CORRELATION_ID = 100;
        private const int READ_BUFFER_CAPACITY = 1024;
        private static readonly byte FLAGS = FrameDescriptor.UNFRAGMENTED;
        private static readonly int FRAGMENT_COUNT_LIMIT = int.MaxValue;
        private const int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

        private UnsafeBuffer AtomicReadBuffer;
        private ClientConductor Conductor;
        private IFragmentHandler FragmentHandler;
        private Image ImageOneMock;
        private Header Header;
        private Image ImageTwoMock;

        private Subscription Subscription;

        [SetUp]
        public void Setup()
        {
            AtomicReadBuffer = new UnsafeBuffer(new byte[READ_BUFFER_CAPACITY]);
            Conductor = A.Fake<ClientConductor>();
            FragmentHandler = A.Fake<IFragmentHandler>();
            ImageOneMock = A.Fake<Image>();
            ImageTwoMock = A.Fake<Image>();
            Header = A.Fake<Header>();

            A.CallTo(() => Header.Flags()).Returns(FLAGS);

            Subscription = new Subscription(Conductor, CHANNEL, STREAM_ID_1, SUBSCRIPTION_CORRELATION_ID);
            A.CallTo(() => Conductor.ReleaseSubscription(Subscription));
        }

        [Test]
        public virtual void ShouldEnsureTheSubscriptionIsOpenWhenPolling()
        {
            Subscription.Dispose();
            Assert.True(Subscription.Closed);
        }

        [Test]
        public virtual void ShouldReadNothingWhenNoImages()
        {
            Assert.AreEqual(Subscription.Poll(FragmentHandler, 1), 0);
        }

        [Test]
        public virtual void ShouldReadNothingWhenThereIsNoData()
        {
            Subscription.AddImage(ImageOneMock);


            A.CallTo(() => ImageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).Returns(0);

            Assert.AreEqual(Subscription.Poll(FragmentHandler, 1), 0);
        }

        [Test]
        public virtual void ShouldReadData()
        {
            Subscription.AddImage(ImageOneMock);

            A.CallTo(() => ImageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (IFragmentHandler) o.Arguments[0];
                handler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            Assert.AreEqual(Subscription.Poll(FragmentHandler, FRAGMENT_COUNT_LIMIT), 1);

            A.CallTo(() => FragmentHandler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, A<Header>._)).MustHaveHappened();
        }

        [Test]
        public virtual void ShouldReadDataFromMultipleSources()
        {
            Subscription.AddImage(ImageOneMock);
            Subscription.AddImage(ImageTwoMock);

            A.CallTo(() => ImageOneMock.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (IFragmentHandler) o.Arguments[0];
                handler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            A.CallTo(() => ImageTwoMock.Poll(A<IFragmentHandler>._, A<int>._)).ReturnsLazily(o =>
            {
                var handler = (IFragmentHandler) o.Arguments[0];
                handler.OnFragment(AtomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, Header);
                return 1;
            });

            Assert.AreEqual(Subscription.Poll(FragmentHandler, FRAGMENT_COUNT_LIMIT), 2);
        }
    }
}