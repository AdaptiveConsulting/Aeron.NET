using System;
using Adaptive.Aeron.LogBuffer;
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

        private FragmentHandler delegateFragmentHandler;
        private UnsafeBuffer termBuffer;
        private Header header;
        private FragmentAssembler adapter;

        [SetUp]
        public void SetUp()
        {
            delegateFragmentHandler = A.Fake<FragmentHandler>();
            termBuffer = A.Fake<UnsafeBuffer>();
            adapter = new FragmentAssembler(delegateFragmentHandler);
            header = A.Fake<Header>(x => x.Wrapping(new Header(INITIAL_TERM_ID, LogBufferDescriptor.TERM_MIN_LENGTH)));

            header.SetBuffer(termBuffer, 0);

            A.CallTo(() => termBuffer.GetInt(A<int>._)).Returns(SESSION_ID);
        }

        [Test]
        public virtual void ShouldPassThroughUnfragmentedMessage()
        {
            A.CallTo(() => header.Flags).Returns(FrameDescriptor.UNFRAGMENTED);

            var srcBuffer = new UnsafeBuffer(new byte[128]);
            const int offset = 8;
            const int length = 32;

            adapter.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler(srcBuffer, offset, length, header))
                .MustHaveHappened(Repeated.Exactly.Once);
        }

        [Test]
        public virtual void ShouldAssembleTwoPartMessage()
        {
            A.CallTo(() => header.Flags).ReturnsNextFromSequence(FrameDescriptor.BEGIN_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG);
            // Need to add this twice because FakeItEasy doesn't fall back to the implementation

            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            srcBuffer.SetMemory(0, length, 65);
            srcBuffer.SetMemory(length, length, 66);

            adapter.OnFragment(srcBuffer, offset, length, header);
            adapter.OnFragment(srcBuffer, length, length, header);

            Func<UnsafeBuffer, bool> bufferAssertion = capturedBuffer =>
            {
                for (var i = 0; i < srcBuffer.Capacity; i++)
                {
                    if (capturedBuffer.GetByte(i) != srcBuffer.GetByte(i))
                    {
                        return false;
                    }
                }
                return true;
            };

            Func<Header, bool> headerAssertion = capturedHeader => capturedHeader.SessionId == SESSION_ID &&
                                                                   capturedHeader.Flags == FrameDescriptor.END_FRAG_FLAG;
            A.CallTo(() => delegateFragmentHandler(
                A<UnsafeBuffer>.That.Matches(bufferAssertion, "buffer"),
                offset,
                length*2,
                A<Header>.That.Matches(headerAssertion, "header")))
                .MustHaveHappened(Repeated.Exactly.Once);
        }

        [Test]
        public virtual void ShouldAssembleFourPartMessage()
        {
            A.CallTo(() => header.Flags).ReturnsNextFromSequence<byte>(FrameDescriptor.BEGIN_FRAG_FLAG, 0, 0, FrameDescriptor.END_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/4;

            for (var i = 0; i < 4; i++)
            {
                srcBuffer.SetMemory(i*length, length, (byte) (65 + i));
            }

            adapter.OnFragment(srcBuffer, offset, length, header);
            adapter.OnFragment(srcBuffer, offset + length, length, header);
            adapter.OnFragment(srcBuffer, offset + (length*2), length, header);
            adapter.OnFragment(srcBuffer, offset + (length*3), length, header);


            Func<UnsafeBuffer, bool> bufferAssertion = capturedBuffer =>
            {
                for (var i = 0; i < srcBuffer.Capacity; i++)
                {
                    if (capturedBuffer.GetByte(i) != srcBuffer.GetByte(i))
                    {
                        return false;
                    }
                }
                return true;
            };

            Func<Header, bool> headerAssertion = capturedHeader => capturedHeader.SessionId == SESSION_ID &&
                                                                   capturedHeader.Flags == FrameDescriptor.END_FRAG_FLAG;

            A.CallTo(() => delegateFragmentHandler(
                A<UnsafeBuffer>.That.Matches(bufferAssertion, "buffer"),
                offset,
                length*4,
                A<Header>.That.Matches(headerAssertion, "header")))
                .MustHaveHappened(Repeated.Exactly.Once);
        }

        [Test]
        public virtual void ShouldFreeSessionBuffer()
        {
            A.CallTo(() => header.Flags).ReturnsNextFromSequence(FrameDescriptor.BEGIN_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG);

            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            srcBuffer.SetMemory(0, length, 65);
            srcBuffer.SetMemory(length, length, 66);

            Assert.False(adapter.FreeSessionBuffer(SESSION_ID));

            adapter.OnFragment(srcBuffer, offset, length, header);
            adapter.OnFragment(srcBuffer, length, length, header);

            Assert.True(adapter.FreeSessionBuffer(SESSION_ID));
            Assert.False(adapter.FreeSessionBuffer(SESSION_ID));
        }

        [Test]
        public virtual void ShouldDoNotingIfEndArrivesWithoutBegin()
        {
            A.CallTo(() => header.Flags).Returns(FrameDescriptor.END_FRAG_FLAG);
            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            adapter.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }

        [Test]
        public virtual void ShouldDoNotingIfMidArrivesWithoutBegin()
        {
            A.CallTo(() => header.Flags).Returns(FrameDescriptor.END_FRAG_FLAG);
            var srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            var length = srcBuffer.Capacity/2;

            adapter.OnFragment(srcBuffer, offset, length, header);
            adapter.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler(A<UnsafeBuffer>._, A<int>._, A<int>._, A<Header>._)).MustNotHaveHappened();
        }
    }
}