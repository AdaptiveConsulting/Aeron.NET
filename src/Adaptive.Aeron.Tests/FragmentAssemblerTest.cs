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

        private IFragmentHandler delegateFragmentHandler;
        private IDirectBuffer termBuffer;
        private Header header;
        private FragmentAssembler adapter;

        [SetUp]
        public void SetUp()
        {
            delegateFragmentHandler = A.Fake<IFragmentHandler>();
            termBuffer = A.Fake<IDirectBuffer>();
            adapter = new FragmentAssembler(delegateFragmentHandler);
            header = A.Fake<Header>(x => x.Wrapping(new Header(INITIAL_TERM_ID, LogBufferDescriptor.TERM_MIN_LENGTH)));

            header.Buffer(termBuffer);

            A.CallTo(() => termBuffer.GetInt(A<int>._)).Returns(SESSION_ID);
        }

        [Test]
        public virtual void ShouldPassThroughUnfragmentedMessage()
        {
            A.CallTo(() => header.Flags()).Returns(FrameDescriptor.UNFRAGMENTED);

            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[128]);
            const int offset = 8;
            const int length = 32;

            adapter.OnFragment(srcBuffer, offset, length, header);

            A.CallTo(() => delegateFragmentHandler.OnFragment(srcBuffer, offset, length, header))
                .MustHaveHappened(Repeated.Exactly.Once);
        }

        [Test]
        [Ignore("James to have a look")]
        public virtual void ShouldAssembleTwoPartMessage()
        {
            A.CallTo(() => header.Flags()).ReturnsNextFromSequence(FrameDescriptor.BEGIN_FRAG_FLAG, FrameDescriptor.END_FRAG_FLAG);

            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
            const int offset = 0;
            int length = srcBuffer.Capacity / 2;

            srcBuffer.SetMemory(0, length, 65);
            srcBuffer.SetMemory(length, length, 66);

            adapter.OnFragment(srcBuffer, offset, length, header);
            adapter.OnFragment(srcBuffer, length, length, header);
            
            Func<UnsafeBuffer, bool> bufferAssertion = capturedBuffer =>
            {
                for (int i = 0; i < srcBuffer.Capacity; i++)
                {
                    if (capturedBuffer.GetByte(i) != srcBuffer.GetByte(i))
                    {
                        return false;
                    }
                }
                return true;
            };

            Func<Header, bool> headerAssertion = capturedHeader => capturedHeader.SessionId() == SESSION_ID &&
                                                                   capturedHeader.Flags() == FrameDescriptor.END_FRAG_FLAG;

            A.CallTo(() => delegateFragmentHandler.OnFragment(
                A<UnsafeBuffer>.That.Matches(bufferAssertion, "buffer"), 
                offset, 
                length * 2, 
                A<Header>.That.Matches(headerAssertion, "header")))
                .MustHaveHappened(Repeated.Exactly.Once);
        }

    }
}