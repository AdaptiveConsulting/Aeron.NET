using System;
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent
{
    public class CountersManagerTest
    {
        private bool InstanceFieldsInitialized = false;

        public CountersManagerTest()
        {
            if (!InstanceFieldsInitialized)
            {
                InitializeInstanceFields();
                InstanceFieldsInitialized = true;
            }
        }

        private void InitializeInstanceFields()
        {
            Manager = new CountersManager(LabelsBuffer, CounterBuffer);
            OtherManager = new CountersManager(LabelsBuffer, CounterBuffer);
        }

        private const int NUMBER_OF_COUNTERS = 4;

        private UnsafeBuffer LabelsBuffer = new UnsafeBuffer(new byte[NUMBER_OF_COUNTERS*CountersReader.METADATA_LENGTH]);
        private UnsafeBuffer CounterBuffer = new UnsafeBuffer(new byte[NUMBER_OF_COUNTERS*CountersReader.COUNTER_LENGTH]);
        private CountersManager Manager;
        private CountersReader OtherManager;

        private IntObjConsumer<string> Consumer;
        private CountersReader.MetaData MetaData;

        [SetUp]
        public void Setup()
        {
            Consumer = A.Fake<IntObjConsumer<string>>();
            MetaData = A.Fake<CountersReader.MetaData>();
        }

        [Test]
        public virtual void ManagerShouldStoreLabels()
        {
            var counterId = Manager.Allocate("abc");
            OtherManager.ForEach(Consumer);



            verify(Consumer).accept(counterId, "abc");
        }

        [Test]
        public virtual void ManagerShouldStoreMultipleLabels()
        {
            var abc = Manager.Allocate("abc");
            var def = Manager.Allocate("def");
            var ghi = Manager.Allocate("ghi");

            OtherManager.ForEach(Consumer);

            InOrder inOrder = Mockito.inOrder(Consumer);
            inOrder.verify(Consumer).accept(abc, "abc");
            inOrder.verify(Consumer).accept(def, "def");
            inOrder.verify(Consumer).accept(ghi, "ghi");
            inOrder.verifyNoMoreInteractions();
        }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void shouldFreeAndReuseCounters()
        public virtual void ShouldFreeAndReuseCounters()
        {
            var abc = Manager.Allocate("abc");
            var def = Manager.Allocate("def");
            var ghi = Manager.Allocate("ghi");

            Manager.Free(def);

            OtherManager.ForEach(Consumer);

//JAVA TO C# CONVERTER WARNING: The original Java variable was marked 'final':
//ORIGINAL LINE: final org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(consumer);
            InOrder inOrder = Mockito.inOrder(Consumer);
            inOrder.verify(Consumer).accept(abc, "abc");
            inOrder.verify(Consumer).accept(ghi, "ghi");
            inOrder.verifyNoMoreInteractions();

            assertThat(Manager.Allocate("the next label"), @is(def));
        }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test(expected = IllegalArgumentException.class) public void managerShouldNotOverAllocateCounters()
[ExpectedException(typeof(ArgumentException))]
        public virtual void ManagerShouldNotOverAllocateCounters()
        {
            Manager.Allocate("abc");
            Manager.Allocate("def");
            Manager.Allocate("ghi");
            Manager.Allocate("jkl");
            Manager.Allocate("mno");
        }

        [Test]
        public virtual void AllocatedCountersCanBeMapped()
        {
            Manager.Allocate("def");

            var id = Manager.Allocate("abc");
            IReadablePosition reader = new UnsafeBufferPosition(CounterBuffer, id);
            IPosition writer = new UnsafeBufferPosition(CounterBuffer, id);
            const long expectedValue = 0xFFFFFFFFFL;

            writer.Ordered = expectedValue;

            assertThat(reader.Volatile, @is(expectedValue));
        }

//JAVA TO C# CONVERTER TODO TASK: Most Java annotations will not have direct .NET equivalent attributes:
//ORIGINAL LINE: @Test public void shouldStoreMetaData()
        public virtual void ShouldStoreMetaData()
        {
            const int typeIdOne = 333;
            const long keyOne = 777L;

            const int typeIdTwo = 222;
            const long keyTwo = 444;

            var counterIdOne = Manager.Allocate("Test Label One", typeIdOne, (buffer) => buffer.PutLong(0, keyOne));
            var counterIdTwo = Manager.Allocate("Test Label Two", typeIdTwo, (buffer) => buffer.PutLong(0, keyTwo));

            Manager.ForEach(MetaData);

            ArgumentCaptor<DirectBuffer> argCaptorOne = ArgumentCaptor.forClass(typeof(IDirectBuffer));
            ArgumentCaptor<DirectBuffer> argCaptorTwo = ArgumentCaptor.forClass(typeof(IDirectBuffer));

            InOrder inOrder = Mockito.inOrder(MetaData);
            inOrder.verify(MetaData).accept(eq(counterIdOne), eq(typeIdOne), argCaptorOne.capture(), eq("Test Label One"));
            inOrder.verify(MetaData).accept(eq(counterIdTwo), eq(typeIdTwo), argCaptorTwo.capture(), eq("Test Label Two"));
            inOrder.verifyNoMoreInteractions();

            IDirectBuffer keyOneBuffer = argCaptorOne.Value;
            Assert.AreEqual(keyOneBuffer.GetLong(0), keyOne);

            IDirectBuffer keyTwoBuffer = argCaptorTwo.Value;
            Assert.AreEqual(keyTwoBuffer.GetLong(0), keyTwo);
        }

        [Test]
        public virtual void ShouldStoreAndLoadValue()
        {
            var counterId = Manager.Allocate("Test Counter");

            const long value = 7L;
            Manager.SetCounterValue(counterId, value);

            Assert.AreEqual(Manager.GetCounterValue(counterId), value);
        }
    }
}