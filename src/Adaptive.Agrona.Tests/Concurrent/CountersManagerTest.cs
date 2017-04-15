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
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent
{
    public class CountersManagerTest
    {
        private const int NumberOfCounters = 4;

        private UnsafeBuffer _labelsBuffer;
        private UnsafeBuffer _counterBuffer;
        private CountersManager _manager;
        private CountersReader _otherManager;

        private IntObjConsumer<string> _consumer;
        private CountersReader.MetaData _metaData;

        [SetUp]
        public void Setup()
        {
            _consumer = A.Fake<IntObjConsumer<string>>();
            _metaData = A.Fake<CountersReader.MetaData>();

            _labelsBuffer = new UnsafeBuffer(new byte[NumberOfCounters*CountersReader.METADATA_LENGTH]);
            _counterBuffer = new UnsafeBuffer(new byte[NumberOfCounters*CountersReader.COUNTER_LENGTH]);

            _manager = new CountersManager(_labelsBuffer, _counterBuffer);
            _otherManager = new CountersManager(_labelsBuffer, _counterBuffer);
        }

        [Test]
        public void ManagerShouldStoreLabels()
        {
            var counterId = _manager.Allocate("abc");
            _otherManager.ForEach(_consumer);

            A.CallTo(() => _consumer(counterId, "abc")).MustHaveHappened();
        }

        [Test]
        public void ManagerShouldStoreMultipleLabels()
        {
            var abc = _manager.Allocate("abc");
            var def = _manager.Allocate("def");
            var ghi = _manager.Allocate("ghi");

            _otherManager.ForEach(_consumer);

            A.CallTo(() => _consumer(abc, "abc")).MustHaveHappened()
                .Then(A.CallTo(() => _consumer(def, "def")).MustHaveHappened())
                .Then(A.CallTo(() => _consumer(ghi, "ghi")).MustHaveHappened());

            A.CallTo(() => _consumer(A<int>._, A<string>._)).MustHaveHappened(Repeated.Exactly.Times(3));
        }

        [Test]
        public void ShouldFreeAndReuseCounters()
        {
            var abc = _manager.Allocate("abc");
            var def = _manager.Allocate("def");
            var ghi = _manager.Allocate("ghi");

            _manager.Free(def);
            _otherManager.ForEach(_consumer);

            A.CallTo(() => _consumer(abc, "abc")).MustHaveHappened()
                .Then(A.CallTo(() => _consumer(ghi, "ghi")).MustHaveHappened());

            A.CallTo(() => _consumer(A<int>._, A<string>._)).MustHaveHappened(Repeated.Exactly.Twice);

            Assert.AreEqual(_manager.Allocate("the next label"), def);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void ManagerShouldNotOverAllocateCounters()
        {
            _manager.Allocate("abc");
            _manager.Allocate("def");
            _manager.Allocate("ghi");
            _manager.Allocate("jkl");
            _manager.Allocate("mno");
        }


        [Test]
        public void AllocatedCountersCanBeMapped()
        {
            _manager.Allocate("def");

            var id = _manager.Allocate("abc");
            IReadablePosition reader = new UnsafeBufferPosition(_counterBuffer, id);
            IPosition writer = new UnsafeBufferPosition(_counterBuffer, id);
            const long expectedValue = 0xFFFFFFFFFL;

            writer.SetOrdered(expectedValue);

            Assert.AreEqual(reader.Volatile, expectedValue);
        }

        [Test]
        public void ShouldStoreMetaData()
        {
            const int typeIdOne = 333;
            const long keyOne = 777L;

            const int typeIdTwo = 222;
            const long keyTwo = 444;

            var counterIdOne = _manager.Allocate("Test Label One", typeIdOne, buffer => buffer.PutLong(0, keyOne));
            var counterIdTwo = _manager.Allocate("Test Label Two", typeIdTwo, buffer => buffer.PutLong(0, keyTwo));

            _manager.ForEach(_metaData);

            A.CallTo(() => _metaData(counterIdOne, typeIdOne, A<IDirectBuffer>.That.Matches(d => d.GetLong(0) == keyOne), "Test Label One")).MustHaveHappened()
                .Then(A.CallTo(() => _metaData(counterIdTwo, typeIdTwo, A<IDirectBuffer>.That.Matches(d => d.GetLong(0) == keyTwo), "Test Label Two")).MustHaveHappened());

            A.CallTo(() => _metaData(A<int>._, A<int>._, A<IDirectBuffer>._, A<string>._)).MustHaveHappened(Repeated.Exactly.Twice);
        }

        [Test]
        public void ShouldStoreAndLoadValue()
        {
            var counterId = _manager.Allocate("Test Counter");

            const long value = 7L;
            _manager.SetCounterValue(counterId, value);

            Assert.AreEqual(_manager.GetCounterValue(counterId), value);
        }
    }
}