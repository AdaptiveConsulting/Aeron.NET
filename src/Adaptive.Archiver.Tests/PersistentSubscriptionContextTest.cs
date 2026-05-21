/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
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

using System.Collections.Generic;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using FakeItEasy;
using NUnit.Framework;
using AeronType = Adaptive.Aeron.Aeron;

namespace Adaptive.Archiver.Tests
{
    public class PersistentSubscriptionContextTest
    {
        private const string IpcChannel = "aeron:ipc";

        private PersistentSubscription.Context _context;
        private CountersManager _countersManager;

        [SetUp]
        public void SetUp()
        {
            AeronType aeron = A.Fake<AeronType>();
            UnsafeBuffer metaDataBuffer = new UnsafeBuffer(new byte[128 * 1024]);
            UnsafeBuffer valuesBuffer = new UnsafeBuffer(new byte[64 * 1024]);
            _countersManager = new CountersManager(metaDataBuffer, valuesBuffer);

            A.CallTo(() => aeron.AddCounter(A<int>._, A<string>._))
                .ReturnsLazily((int typeId, string label) =>
                {
                    int counterId = _countersManager.Allocate(label, typeId);
                    return new Counter(_countersManager, counterId);
                });

            _context = new PersistentSubscription.Context()
                .RecordingId(1)
                .LiveChannel(IpcChannel)
                .LiveStreamId(1)
                .ReplayChannel(IpcChannel)
                .ReplayStreamId(2)
                .Aeron(aeron)
                .AeronArchiveContext(new AeronArchive.Context());
        }

        [Test]
        public void CanOnlyConcludeOnce()
        {
            _context.Conclude();

            Assert.Throws<ConcurrentConcludeException>(() => _context.Conclude());
        }

        [Test]
        public void ContextMustHaveAnArchiveContext()
        {
            _context.AeronArchiveContext(null);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [Test]
        public void ContextMustHaveRecordingId()
        {
            _context.RecordingId(AeronType.NULL_VALUE);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [TestCase(null)]
        [TestCase("")]
        public void ContextMustHaveLiveChannel(string channel)
        {
            _context.LiveChannel(channel);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [Test]
        public void ContextMustHaveLiveStreamId()
        {
            _context.LiveStreamId(AeronType.NULL_VALUE);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [TestCase(null)]
        [TestCase("")]
        public void ContextMustHaveReplayChannel(string channel)
        {
            _context.ReplayChannel(channel);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [Test]
        public void MustNotRejoinOnReplayChannels()
        {
            ChannelUriStringBuilder builder = new ChannelUriStringBuilder("aeron:udp?endpoint=localhost:0");
            string configuredReplayChannel = builder.Rejoin(true).Build();

            _context.ReplayChannel(configuredReplayChannel);
            _context.Conclude();
            string actualReplayChannel = _context.ReplayChannel();
            ChannelUriStringBuilder channelUri = new ChannelUriStringBuilder(actualReplayChannel);
            Assert.IsFalse(channelUri.Rejoin().GetValueOrDefault(false));
        }

        [Test]
        public void ContextMustHaveReplayStreamId()
        {
            _context.ReplayStreamId(AeronType.NULL_VALUE);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [Test]
        public void ContextThrowsIfStartPositionIsInvalid()
        {
            _context.StartPosition(-3);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [Test]
        public void ContextThrowsIfRecordingIdIsInvalid()
        {
            _context.RecordingId(-2);
            Assert.Throws<ConfigurationException>(() => _context.Conclude());
        }

        [Test]
        public void ContextCanBeCloned()
        {
            PersistentSubscription.Context clonedCtx = _context.Clone();

            Assert.AreNotSame(_context, clonedCtx);

            Assert.AreEqual(_context.StartPosition(), clonedCtx.StartPosition());
            Assert.AreEqual(_context.RecordingId(), clonedCtx.RecordingId());
            Assert.AreEqual(_context.LiveChannel(), clonedCtx.LiveChannel());
            Assert.AreEqual(_context.LiveStreamId(), clonedCtx.LiveStreamId());

            Assert.AreSame(_context.Listener(), clonedCtx.Listener());
            Assert.AreSame(_context.AeronArchiveContext(), clonedCtx.AeronArchiveContext());
        }

        [Test]
        public void ContextShouldCreateListenerIfNoneProvided()
        {
            _context.Listener(null);
            _context.Conclude();

            Assert.NotNull(_context.Listener());
        }

        [Test]
        public void ContextShouldCreateStateCounterIfNoneProvided()
        {
            _context.StateCounter(null);
            _context.Conclude();

            Assert.NotNull(_context.StateCounter());
        }

        [Test]
        public void ContextShouldCreateJoinDifferenceCounterIfNoneProvided()
        {
            _context.JoinDifferenceCounter(null);
            _context.Conclude();

            Assert.NotNull(_context.JoinDifferenceCounter());
        }

        [Test]
        public void ContextShouldCreateLiveLeftCounterIfNoneProvided()
        {
            _context.LiveLeftCounter(null);
            _context.Conclude();

            Assert.NotNull(_context.LiveLeftCounter());
        }

        [Test]
        public void ContextShouldCreateLiveJoinedCounterIfNoneProvided()
        {
            _context.LiveJoinedCounter(null);
            _context.Conclude();

            Assert.NotNull(_context.LiveJoinedCounter());
        }

        [TestCaseSource(nameof(ReplayAndControlChannels))]
        public void ReplayAndControlChannelMediaTypesMustMatchWhenUsingResponseChannels(
            bool expectSuccess,
            string replayChannel,
            string archiveControlRequestChannel,
            string archiveControlResponseChannel)
        {
            _context.ReplayChannel(replayChannel).AeronArchiveContext()
                .ControlRequestChannel(archiveControlRequestChannel)
                .ControlResponseChannel(archiveControlResponseChannel);
            if (expectSuccess)
            {
                _context.Conclude();
            }
            else
            {
                Assert.Throws<ConfigurationException>(() => _context.Conclude());
            }
        }

        private static IEnumerable<object[]> ReplayAndControlChannels()
        {
            yield return new object[] { true, "aeron:udp?endpoint=localhost:0", null, null };
            yield return new object[]
            {
                true,
                "aeron:udp?endpoint=localhost:0",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?endpoint=localhost:0"
            };
            yield return new object[]
            {
                true,
                "aeron:udp?endpoint=localhost:0",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?control-mode=response|control=localhost:10002"
            };
            yield return new object[] { true, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc" };
            yield return new object[]
            {
                true, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc?control-mode=response"
            };
            yield return new object[]
            {
                true, "aeron:udp?control=localhost:10001|control-mode=response", null, null
            };
            yield return new object[]
            {
                true,
                "aeron:udp?control=localhost:10001|control-mode=response",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?endpoint=localhost:0"
            };
            yield return new object[]
            {
                true,
                "aeron:udp?control=localhost:10001|control-mode=response",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?control-mode=response|control=localhost:10002"
            };
            yield return new object[]
            {
                false, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:ipc", "aeron:ipc"
            };
            yield return new object[]
            {
                false,
                "aeron:udp?control=localhost:10001|control-mode=response",
                "aeron:ipc",
                "aeron:ipc?control-mode=response"
            };
            yield return new object[] { true, "aeron:ipc", null, null };
            yield return new object[]
            {
                true, "aeron:ipc", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"
            };
            yield return new object[]
            {
                true,
                "aeron:ipc",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?control-mode=response|control=localhost:10002"
            };
            yield return new object[] { true, "aeron:ipc", "aeron:ipc", "aeron:ipc" };
            yield return new object[] { true, "aeron:ipc", "aeron:ipc", "aeron:ipc?control-mode=response" };
            yield return new object[] { true, "aeron:ipc?control-mode=response", null, null };
            yield return new object[]
            {
                false,
                "aeron:ipc?control-mode=response",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?endpoint=localhost:0"
            };
            yield return new object[]
            {
                false,
                "aeron:ipc?control-mode=response",
                "aeron:udp?endpoint=localhost:8010",
                "aeron:udp?control-mode=response|control=localhost:10002"
            };
            yield return new object[] { true, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc" };
            yield return new object[]
            {
                true, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc?control-mode=response"
            };
        }
    }
}
