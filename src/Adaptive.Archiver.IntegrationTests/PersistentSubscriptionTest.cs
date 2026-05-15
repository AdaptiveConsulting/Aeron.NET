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

using System;
using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Archiver.IntegrationTests.Helpers;
using Adaptive.Archiver.IntegrationTests.Infrastructure;
using NUnit.Framework;
using AeronClient = Adaptive.Aeron.Aeron;

namespace Adaptive.Archiver.IntegrationTests
{
    /// <summary>
    /// Common SetUp / TearDown for PersistentSubscription system tests. Mirrors the
    /// abstract base in <c>aeron-system-tests/.../PersistentSubscriptionTest.java</c>:
    /// concrete subclasses pick controlled vs uncontrolled poll semantics.
    /// </summary>
    [Category("Integration")]
    internal abstract class PersistentSubscriptionTest
    {
        protected const int TermLength = Adaptive.Aeron.LogBuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
        protected const int OneKbMessageSize = 1024 - Adaptive.Aeron.Protocol.DataHeaderFlyweight.HEADER_LENGTH;

        private static readonly System.Random Random = new();

        protected EmbeddedMediaDriver Driver;
        protected EmbeddedArchive Archive;
        protected AeronClient Aeron;
        protected AeronArchive AeronArchive;
        protected AeronArchive.Context AeronArchiveCtxTpl;
        protected PersistentSubscription.Context PersistentSubscriptionCtx;
        protected PersistentSubscriptionListenerImpl Listener;
        protected readonly List<IDisposable> Closeables = new();

        // MDC control port — picked per test in SetUp. Two tests sharing this port would
        // collide on the UDP bind exactly the way the archive control port used to.
        protected int MdcControlPort;
        protected string MdcSubscriptionChannel => $"aeron:udp?control=localhost:{MdcControlPort}";
        protected string MdcPublicationChannel =>
            $"aeron:udp?control=localhost:{MdcControlPort}|control-mode=dynamic|fc=max";

        [SetUp]
        public virtual void SetUp()
        {
            lock (Random) { MdcControlPort = Random.Next(25_000, 30_000); }

            Driver = new EmbeddedMediaDriver();
            Aeron = AeronClient.Connect(new AeronClient.Context().AeronDirectoryName(Driver.AeronDirectoryName));
            Archive = new EmbeddedArchive(Driver.AeronDirectoryName, aeronClient: Aeron);

            AeronArchiveCtxTpl = Archive
                .CreateClientContext(Driver.AeronDirectoryName)
                .AeronClient(Aeron);

            AeronArchive = AeronArchive.Connect(CloneArchiveCtx());

            Listener = new PersistentSubscriptionListenerImpl();
            PersistentSubscriptionCtx = new PersistentSubscription.Context()
                .Aeron(Aeron)
                .RecordingId(13)
                .StartPosition(0)
                .LiveChannel(TestContexts.IpcChannel)
                .LiveStreamId(TestContexts.StreamId)
                .ReplayChannel(TestContexts.EphemeralReplayChannel)
                .ReplayStreamId(-5)
                .Listener(Listener)
                .AeronArchiveContext(CloneArchiveCtx());
        }

        [TearDown]
        public virtual void TearDown()
        {
            foreach (var c in System.Linq.Enumerable.Reverse(Closeables))
            {
                DisposeWithTimeout(c, 3_000, "Closeable");
            }
            Closeables.Clear();

            DisposeWithTimeout(AeronArchive, 3_000, "AeronArchive");
            DisposeWithTimeout(Aeron, 3_000, "Aeron");

            try { Archive?.Dispose(); } catch { }
            try { Driver?.Dispose(); } catch { }
        }

        private static void DisposeWithTimeout(IDisposable target, int timeoutMs, string name)
        {
            if (target == null)
            {
                return;
            }
            var task = System.Threading.Tasks.Task.Run(() =>
            {
                try { target.Dispose(); } catch { }
            });
            if (!task.Wait(timeoutMs))
            {
                TestContext.Progress.WriteLine(
                    $"TearDown: {name}.Dispose did not return within {timeoutMs}ms; "
                    + "abandoning to JVM kill below.");
            }
        }

        /// <summary>
        /// Concrete subclasses pick controlled vs uncontrolled poll.
        /// </summary>
        protected abstract int Poll(PersistentSubscription subscription, IFragmentHandler handler, int fragmentLimit);

        protected AeronArchive.Context CloneArchiveCtx() =>
            Archive
                .CreateClientContext(Driver.AeronDirectoryName)
                .AeronClient(Aeron);

        protected static IList<byte[]> GenerateFixedPayloads(int count, int size)
        {
            var payloads = new List<byte[]>(count);
            for (var i = 0; i < count; i++)
            {
                var payload = new byte[size];
                lock (Random) { Random.NextBytes(payload); }
                payloads.Add(payload);
            }
            return payloads;
        }

        protected static IList<byte[]> GenerateRandomPayloads(int count)
        {
            var payloads = new List<byte[]>(count);
            for (var i = 0; i < count; i++)
            {
                int length;
                lock (Random) { length = Random.Next(2048); }
                var payload = new byte[length];
                lock (Random) { Random.NextBytes(payload); }
                payloads.Add(payload);
            }
            return payloads;
        }

        protected static void AssertPayloads(IList<byte[]> received, params IList<byte[]>[] expectedBatches)
        {
            var expected = new List<byte[]>();
            foreach (var batch in expectedBatches) { expected.AddRange(batch); }

            Assert.That(received.Count, Is.EqualTo(expected.Count),
                "payload count mismatch: received {0} vs expected {1}", received.Count, expected.Count);

            for (var i = 0; i < expected.Count; i++)
            {
                Assert.That(received[i], Is.EqualTo(expected[i]), "payload mismatch at index " + i);
            }
        }

        [Test, Timeout(15_000)]
        public void ShouldErrorIfRecordingDoesNotExist()
        {
            const int nonExistentRecordingId = 13;
            PersistentSubscriptionCtx.RecordingId(nonExistentRecordingId);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed, () => Poll(persistentSubscription, null, 1));

            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(
                ((PersistentSubscriptionException)Listener.LastException).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.RECORDING_NOT_FOUND));
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [Test, Timeout(15_000)]
        public void ShouldNotRequireEventListener()
        {
            PersistentSubscriptionCtx.Listener(null);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed, () => Poll(persistentSubscription, null, 1));
        }

        [Test, Timeout(15_000)]
        public void ShouldErrorIfRecordingStreamDoesNotMatchLiveStream()
        {
            const int liveStreamId = 1001;

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveStreamId(liveStreamId);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed, () => Poll(persistentSubscription, null, 1));

            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(
                ((PersistentSubscriptionException)Listener.LastException).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.STREAM_ID_MISMATCH));
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [Test, Timeout(15_000)]
        public void ShouldErrorIfStartPositionIsBeforeRecordingStartPosition()
        {
            var channel = new Adaptive.Aeron.ChannelUriStringBuilder()
                .Media("ipc")
                .InitialPosition(1024, 0, TermLength)
                .Build();

            var persistentPublication = PersistentPublication.Create(AeronArchive, channel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            const int startPosition = 0;
            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(startPosition);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed, () => Poll(persistentSubscription, null, 1));

            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(
                ((PersistentSubscriptionException)Listener.LastException).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.INVALID_START_POSITION));
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [Test, Timeout(15_000)]
        public void ShouldErrorIfStartPositionIsAfterStopPosition()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            persistentPublication.Persist(GenerateFixedPayloads(1, OneKbMessageSize));

            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));

            var startPosition = stopPosition * 2;
            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(startPosition);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed, () => Poll(persistentSubscription, null, 1));

            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(
                ((PersistentSubscriptionException)Listener.LastException).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.INVALID_START_POSITION));
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [Test, Timeout(15_000)]
        public void ShouldReplayFromSpecificMidRecordingPosition()
        {
            var channel = new Adaptive.Aeron.ChannelUriStringBuilder().Media("ipc").Build();
            var persistentPublication = PersistentPublication.Create(AeronArchive, channel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var firstBatch = GenerateFixedPayloads(4, OneKbMessageSize);
            persistentPublication.Persist(firstBatch);
            var secondBatch = GenerateFixedPayloads(2, OneKbMessageSize);
            persistentPublication.Persist(secondBatch);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(4096);

            var fragmentHandler = new BufferingFragmentHandler();

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(secondBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, secondBatch);
        }

        [Test, Timeout(15_000)]
        public void CanJoinALiveStreamAtTheBeginning()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(fragmentHandler.ReceivedPayloads, Is.Empty);

            var messages = GenerateRandomPayloads(3);
            persistentPublication.Persist(messages);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(messages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
        }

        [Test, Timeout(15_000)]
        public void ShouldNotReplayOldMessagesWhenStartingFromLive()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var oldMessages = GenerateRandomPayloads(5);
            persistentPublication.Persist(oldMessages);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(fragmentHandler.ReceivedPayloads, Is.Empty);

            var newMessages = GenerateRandomPayloads(3);
            persistentPublication.Persist(newMessages);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(newMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, newMessages);
        }

        [Test, Timeout(15_000)]
        public void ShouldErrorWhenStartPositionDoesNotAlignWithFrame()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            persistentPublication.Persist(GenerateFixedPayloads(1, OneKbMessageSize));

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(OneKbMessageSize - 32)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [TestCase(0L)]
        [TestCase(1024L)]
        [Timeout(15_000)]
        public void ShouldReplayFromRecordingStartPositionWhenStartingFromStart(long recordingStartPosition)
        {
            var channel = new Adaptive.Aeron.ChannelUriStringBuilder()
                .Media("ipc")
                .InitialPosition(recordingStartPosition, 0, TermLength)
                .Build();

            var persistentPublication = PersistentPublication.Create(AeronArchive, channel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var messages = GenerateFixedPayloads(3, OneKbMessageSize);
            persistentPublication.Persist(messages);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_START);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(messages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
        }

        [Test, Timeout(20_000)]
        public void ShouldStartFromLiveWhenThereIsNoDataToReplay()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10), 18_000);

            Assert.That(fragmentHandler.ReceivedPayloads, Is.Empty);

            var messages = GenerateRandomPayloads(5);
            persistentPublication.Persist(messages);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(messages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10), 18_000);
        }

        [Test, Timeout(15_000)]
        public void CanStartFromLiveWhenRecordingHasStopped()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var firstBatch = GenerateRandomPayloads(1);
            var secondBatch = GenerateRandomPayloads(1);
            persistentPublication.Persist(firstBatch);

            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.Publish(secondBatch);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(secondBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 1));

            AssertPayloads(fragmentHandler.ReceivedPayloads, secondBatch);
        }

        [Test, Timeout(15_000)]
        public void CanStartAtRecordingStopPositionWhenLiveHasNotAdvanced()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            persistentPublication.Persist(GenerateFixedPayloads(1, OneKbMessageSize));

            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(stopPosition);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            var liveMessages = GenerateRandomPayloads(3);
            persistentPublication.Publish(liveMessages);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(liveMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 1));
            AssertPayloads(fragmentHandler.ReceivedPayloads, liveMessages);
            Assert.That(Listener.ErrorCount, Is.EqualTo(0));
        }

        [Test, Timeout(15_000)]
        public void ShouldStartFromStoppedRecordingAndJoinLiveWhenLiveHasNotAdvanced()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var oldMessages = GenerateFixedPayloads(8, OneKbMessageSize);
            persistentPublication.Persist(oldMessages);

            persistentPublication.Stop();

            PersistentSubscriptionCtx
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(persistentPublication.RecordingId);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(oldMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));
            AssertPayloads(fragmentHandler.ReceivedPayloads, oldMessages);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var newMessages = GenerateFixedPayloads(16, OneKbMessageSize);
            persistentPublication.Publish(newMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(oldMessages.Count + newMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsLive(), Is.True);
            Assert.That(persistentSubscription.IsReplaying(), Is.False);
        }

        [Test, Timeout(30_000)]
        public void ShouldStartFromStoppedRecordingAndErrorWhenLiveHasAdvanced()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var recordedMessages = GenerateFixedPayloads(8, OneKbMessageSize);
            persistentPublication.Persist(recordedMessages);

            persistentPublication.Stop();

            // Add another subscriber so the publication's live position can advance.
            using (var subscriber2 = Aeron.AddSubscription(MdcSubscriptionChannel, TestContexts.StreamId))
            {
                var afterRecording = GenerateFixedPayloads(1, OneKbMessageSize);
                persistentPublication.Publish(afterRecording);

                var subscriber2Handler = new BufferingFragmentHandler();
                Tests.ExecuteUntil(
                    () => subscriber2Handler.HasReceivedPayloads(afterRecording.Count),
                    () => subscriber2.Poll(subscriber2Handler, 10));
            }

            PersistentSubscriptionCtx
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(persistentPublication.RecordingId);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(
                PersistentSubscriptionCtx.Clone()
                    .AeronArchiveContext(CloneArchiveCtx().MessageTimeoutNs(15_000_000_000L)));

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(recordedMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(recordedMessages.Count));
            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(
                ((PersistentSubscriptionException)Listener.LastException).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.INVALID_START_POSITION));
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [Test, Timeout(15_000)]
        public void ShouldFailIfLiveStreamPositionGoesBackwards()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var payloads = GenerateFixedPayloads(2, 32);
            persistentPublication.Persist(payloads);

            PersistentSubscriptionCtx.RecordingId(persistentPublication.RecordingId);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            persistentPublication.Dispose();
            Closeables.Remove(persistentPublication);

            var replacement = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(replacement);

            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            // Java asserts the full "ERROR - live stream joined..." string, where "ERROR - " is
            // prepended by Java's AeronException base-class constructor. The .NET AeronException
            // does NOT prepend the category name (see follow-up task) -- a separate base-class
            // fix is needed before this can match Java byte-for-byte. Test logical content for now.
            Assert.That(persistentSubscription.FailureReason()?.Message,
                Does.Contain("live stream joined at position 0 which is earlier than last seen position 128"));
        }

        [Test, Timeout(30_000)]
        public void ShouldStayOnReplayWhenLiveCannotConnect()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var messages = GenerateRandomPayloads(5);
            persistentPublication.Persist(messages);

            // Wrong control endpoint for the live channel -- the live subscription will never connect.
            const string incorrectLiveChannel = "aeron:udp?control=localhost:49582|control-mode=dynamic";

            // 5 s: long enough for the subprocess JVM archive handshake on slow Windows CI (which can
            // exceed 500 ms), yet short enough for "No image became available" to fire within the timeout.
            var archiveCtx = CloneArchiveCtx().MessageTimeoutNs(5_000_000_000L);
            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_START)
                .LiveChannel(incorrectLiveChannel)
                .AeronArchiveContext(archiveCtx);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(5),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            Tests.ExecuteUntil(() => Listener.ErrorCount > 0,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LastException?.Message,
                Does.Contain("No image became available on the live subscription"));
            Assert.That(persistentSubscription.HasFailed(), Is.False);
            Assert.That(persistentSubscription.FailureReason(), Is.Null);
            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            var moreMessages = GenerateRandomPayloads(3);
            persistentPublication.Persist(moreMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(messages.Count + moreMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsReplaying(), Is.True);
            AssertPayloads(fragmentHandler.ReceivedPayloads, messages, moreMessages);
        }

        [Test, Timeout(15_000)]
        public void FallbackFromLiveFailsWhenRecordingStoppedBeforeLivePosition()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            persistentPublication.Persist(GenerateRandomPayloads(1));

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.Stop();

            // Messages published past the now-frozen recording stop position.
            var liveOnly = GenerateRandomPayloads(3);
            persistentPublication.Publish(liveOnly);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(liveOnly.Count),
                () => Poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.ClosePublicationOnly();

            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            Assert.That(
                ((PersistentSubscriptionException)persistentSubscription.FailureReason()).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.INVALID_START_POSITION));
        }

        [Test, Timeout(20_000)]
        public void CannotFallbackToReplayWhenRecordingHasStoppedAtAnEarlierPosition()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            using var fastDriver = new EmbeddedMediaDriver();
            using var fastAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(fastDriver.AeronDirectoryName));
            using var fastSubscription = fastAeron.AddSubscription(MdcSubscriptionChannel, TestContexts.StreamId);
            var fastHandler = new CountingFragmentHandler();

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            // Consume a batch of messages on live.
            var firstBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(firstBatch);
            Tests.ExecuteUntil(
                () => fastHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount),
                () => fastSubscription.Poll(fastHandler, 10));

            // Stop the recording.
            AeronArchive.StopRecording(persistentPublication.ExclusivePub);

            // Publish more messages past the now-frozen recording stop position.
            var secondBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Publish(secondBatch);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(firstBatch.Count + secondBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            // End the live image via EOS rather than waiting for a flow-control timeout.
            persistentPublication.ClosePublicationOnly();

            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
            Assert.That(
                ((PersistentSubscriptionException)Listener.LastException).ReasonValue,
                Is.EqualTo(PersistentSubscriptionException.Reason.INVALID_START_POSITION));
        }

        [Test, Timeout(20_000)]
        public void CannotFallbackToReplayWhenRecordingHasBeenRemoved()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            using var fastDriver = new EmbeddedMediaDriver();
            using var fastAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(fastDriver.AeronDirectoryName));
            using var fastConsumer = fastAeron.AddSubscription(MdcSubscriptionChannel, TestContexts.StreamId);
            var fastHandler = new CountingFragmentHandler();

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            Tests.AwaitConnected(fastConsumer);

            var firstBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(firstBatch);
            Tests.ExecuteUntil(
                () => fastHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount),
                () => fastConsumer.Poll(fastHandler, 10));

            // Remove the recording entirely.
            AeronArchive.StopRecording(persistentPublication.ExclusivePub);
            AeronArchive.PurgeRecording(persistentPublication.RecordingId);

            // Let the fast consumer race ahead of the persistent subscription so PS falls off live.
            var secondBatch = new System.Collections.Generic.List<byte[]>();
            for (var i = 0; i < 3; i++)
            {
                var batch = GenerateFixedPayloads(32, OneKbMessageSize);
                persistentPublication.Publish(batch);
                secondBatch.AddRange(batch);
                Tests.ExecuteUntil(
                    () => fastHandler.HasReceivedPayloads(secondBatch.Count),
                    () => fastConsumer.Poll(fastHandler, 10));
            }

            // PS cannot fall back -- the recording is gone.
            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
            Assert.That(Listener.LastException.Message, Does.Contain("unknown recording id:"));
        }

        [Test, Timeout(15_000)]
        public void ShouldErrorIfStartPositionIsAfterRecordingLivePosition()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            persistentPublication.Persist(GenerateFixedPayloads(1, OneKbMessageSize));

            var recordedPosition = persistentPublication.Position;
            Assert.That(recordedPosition, Is.GreaterThan(0));

            var startPosition = recordedPosition * 2;
            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(startPosition);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            Tests.ExecuteUntil(persistentSubscription.HasFailed, () => Poll(persistentSubscription, null, 1));

            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException, Is.InstanceOf<PersistentSubscriptionException>());
            Assert.That(persistentSubscription.FailureReason(), Is.SameAs(Listener.LastException));
        }

        [Test, Timeout(20_000)]
        public void ShouldDropFromLiveBackToReplayThenJoinLiveAgain()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var firstBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(firstBatch);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(0));

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(1),
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));
            Assert.That(fragmentHandler.HasReceivedPayloads(firstBatch.Count), Is.True);

            var secondBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(secondBatch);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(firstBatch.Count + secondBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(persistentSubscription.IsLive(), Is.True);

            using var fastDriver = new EmbeddedMediaDriver();
            using var fastAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(fastDriver.AeronDirectoryName));
            using var fastConsumer = fastAeron.AddSubscription(MdcSubscriptionChannel, TestContexts.StreamId);
            var fastHandler = new CountingFragmentHandler();
            Tests.AwaitConnected(fastConsumer);

            var thirdBatch = new List<byte[]>();
            for (var i = 0; i < 3; i++)
            {
                var batch = GenerateFixedPayloads(32, OneKbMessageSize);
                persistentPublication.Publish(batch);
                thirdBatch.AddRange(batch);
                Tests.ExecuteUntil(
                    () => fastHandler.HasReceivedPayloads(thirdBatch.Count),
                    () => fastConsumer.Poll(fastHandler, 10));
            }

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(1));

            var fourthBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(fourthBatch);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount)
                      && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(2));
            AssertPayloads(fragmentHandler.ReceivedPayloads, firstBatch, secondBatch, thirdBatch, fourthBatch);
        }

        [Test, Timeout(20_000)]
        public void CanFallbackToReplayAfterStartingFromLive()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            // These are persisted BEFORE the PS starts -- since PS uses FROM_LIVE, it must
            // NOT receive them. This catches accidental replay of pre-live history.
            var firstBatch = GenerateRandomPayloads(2);
            persistentPublication.Persist(firstBatch);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(persistentSubscription.IsReplaying(), Is.False);

            var secondBatch = GenerateRandomPayloads(5);
            persistentPublication.Persist(secondBatch);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(secondBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            using var fastDriver = new EmbeddedMediaDriver();
            using var fastAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(fastDriver.AeronDirectoryName));
            using var fastConsumer = fastAeron.AddSubscription(MdcSubscriptionChannel, TestContexts.StreamId);
            var fastHandler = new CountingFragmentHandler();
            Tests.AwaitConnected(fastConsumer);

            var thirdBatch = new List<byte[]>();
            for (var i = 0; i < 3; i++)
            {
                var batch = GenerateFixedPayloads(32, OneKbMessageSize);
                persistentPublication.Publish(batch);
                thirdBatch.AddRange(batch);
                Tests.ExecuteUntil(
                    () => fastHandler.HasReceivedPayloads(thirdBatch.Count),
                    () => fastConsumer.Poll(fastHandler, 10));
            }

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(1));

            var fourthBatch = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(fourthBatch);

            var expectedCount = secondBatch.Count + thirdBatch.Count + fourthBatch.Count;
            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(expectedCount) && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(2));
            AssertPayloads(fragmentHandler.ReceivedPayloads, secondBatch, thirdBatch, fourthBatch);
        }


        [Test, Timeout(10_000)]
        public void ShouldHandleReplayBeingAheadOfLive()
        {
            int controlPort;
            lock (Random) { controlPort = Random.Next(30_000, 35_000); }
            var pubChannel = $"aeron:udp?control=localhost:{controlPort}|control-mode=dynamic|fc=min";
            var subChannel = $"aeron:udp?control=localhost:{controlPort}|rcv-wnd=4k";

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, pubChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            using var slowDriver = new EmbeddedMediaDriver();
            using var aeron2 = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(slowDriver.AeronDirectoryName));
            using var slowConsumer = aeron2.AddSubscription(subChannel, TestContexts.StreamId);
            Tests.AwaitConnected(slowConsumer);

            persistentPublication.Persist(GenerateFixedPayloads(32, OneKbMessageSize));

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(subChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(32),
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            Tests.ExecuteUntil(() => persistentPublication.ReceiverCount() == 2,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            // Consuming on the slow consumer lets the sender push past the initial 4k window.
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () =>
                {
                    Poll(persistentSubscription, fragmentHandler, 10);
                    slowConsumer.Poll((b, o, l, h) => { }, 10);
                });

            Assert.That(persistentSubscription.JoinDifference(), Is.LessThanOrEqualTo(0));
        }

        [Test, Timeout(60_000)]
        public void ShouldReceiveAllMessagesWithModerateReplayLoss()
        {
            RunReplayLossTest(100, 0.3);
        }

        [Test, Timeout(90_000)]
        public void ShouldReceiveAllMessagesWithHeavyReplayLoss()
        {
            RunReplayLossTest(50, 0.8);
        }

        private void RunReplayLossTest(int messageCount, double dropRate)
        {
            // PS lives on a separate driver whose receive endpoint runs through the loss generator.
            // The archive (on the main driver) keeps a complete recording; loss only affects what
            // arrives at PS, so NAK/retransmit must restore everything.
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            var messages = GenerateFixedPayloads(messageCount, OneKbMessageSize);
            persistentPublication.Persist(messages);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            lossController.EnableStreamIdFrameDataRandom(
                PersistentSubscriptionCtx.ReplayStreamId(), dropRate);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(messages.Count) && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10), timeoutMs: 50_000);

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
            lossController.DisableStreamIdFrameData();
        }

        [Test, Timeout(20_000)]
        public void CanJoinLiveInTheMiddleOfAFragmentedMessage()
        {
            // Publisher lives on a second driver whose SEND endpoint drops post-match frames.
            // PS lives on the main driver (no loss). Archive records REMOTE from the same MDC
            // channel — but only the first half of the message reaches everyone before sticky
            // drop kicks in. After loss is disabled, NAK/retransmit must deliver the rest.
            using var pubDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var pubAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(pubDriver.AeronDirectoryName));
            using var lossController = new LossGenController(pubAeron);

            const int maxPayloadLength = 1408 - 32;
            var firstHalfOfMessage = new byte[maxPayloadLength];
            Array.Fill(firstHalfOfMessage, (byte)1);
            var secondHalfOfMessage = new byte[maxPayloadLength];
            Array.Fill(secondHalfOfMessage, (byte)2);
            var largeMessage = new byte[firstHalfOfMessage.Length + secondHalfOfMessage.Length];
            Array.Copy(firstHalfOfMessage, 0, largeMessage, 0, firstHalfOfMessage.Length);
            Array.Copy(secondHalfOfMessage, 0, largeMessage,
                firstHalfOfMessage.Length, secondHalfOfMessage.Length);

            using var exclusivePublication = pubAeron.AddExclusivePublication(
                MdcPublicationChannel, TestContexts.StreamId);
            AeronArchive.StartRecording(
                MdcPublicationChannel, TestContexts.StreamId, Adaptive.Archiver.Codecs.SourceLocation.REMOTE);
            Tests.Await(() => exclusivePublication.IsConnected);

            var persistentPublication = PersistentPublication.Create(AeronArchive, exclusivePublication);
            Closeables.Add(persistentPublication);

            lossController.EnableFrameDataPayloadSticky(secondHalfOfMessage);
            persistentPublication.Publish(new System.Collections.Generic.List<byte[]> { largeMessage });

            PersistentSubscriptionCtx
                .LiveChannel(MdcSubscriptionChannel)
                .LiveStreamId(TestContexts.StreamId)
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_START);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(0));

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(0));

            lossController.DisableFrameData();

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(1),
                () => Poll(persistentSubscription, fragmentHandler, 1));
            AssertPayloads(fragmentHandler.ReceivedPayloads,
                new System.Collections.Generic.List<byte[]> { largeMessage });
        }

        [Test, Timeout(20_000)]
        public void CanFallbackToReplayInTheMiddleOfAFragmentedMessage()
        {
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var maxPayloadLength = persistentPublication.MaxPayloadLength;
            var firstHalfOfMessage = new byte[maxPayloadLength];
            Array.Fill(firstHalfOfMessage, (byte)1);
            var secondHalfOfMessage = new byte[maxPayloadLength];
            Array.Fill(secondHalfOfMessage, (byte)2);
            var largeMessage = new byte[firstHalfOfMessage.Length + secondHalfOfMessage.Length];
            Array.Copy(firstHalfOfMessage, 0, largeMessage, 0, firstHalfOfMessage.Length);
            Array.Copy(secondHalfOfMessage, 0, largeMessage,
                firstHalfOfMessage.Length, secondHalfOfMessage.Length);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .LiveChannel(MdcSubscriptionChannel)
                .LiveStreamId(TestContexts.StreamId)
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            // Sticky drop starting from the first frame whose payload matches the second half:
            // PS receives the first fragment, then loses the rest of the stream. It must
            // fall back to replay and refetch the complete message from the archive.
            lossController.EnableStreamIdFrameDataPayloadSticky(TestContexts.StreamId, secondHalfOfMessage);

            persistentPublication.Publish(new System.Collections.Generic.List<byte[]> { largeMessage });

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(0));

            lossController.DisableStreamIdFrameData();

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            AssertPayloads(fragmentHandler.ReceivedPayloads,
                new System.Collections.Generic.List<byte[]> { largeMessage });
        }

        [Test, Timeout(20_000)]
        public void CanSwitchFromReplayToLiveWhenLivePositionIsAheadOfReplayPosition()
        {
            // Long image-liveness timeout: stream-id loss freezes replay-channel DATA for the
            // duration of the catchup phase. With the standard 2s timeout used by other tests,
            // the replay image times out under system load → CleanUpLiveSubscription discards
            // the eager-add live subscription → PS falls through to AWAIT_LIVE which sets
            // joinDifference(0), defeating the REPLAY → ATTEMPT_SWITCH path this test verifies.
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true,
                imageLivenessTimeout: "30s");
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(persistentPublication.RecordingId);

            var messagesToConsumeOnReplay = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(messagesToConsumeOnReplay);
            var initialStopPosition = persistentPublication.Position;

            // Block publisher SETUPs at snd_pos=5KB. Publisher keeps retrying; every retry until
            // we publish more carries this same tuple and is dropped, so the eager-add live image
            // can't attach during the replay phase.
            ArmSetupDropAtPosition(lossController, persistentPublication.RecordingId, initialStopPosition);
            lossController.EnableSetupAtPosition();

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(1),
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount),
                () =>
                {
                    Poll(persistentSubscription, fragmentHandler, 1);
                    Assert.That(persistentSubscription.IsReplaying(), Is.True);
                });

            // Stop replay from advancing.
            lossController.EnableStreamId(PersistentSubscriptionCtx.ReplayStreamId());

            // Send 2 more so live position advances ahead of replay. Publisher's next SETUP now
            // carries the advanced snd_pos (no longer matches drop target) and is delivered,
            // attaching the live image at 7KB while replay is frozen at 5KB by the streamId loss.
            var messagesToConsumeAfterAddingLive = GenerateFixedPayloads(2, OneKbMessageSize);
            persistentPublication.Publish(messagesToConsumeAfterAddingLive);

            Tests.ExecuteUntil(
                () => persistentSubscription.JoinDifference() != long.MinValue,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            lossController.DisableStreamId();
            lossController.DisableSetupAtPosition();

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount)
                      && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.JoinDifference(), Is.EqualTo(2048));

            var messagesToConsumeOnLive = GenerateFixedPayloads(2, OneKbMessageSize);
            persistentPublication.Publish(messagesToConsumeOnLive);
            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsLive(), Is.True);
            AssertPayloads(
                fragmentHandler.ReceivedPayloads,
                messagesToConsumeOnReplay, messagesToConsumeAfterAddingLive, messagesToConsumeOnLive);
        }

        [Test, Timeout(60_000)]
        public void ShouldReplayAndCatchUpWhenExtendedRecordingIsAheadOfLivePosition()
        {
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            persistentPublication.Persist(GenerateFixedPayloads(1, OneKbMessageSize));
            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));
            var recordingId = persistentPublication.RecordingId;

            persistentPublication.ClosePublicationOnly();
            Tests.Await(() => !persistentPublication.PublicationCountersExist());

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(recordingId)
                .StartPosition(stopPosition)
                .LiveChannel(MdcSubscriptionChannel)
                .AeronArchiveContext().MessageTimeoutNs(15_000_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            var observedReplaying = new[] { false };
            Action pollAndTrack = () =>
            {
                Poll(persistentSubscription, fragmentHandler, 10);
                if (persistentSubscription.IsReplaying())
                {
                    observedReplaying[0] = true;
                }
            };

            // Wait for one AWAIT_LIVE deadline breach.
            Tests.ExecuteUntil(() => Listener.ErrorCount > 0, pollAndTrack, 30_000);
            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(persistentSubscription.IsLive(), Is.False);
            Assert.That(observedReplaying[0], Is.False);

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId, recordingId);

            // Publish first then wait for receipt — ensures the live image is attached before
            // revoke, otherwise PS could be stuck in AWAIT_LIVE.
            var firstBatch = GenerateFixedPayloads(1, OneKbMessageSize);
            resumedPublication.Persist(firstBatch);
            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(firstBatch.Count), pollAndTrack);
            Tests.ExecuteUntil(persistentSubscription.IsLive, pollAndTrack);

            // Reset observedReplaying — only post-revoke catchup-refresh should flip it back true.
            observedReplaying[0] = false;

            ArmDataDropFromPosition(lossController, recordingId, stopPosition + 1024L);
            lossController.EnableDataInRange();

            // Recording sees all 40; PS's live image sees none (dropped on PS's endpoint).
            var catchupMessages = GenerateFixedPayloads(40, OneKbMessageSize);
            resumedPublication.Persist(catchupMessages);

            // Revoke ends the live image; after EOS+REVOKED heartbeat, the dropped bytes are
            // gone from the live channel and PS must replay them.
            resumedPublication.ExclusivePub.Revoke();

            var expected = new System.Collections.Generic.List<byte[]>(firstBatch);
            expected.AddRange(catchupMessages);
            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(expected.Count), pollAndTrack);

            lossController.DisableDataInRange();

            AssertPayloads(fragmentHandler.ReceivedPayloads, expected);
            Assert.That(observedReplaying[0], Is.True,
                "PS did not transition through REPLAY/ATTEMPT_SWITCH after revoke");
            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
        }

        private void ArmDataDropFromPosition(LossGenController controller, long recordingId, long fromPosition)
        {
            var descriptor = new ResumeDescriptorReader();
            AeronArchive.ListRecording(recordingId, descriptor);
            var initialTermId = descriptor.InitialTermId;
            var termBufferLength = descriptor.TermBufferLength;
            var positionBitsToShift = Adaptive.Aeron.LogBuffer.LogBufferDescriptor.PositionBitsToShift(
                termBufferLength);
            var targetActiveTermId = Adaptive.Aeron.LogBuffer.LogBufferDescriptor.ComputeTermIdFromPosition(
                fromPosition, positionBitsToShift, initialTermId);
            var targetTermOffsetMin = (int)(fromPosition & (termBufferLength - 1L));
            controller.SetDataInRangeTarget(
                TestContexts.StreamId, targetActiveTermId, targetTermOffsetMin, termBufferLength);
        }

        [Test, Timeout(30_000)]
        public void ShouldRefreshAndReplayWhenLiveAheadOfStopPositionAfterResume()
        {
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            persistentPublication.Persist(GenerateFixedPayloads(1, OneKbMessageSize));
            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));
            var recordingId = persistentPublication.RecordingId;

            ArmSetupDropAtPosition(lossController, recordingId, stopPosition);

            persistentPublication.ClosePublicationOnly();
            Tests.Await(() => !persistentPublication.PublicationCountersExist());

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(recordingId)
                .StartPosition(stopPosition)
                .LiveChannel(MdcSubscriptionChannel)
                .AeronArchiveContext().MessageTimeoutNs(5_000_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            var observedReplaying = new[] { false };
            Action pollAndTrack = () =>
            {
                Poll(persistentSubscription, fragmentHandler, 10);
                if (persistentSubscription.IsReplaying())
                {
                    observedReplaying[0] = true;
                }
            };

            // Wait for one AWAIT_LIVE deadline breach.
            Tests.ExecuteUntil(() => Listener.ErrorCount > 0, pollAndTrack);
            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
            Assert.That(Listener.LastException.Message,
                Does.Contain("No image became available on the live subscription"));
            Assert.That(persistentSubscription.IsLive(), Is.False);
            Assert.That(observedReplaying[0], Is.False);

            lossController.EnableSetupAtPosition();

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);

            var postResumeMessages = GenerateFixedPayloads(4, OneKbMessageSize);
            resumedPublication.Persist(postResumeMessages);

            Tests.ExecuteUntil(persistentSubscription.IsLive, pollAndTrack);
            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(postResumeMessages.Count), pollAndTrack);
            AssertPayloads(fragmentHandler.ReceivedPayloads, postResumeMessages);

            lossController.DisableSetupAtPosition();

            Assert.That(observedReplaying[0], Is.True,
                "PS did not transition through REPLAY/ATTEMPT_SWITCH; refresh path was not exercised");
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));
            Assert.That(Listener.ErrorCount, Is.EqualTo(1));
        }

        [TestCase(1), TestCase(10), Timeout(20_000)]
        public void ShouldReplayExistingRecordingThenJoinLive(int fragmentLimit)
        {
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var replayMessages = GenerateRandomPayloads(5);
            persistentPublication.Persist(replayMessages);
            var stopPosition = persistentPublication.Position;

            // Drop publisher's SETUP at its current snd_pos on PS's endpoint. Publisher is idle
            // through the replay phase so snd_pos doesn't advance — every SETUP carries the same
            // tuple and gets dropped until we disable the generator.
            ArmSetupDropAtPosition(lossController, persistentPublication.RecordingId, stopPosition);
            lossController.EnableSetupAtPosition();

            AeronArchive.StopRecording(persistentPublication.ExclusivePub);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(persistentPublication.RecordingId);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, fragmentLimit));
            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(replayMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, fragmentLimit));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(0));

            // Wait for PS to leave REPLAY/ATTEMPT_SWITCH — proves closed-image cleanup path ran.
            Tests.ExecuteUntil(() => !persistentSubscription.IsReplaying(),
                () => Poll(persistentSubscription, fragmentHandler, fragmentLimit));

            // Re-allow SETUPs; the next one carries the live image at snd_pos == stopPosition,
            // matching PS's current position so AWAIT_LIVE → LIVE with joinDifference == 0.
            lossController.DisableSetupAtPosition();

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, fragmentLimit));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(replayMessages.Count));

            var liveMessages = GenerateRandomPayloads(15);
            persistentPublication.Publish(liveMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(replayMessages.Count + liveMessages.Count),
                () =>
                {
                    Poll(persistentSubscription, fragmentHandler, fragmentLimit);
                    Assert.That(persistentSubscription.IsLive(), Is.True);
                });

            Assert.That(persistentSubscription.IsReplaying(), Is.False);
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));
            Assert.That(persistentSubscription.JoinDifference(), Is.EqualTo(0));
            AssertPayloads(fragmentHandler.ReceivedPayloads, replayMessages, liveMessages);
        }

        private void ArmSetupDropAtPosition(LossGenController controller, long recordingId, long position)
        {
            var descriptor = new ResumeDescriptorReader();
            AeronArchive.ListRecording(recordingId, descriptor);
            var initialTermId = descriptor.InitialTermId;
            var termBufferLength = descriptor.TermBufferLength;
            var positionBitsToShift = Adaptive.Aeron.LogBuffer.LogBufferDescriptor.PositionBitsToShift(
                termBufferLength);
            var targetActiveTermId = Adaptive.Aeron.LogBuffer.LogBufferDescriptor.ComputeTermIdFromPosition(
                position, positionBitsToShift, initialTermId);
            var targetTermOffset = (int)(position & (termBufferLength - 1L));
            controller.SetSetupAtPositionTarget(
                TestContexts.StreamId, initialTermId, targetActiveTermId, targetTermOffset);
        }

        private sealed class ResumeDescriptorReader : IRecordingDescriptorConsumer
        {
            public int InitialTermId { get; private set; }
            public int TermBufferLength { get; private set; }

            public void OnRecordingDescriptor(
                long controlSessionId, long correlationId, long recordingId, long startTimestamp,
                long stopTimestamp, long startPosition, long stopPosition, int initialTermId,
                int segmentFileLength, int termBufferLength, int mtuLength, int sessionId,
                int streamId, string strippedChannel, string originalChannel, string sourceIdentity)
            {
                InitialTermId = initialTermId;
                TermBufferLength = termBufferLength;
            }
        }

        [Test, Timeout(20_000)]
        public void ShouldRejoinLiveEvenIfNoFragmentsHaveBeenConsumedAfterJoiningFromLive()
        {
            int controlPort;
            lock (Random) { controlPort = Random.Next(40_000, 45_000); }
            var pubChannel =
                $"aeron:udp?term-length=16m|control=localhost:{controlPort}|control-mode=dynamic|fc=min";
            var subChannel = $"aeron:udp?control=localhost:{controlPort}|group=true";

            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, pubChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            int unconsumedCount;
            lock (Random) { unconsumedCount = Random.Next(0, 3); }
            var oldMessages = GenerateRandomPayloads(unconsumedCount);
            if (oldMessages.Count > 0)
            {
                persistentPublication.Persist(oldMessages);
            }

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(subChannel)
                .StartPosition(PersistentSubscription.FROM_LIVE);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(0));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));

            lossController.EnableStreamId(PersistentSubscriptionCtx.LiveStreamId());

            Tests.ExecuteUntil(() => !persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(1));

            lossController.DisableStreamId();

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(0));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(2));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(1));

            var payloads = GenerateRandomPayloads(3);
            persistentPublication.Persist(payloads);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(payloads.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));
            AssertPayloads(fragmentHandler.ReceivedPayloads, payloads);
        }

        [Test, Timeout(60_000)]
        public void ShouldReceiveAllMessagesWithLossOnLiveChannel()
        {
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            var initialMessages = GenerateFixedPayloads(5, OneKbMessageSize);
            persistentPublication.Persist(initialMessages);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            // 30% drop on the live stream-id, set up BEFORE PS reaches live so SETUPs go through
            // but post-attach DATA frames are sampled for drop.
            lossController.EnableStreamIdFrameDataRandom(TestContexts.StreamId, 0.3);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var liveMessages = GenerateFixedPayloads(30, OneKbMessageSize);
            persistentPublication.Publish(liveMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(initialMessages.Count + liveMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10), timeoutMs: 50_000);

            AssertPayloads(fragmentHandler.ReceivedPayloads, initialMessages, liveMessages);
            lossController.DisableStreamIdFrameData();
        }

        [Test, Timeout(60_000)]
        public void ShouldTransitionToLiveThroughLossyReplayWhilePublishing()
        {
            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            var initialMessages = GenerateFixedPayloads(20, OneKbMessageSize);
            persistentPublication.Persist(initialMessages);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            lossController.EnableStreamIdFrameDataRandom(
                PersistentSubscriptionCtx.ReplayStreamId(), 0.5);

            // Drain a few before publishing more — PS will still be catching up via lossy replay.
            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(initialMessages.Count / 2),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var liveMessages = GenerateFixedPayloads(20, OneKbMessageSize);
            persistentPublication.Publish(liveMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(initialMessages.Count + liveMessages.Count)
                      && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10), timeoutMs: 50_000);

            AssertPayloads(fragmentHandler.ReceivedPayloads, initialMessages, liveMessages);
            lossController.DisableStreamIdFrameData();
        }

        [Test, Timeout(60_000)]
        public void ShouldReconnectToTheArchiveAfterArchiveRestart()
        {
            var remoteAeronDir = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(), "aeron-remote-" + Guid.NewGuid().ToString("N"));
            var remoteDriver = new EmbeddedMediaDriver(remoteAeronDir);
            Closeables.Add(remoteDriver);
            var remoteAeron = AeronClient.Connect(new AeronClient.Context().AeronDirectoryName(remoteAeronDir));
            Closeables.Add(remoteAeron);

            int archivePort;
            lock (Random) { archivePort = Random.Next(35_000, 40_000); }
            var archiveControlChannel = $"aeron:udp?endpoint=localhost:{archivePort}";
            var remoteArchiveDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(),
                "aeron-archive-remote-" + Guid.NewGuid().ToString("N"));

            var remoteArchive = new EmbeddedArchive(remoteAeronDir, remoteArchiveDir,
                deleteArchiveOnStart: false, controlChannel: archiveControlChannel, aeronClient: remoteAeron);
            Closeables.Add(remoteArchive);

            var remoteAeronArchiveCtx = new AeronArchive.Context()
                .ControlRequestChannel(archiveControlChannel)
                .ControlResponseChannel(TestContexts.LocalhostControlResponseChannel)
                .AeronClient(remoteAeron);
            var remoteAeronArchive = AeronArchive.Connect(remoteAeronArchiveCtx.Clone());
            Closeables.Add(remoteAeronArchive);

            using var exclusivePublication = Aeron.AddExclusivePublication(
                MdcPublicationChannel, TestContexts.StreamId);
            remoteAeronArchive.StartRecording(
                MdcSubscriptionChannel, TestContexts.StreamId, Adaptive.Archiver.Codecs.SourceLocation.REMOTE);
            Tests.Await(() => exclusivePublication.IsConnected);

            var persistentPublication = PersistentPublication.Create(remoteAeronArchive, exclusivePublication);
            Closeables.Add(persistentPublication);

            var firstBatch = GenerateFixedPayloads(1, OneKbMessageSize);
            var secondBatch = GenerateFixedPayloads(1, OneKbMessageSize);
            var thirdBatch = GenerateFixedPayloads(64, OneKbMessageSize);

            persistentPublication.Persist(firstBatch);

            PersistentSubscriptionCtx
                .Aeron(Aeron)
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(persistentPublication.RecordingId)
                .AeronArchiveContext(
                    new AeronArchive.Context()
                        .ControlRequestChannel(archiveControlChannel)
                        .ControlResponseChannel(TestContexts.LocalhostControlResponseChannel)
                        .AeronClient(Aeron))
                .StartPosition(PersistentSubscription.FROM_START);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1), timeoutMs: 30_000);

            Assert.That(fragmentHandler.ReceivedPayloads.Count,
                Is.EqualTo(persistentPublication.PublishedMessageCount));

            persistentPublication.Persist(secondBatch);
            persistentPublication.Persist(thirdBatch);

            DisposeWithTimeout(remoteAeronArchive, 3_000, "remoteAeronArchive");
            DisposeWithTimeout(remoteAeron, 3_000, "remoteAeron");
            remoteArchive.KillProcess();
            remoteDriver.Dispose();

            // Restart remote driver, aeron, and archive at the same paths.
            var remoteDriver2 = new EmbeddedMediaDriver(remoteAeronDir);
            Closeables.Add(remoteDriver2);
            var remoteAeron2 = AeronClient.Connect(new AeronClient.Context().AeronDirectoryName(remoteAeronDir));
            Closeables.Add(remoteAeron2);
            var remoteArchive2 = new EmbeddedArchive(remoteAeronDir, remoteArchiveDir,
                deleteArchiveOnStart: false, controlChannel: archiveControlChannel, aeronClient: remoteAeron2);
            Closeables.Add(remoteArchive2);

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, 10), timeoutMs: 30_000);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10), timeoutMs: 30_000);

            AssertPayloads(fragmentHandler.ReceivedPayloads, firstBatch, secondBatch, thirdBatch);
        }

        [Test, Timeout(60_000)]
        public void ShouldRecoverFromArchiveRestartDuringReplay()
        {
            var remoteAeronDir = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(), "aeron-remote-" + Guid.NewGuid().ToString("N"));
            var remoteDriver = new EmbeddedMediaDriver(remoteAeronDir);
            Closeables.Add(remoteDriver);
            var remoteAeron = AeronClient.Connect(new AeronClient.Context().AeronDirectoryName(remoteAeronDir));
            Closeables.Add(remoteAeron);

            int archivePort;
            lock (Random) { archivePort = Random.Next(35_000, 40_000); }
            var archiveControlChannel = $"aeron:udp?endpoint=localhost:{archivePort}";
            var remoteArchiveDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(),
                "aeron-archive-remote-" + Guid.NewGuid().ToString("N"));

            var remoteArchive = new EmbeddedArchive(remoteAeronDir, remoteArchiveDir,
                deleteArchiveOnStart: false, controlChannel: archiveControlChannel, aeronClient: remoteAeron);
            Closeables.Add(remoteArchive);

            var remoteAeronArchiveCtx = new AeronArchive.Context()
                .ControlRequestChannel(archiveControlChannel)
                .ControlResponseChannel(TestContexts.LocalhostControlResponseChannel)
                .AeronClient(remoteAeron);
            var remoteAeronArchive = AeronArchive.Connect(remoteAeronArchiveCtx.Clone());
            Closeables.Add(remoteAeronArchive);

            using var exclusivePublication = Aeron.AddExclusivePublication(
                MdcPublicationChannel, TestContexts.StreamId);
            remoteAeronArchive.StartRecording(
                MdcSubscriptionChannel, TestContexts.StreamId, Adaptive.Archiver.Codecs.SourceLocation.REMOTE);
            Tests.Await(() => exclusivePublication.IsConnected);

            var persistentPublication = PersistentPublication.Create(remoteAeronArchive, exclusivePublication);
            Closeables.Add(persistentPublication);

            // Publish enough so PS will still be replaying when we kill the archive.
            var messages = GenerateFixedPayloads(80, OneKbMessageSize);
            persistentPublication.Persist(messages);

            PersistentSubscriptionCtx
                .Aeron(Aeron)
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(persistentPublication.RecordingId)
                .AeronArchiveContext(
                    new AeronArchive.Context()
                        .ControlRequestChannel(archiveControlChannel)
                        .ControlResponseChannel(TestContexts.LocalhostControlResponseChannel)
                        .AeronClient(Aeron))
                .StartPosition(PersistentSubscription.FROM_START);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(5),
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            // Kill archive while PS is still replaying.
            DisposeWithTimeout(remoteAeronArchive, 3_000, "remoteAeronArchive");
            DisposeWithTimeout(remoteAeron, 3_000, "remoteAeron");
            remoteArchive.KillProcess();
            remoteDriver.Dispose();

            Tests.ExecuteUntil(() => !persistentSubscription.IsReplaying(),
                () => Poll(persistentSubscription, fragmentHandler, 1), timeoutMs: 30_000);
            Assert.That(persistentSubscription.HasFailed(), Is.False);

            // Restart remote driver, aeron, archive.
            var remoteDriver2 = new EmbeddedMediaDriver(remoteAeronDir);
            Closeables.Add(remoteDriver2);
            var remoteAeron2 = AeronClient.Connect(new AeronClient.Context().AeronDirectoryName(remoteAeronDir));
            Closeables.Add(remoteAeron2);
            var remoteArchive2 = new EmbeddedArchive(remoteAeronDir, remoteArchiveDir,
                deleteArchiveOnStart: false, controlChannel: archiveControlChannel, aeronClient: remoteAeron2);
            Closeables.Add(remoteArchive2);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10), timeoutMs: 30_000);

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
        }

        [TestCase(PersistentSubscription.FROM_START), TestCase(PersistentSubscription.FROM_LIVE)]
        [Timeout(20_000)]
        public void ShouldRetryAndRecoverWhenArchiveIsNotAvailableDuringStartUp(long startPosition)
        {
            // Replace the setUp archive with a local archive whose dir we can preserve across a
            // SIGKILL+restart cycle (deleteArchiveOnStart=false, KillProcess instead of Dispose).
            DisposeWithTimeout(AeronArchive, 3_000, "AeronArchive (setUp)");
            AeronArchive = null;
            Archive.Dispose();

            var localArchiveDir = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "aeron-archive-restart-" + Guid.NewGuid().ToString("N"));
            Archive = new EmbeddedArchive(
                Driver.AeronDirectoryName, localArchiveDir, deleteArchiveOnStart: false, aeronClient: Aeron);
            AeronArchive = AeronArchive.Connect(CloneArchiveCtx());

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            persistentPublication.Persist(GenerateRandomPayloads(1));

            var archiveControlChannel = Archive.ControlRequestChannel;
            var localArchive = Archive;
            Closeables.Add(localArchive);

            DisposeWithTimeout(AeronArchive, 3_000, "AeronArchive (pre-restart)");
            AeronArchive = null;
            localArchive.KillProcess();
            Archive = null;

            // PersistentSubscriptionCtx.AeronArchiveContext was set up in SetUp pointing at the
            // (now-disposed) setUp archive's channels. Re-bind to the local archive's channels —
            // otherwise PS would try to connect on a dead port and retry forever.
            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(startPosition)
                .LiveChannel(MdcSubscriptionChannel)
                .AeronArchiveContext(
                    new AeronArchive.Context()
                        .ControlRequestChannel(archiveControlChannel)
                        .ControlResponseChannel(TestContexts.LocalhostControlResponseChannel)
                        .AeronClient(Aeron)
                        .MessageTimeoutNs(500_000_000L));

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => Listener.ErrorCount > 1,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(Listener.LastException, Is.InstanceOf<TimeoutException>());
            Assert.That(persistentSubscription.HasFailed(), Is.False);
            Assert.That(persistentSubscription.FailureReason(), Is.Null);

            Archive = new EmbeddedArchive(
                Driver.AeronDirectoryName, localArchiveDir, deleteArchiveOnStart: false,
                controlChannel: archiveControlChannel, aeronClient: Aeron);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));
        }

        [Test, Timeout(15_000)]
        public void ShouldCloseArchiveConnectionOnFailureInCaseApplicationKeepsPolling()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            // Misaligned start position to force PS into FAILED state.
            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(8192);

            var fragmentHandler = new BufferingFragmentHandler();

            // Capture baseline before PS opens its own archive session (baseline includes the
            // test's AeronArchive session and any infrastructure sessions such as the probe).
            var baselineSessionCount = ReadArchiveControlSessionsCount();

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.HasFailed,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            // After FAILED, PS's SetState(FAILED) disposes its _asyncAeronArchive, which sends
            // a close request to the archive. Session count must return to baseline.
            Tests.ExecuteUntil(
                () => ReadArchiveControlSessionsCount() == baselineSessionCount,
                () => Poll(persistentSubscription, fragmentHandler, 1));
        }

        private long ReadArchiveControlSessionsCount()
        {
            var counters = Aeron.CountersReader;
            long value = -1;
            counters.ForEach((counterId, typeId, keyBuffer, label) =>
            {
                if (typeId == Adaptive.Aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID)
                {
                    value = counters.GetCounterValue((int)counterId);
                }
            });
            return value;
        }

        [Test, Timeout(15_000)]
        public void ShouldContinueConsumingFromLiveWhileArchiveIsUnavailable()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var firstBatch = GenerateRandomPayloads(5);
            var secondBatch = GenerateRandomPayloads(5);
            persistentPublication.Persist(firstBatch);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount)
                      && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            // Kill the archive JVM. PS is already in LIVE so its archive control session is idle;
            // it should keep consuming the live channel without noticing.
            DisposeWithTimeout(AeronArchive, 3_000, "AeronArchive (mid-test)");
            AeronArchive = null;
            Archive.Dispose();
            Archive = null;

            // publish() goes via the local ExclusivePublication on the driver — the archive
            // process is not in the path, so this still works.
            persistentPublication.Publish(secondBatch);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, firstBatch, secondBatch);
        }

        [Test, Timeout(20_000)]
        public void ShouldRecoverWhenThePersistentPublicationIsRestartedDuringReplay()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            var recordedBatch = GenerateRandomPayloads(1);
            persistentPublication.Persist(recordedBatch);
            var recordingId = persistentPublication.RecordingId;

            // Pre-create the state counter so the test can poll it directly. Java accesses
            // the auto-created one via persistentSubscription.context().stateCounter() which
            // .NET doesn't expose; supplying our own is equivalent.
            using var stateCounter = Aeron.AddCounter(
                Adaptive.Aeron.AeronCounters.PERSISTENT_SUBSCRIPTION_STATE_TYPE_ID,
                "test PS state");

            PersistentSubscriptionCtx
                .StartPosition(PersistentSubscription.FROM_START)
                .RecordingId(recordingId)
                .StateCounter(stateCounter);
            PersistentSubscriptionCtx
                .AeronArchiveContext().MessageTimeoutNs(500_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(persistentPublication.PublishedMessageCount),
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            persistentPublication.Dispose();

            // Wait for PS to reach end of recording and transition to AWAIT_LIVE (state 15).
            Tests.ExecuteUntil(() => stateCounter.Get() == 15,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var batchAfterResuming = GenerateRandomPayloads(5);
            resumedPublication.Persist(batchAfterResuming);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(batchAfterResuming.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, recordedBatch, batchAfterResuming);
        }

        [TestCase("aeron:udp?endpoint=localhost:0", -10)]
        [TestCase("aeron:ipc", -12)]
        [Timeout(15_000)]
        public void ShouldReplayOverConfiguredChannel(string replayChannel, int replayStreamId)
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var payloads = GenerateRandomPayloads(5);
            persistentPublication.Persist(payloads);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .ReplayChannel(replayChannel)
                .ReplayStreamId(replayStreamId);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(1),
                () => Poll(persistentSubscription, fragmentHandler, 1));

            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, payloads);
        }

        [TestCase(1, "ipc"), TestCase(10, "ipc"), TestCase(int.MaxValue, "ipc")]
        [TestCase(1, "multicast"), TestCase(10, "multicast"), TestCase(int.MaxValue, "multicast")]
        [TestCase(1, "spy"), TestCase(10, "spy"), TestCase(int.MaxValue, "spy")]
        [Timeout(15_000)]
        public void ShouldConsumeLiveOverConfiguredChannel(int fragmentLimit, string channelKind)
        {
            string subChannel, pubChannel;
            switch (channelKind)
            {
                case "ipc":
                    subChannel = pubChannel = TestContexts.IpcChannel;
                    break;
                case "multicast":
                    subChannel = pubChannel = TestContexts.MulticastChannel;
                    break;
                case "spy":
                    subChannel = AeronClient.Context.SPY_PREFIX + MdcPublicationChannel;
                    pubChannel = MdcPublicationChannel;
                    break;
                default:
                    throw new System.ArgumentException(channelKind);
            }

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, pubChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(subChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, fragmentLimit));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));

            var payloads = GenerateRandomPayloads(5);
            persistentPublication.Persist(payloads);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(1),
                () => Poll(persistentSubscription, fragmentHandler, 1));

            Assert.That(persistentSubscription.IsLive(), Is.True);
            Assert.That(persistentSubscription.IsReplaying(), Is.False);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(payloads.Count),
                () => Poll(persistentSubscription, fragmentHandler, fragmentLimit));

            AssertPayloads(fragmentHandler.ReceivedPayloads, payloads);
        }

        [TestCase("unicast"), TestCase("ipc"), Timeout(30_000)]
        public void AnUntetheredPersistentSubscriptionCanFallBehindATetheredSubscription(string channelKind)
        {
            string channel;
            if (channelKind == "unicast")
            {
                int port;
                lock (Random) { port = Random.Next(30_000, 35_000); }
                channel = $"aeron:udp?endpoint=localhost:{port}";
            }
            else
            {
                channel = TestContexts.IpcChannel;
            }
            var channelUriStringBuilder = new Adaptive.Aeron.ChannelUriStringBuilder(channel);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, channel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(channelUriStringBuilder.Tether(false).Build());

            var fragmentHandler = new BufferingFragmentHandler();
            var fastHandler = new CountingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            using var fastSubscription = Aeron.AddSubscription(
                new Adaptive.Aeron.ChannelUriStringBuilder(channel).Tether(true).Build(),
                TestContexts.StreamId);
            Tests.AwaitConnected(fastSubscription);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            persistentPublication.Persist(GenerateFixedPayloads(32, OneKbMessageSize));
            Tests.ExecuteUntil(() => fastHandler.HasReceivedPayloads(32),
                () => fastSubscription.Poll(fastHandler, 10));

            persistentPublication.Persist(GenerateFixedPayloads(32, OneKbMessageSize));
            Tests.ExecuteUntil(() => fastHandler.HasReceivedPayloads(64),
                () => fastSubscription.Poll(fastHandler, 10));

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(64),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsReplaying(), Is.True);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));
        }

        [Test, Timeout(30_000)]
        public void UntetheredSpyCanFallbackToReplay()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_LIVE)
                .LiveChannel(AeronClient.Context.SPY_PREFIX + MdcPublicationChannel + "|tether=false");

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));
            Assert.That(fragmentHandler.ReceivedPayloads.Count, Is.EqualTo(0));

            using var fastDriver = new EmbeddedMediaDriver();
            using var fastAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(fastDriver.AeronDirectoryName));
            using var fastConsumer = fastAeron.AddSubscription(MdcSubscriptionChannel, TestContexts.StreamId);
            var fastHandler = new CountingFragmentHandler();
            Tests.AwaitConnected(fastConsumer);

            var firstBatch = new List<byte[]>();
            for (var i = 0; i < 3; i++)
            {
                var batch = GenerateFixedPayloads(32, OneKbMessageSize);
                persistentPublication.Publish(batch);
                firstBatch.AddRange(batch);
                Tests.ExecuteUntil(() => fastHandler.HasReceivedPayloads(firstBatch.Count),
                    () => fastConsumer.Poll(fastHandler, 10));
            }

            Tests.ExecuteUntil(persistentSubscription.IsReplaying,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(1));

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(firstBatch.Count) && persistentSubscription.IsLive(),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, firstBatch);
        }

        [Test, Timeout(30_000)]
        public void ShouldCatchUpWhenStartingAtStopPositionAndRecordingResumes()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);

            persistentPublication.Persist(GenerateRandomPayloads(1));
            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));
            var recordingId = persistentPublication.RecordingId;
            persistentPublication.ClosePublicationOnly();

            PersistentSubscriptionCtx
                .RecordingId(recordingId)
                .StartPosition(stopPosition)
                .AeronArchiveContext().MessageTimeoutNs(5_000_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => Listener.ErrorCount > 0,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(Listener.LastException.Message,
                Does.Contain("No image became available on the live subscription"));
            Assert.That(persistentSubscription.IsLive(), Is.False);

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);
            var messages = GenerateRandomPayloads(3);
            resumedPublication.Persist(messages);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(messages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
        }

        [Test, Timeout(15_000)]
        public void ShouldCatchUpWhenStartingAtStopPositionOfExtendedRecording()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);

            persistentPublication.Persist(GenerateRandomPayloads(1));
            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));
            var recordingId = persistentPublication.RecordingId;
            persistentPublication.ClosePublicationOnly();

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);
            var catchupMessages = GenerateRandomPayloads(3);
            resumedPublication.Persist(catchupMessages);

            PersistentSubscriptionCtx
                .RecordingId(recordingId)
                .StartPosition(stopPosition)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(catchupMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var liveMessages = GenerateRandomPayloads(2);
            resumedPublication.Persist(liveMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(catchupMessages.Count + liveMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, catchupMessages, liveMessages);
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
        }

        [Test, Timeout(30_000)]
        public void ShouldJoinLiveUponReachingEndOfRecordingWhenLiveBecomesAvailable()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);

            var oldMessages = GenerateFixedPayloads(8, OneKbMessageSize);
            persistentPublication.Persist(oldMessages);

            persistentPublication.Dispose();
            Tests.Await(() => !persistentPublication.PublicationCountersExist());

            var recordingId = persistentPublication.RecordingId;
            PersistentSubscriptionCtx
                .LiveChannel(MdcSubscriptionChannel)
                .RecordingId(recordingId)
                .AeronArchiveContext().MessageTimeoutNs(5_000_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(oldMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, oldMessages);

            Tests.ExecuteUntil(() => Listener.ErrorCount > 0,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LastException.Message,
                Does.Contain("No image became available on the live subscription"));
            Assert.That(persistentSubscription.HasFailed(), Is.False);
            Assert.That(persistentSubscription.FailureReason(), Is.Null);

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var newMessages = GenerateFixedPayloads(16, OneKbMessageSize);
            resumedPublication.Persist(newMessages);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(oldMessages.Count + newMessages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Assert.That(persistentSubscription.IsLive(), Is.True);
            Assert.That(persistentSubscription.IsReplaying(), Is.False);
            AssertPayloads(fragmentHandler.ReceivedPayloads, oldMessages, newMessages);
        }

        [Test, Timeout(20_000)]
        public void ShouldHandOffToLiveWhenReplayCatchesUpAtPublisherJoinPosition()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var recordedBatch = GenerateFixedPayloads(3, OneKbMessageSize);
            persistentPublication.Persist(recordedBatch);
            var stopPosition = persistentPublication.Stop();
            Assert.That(stopPosition, Is.GreaterThan(0));
            var recordingId = persistentPublication.RecordingId;
            persistentPublication.ClosePublicationOnly();

            // Wait for residual state on publisher A to drain before bringing B up at the same position.
            Tests.Await(() => !persistentPublication.PublicationCountersExist());

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);

            PersistentSubscriptionCtx
                .RecordingId(recordingId)
                .StartPosition(PersistentSubscription.FROM_START)
                .LiveChannel(MdcSubscriptionChannel);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(recordedBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));
            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));

            var liveBatch = GenerateFixedPayloads(3, OneKbMessageSize);
            resumedPublication.Persist(liveBatch);

            Tests.ExecuteUntil(
                () => fragmentHandler.HasReceivedPayloads(recordedBatch.Count + liveBatch.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, recordedBatch, liveBatch);
        }

        [Test, Timeout(30_000)]
        public void ShouldRecoverWhenThePersistentPublicationIsRestartedWhileOnLive()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            persistentPublication.Persist(GenerateRandomPayloads(1));
            var recordingId = persistentPublication.RecordingId;

            PersistentSubscriptionCtx
                .StartPosition(PersistentSubscription.FROM_LIVE)
                .RecordingId(recordingId)
                .AeronArchiveContext().MessageTimeoutNs(5_000_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.Dispose();

            Tests.ExecuteUntil(() => Listener.ErrorCount > 0,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(Listener.LastException.Message,
                Does.Contain("No image became available on the live subscription"));
            Assert.That(persistentSubscription.IsLive(), Is.False);

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var messages = GenerateRandomPayloads(5);
            resumedPublication.Persist(messages);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(messages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
        }

        [Test, Timeout(10_000)]
        public void ShouldAssembleMessages()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            var sizeRequiringFragmentation = persistentPublication.MaxPayloadLength + 1;
            var payload0 = GenerateFixedPayloads(1, sizeRequiringFragmentation);
            var payload1 = GenerateFixedPayloads(1, sizeRequiringFragmentation);

            persistentPublication.Persist(payload0);

            PersistentSubscriptionCtx.RecordingId(persistentPublication.RecordingId);
            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 1));

            persistentPublication.Persist(payload1);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(2),
                () => Poll(persistentSubscription, fragmentHandler, 1));

            AssertPayloads(fragmentHandler.ReceivedPayloads, payload0, payload1);
        }

        [Test, Timeout(30_000)]
        public void ShouldRetryAndRecoverWhenLiveIsNotAvailableDuringStartUp()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId);
            persistentPublication.Persist(GenerateRandomPayloads(1));
            var recordingId = persistentPublication.RecordingId;
            persistentPublication.Dispose();

            PersistentSubscriptionCtx
                .StartPosition(PersistentSubscription.FROM_LIVE)
                .RecordingId(recordingId)
                .AeronArchiveContext().MessageTimeoutNs(5_000_000_000L);

            var fragmentHandler = new BufferingFragmentHandler();
            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);

            Tests.ExecuteUntil(() => Listener.ErrorCount > 0,
                () => Poll(persistentSubscription, fragmentHandler, 1));
            Assert.That(Listener.LastException.Message,
                Does.Contain("No image became available on the live subscription"));
            Assert.That(persistentSubscription.HasFailed(), Is.False);
            Assert.That(persistentSubscription.FailureReason(), Is.Null);
            Assert.That(persistentSubscription.IsLive(), Is.False);

            var resumedPublication = PersistentPublication.Resume(
                AeronArchive, TestContexts.IpcChannel, TestContexts.StreamId, recordingId);
            Closeables.Add(resumedPublication);

            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var messages = GenerateRandomPayloads(5);
            resumedPublication.Persist(messages);

            Tests.ExecuteUntil(() => fragmentHandler.HasReceivedPayloads(messages.Count),
                () => Poll(persistentSubscription, fragmentHandler, 10));

            AssertPayloads(fragmentHandler.ReceivedPayloads, messages);
        }

        [Test, Timeout(10_000)]
        public void ShouldCreateOwnAeronInstanceWhenNotSupplied()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            persistentPublication.Persist(GenerateRandomPayloads(2));

            var ctx = new PersistentSubscription.Context()
                .AeronDirectoryName(Driver.AeronDirectoryName)
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_START)
                .LiveChannel(MdcSubscriptionChannel)
                .LiveStreamId(TestContexts.StreamId)
                .ReplayChannel(TestContexts.EphemeralReplayChannel)
                .ReplayStreamId(-5)
                .Listener(Listener)
                .AeronArchiveContext(CloneArchiveCtx());

            var fragmentHandler = new BufferingFragmentHandler();
            var persistentSubscription = PersistentSubscription.Create(ctx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            var ownAeron = ctx.Aeron();
            Assert.That(ownAeron, Is.Not.Null);
            persistentSubscription.Dispose();
            Assert.That(ownAeron.IsClosed, Is.True);
        }

        [Test, Timeout(10_000)]
        public void ShouldNotCloseSuppliedAeronInstance()
        {
            var persistentPublication = PersistentPublication.Create(
                AeronArchive, MdcPublicationChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);
            persistentPublication.Persist(GenerateRandomPayloads(2));

            var ctx = PersistentSubscriptionCtx.Clone()
                .Aeron(Aeron)
                .RecordingId(persistentPublication.RecordingId)
                .StartPosition(PersistentSubscription.FROM_START)
                .LiveChannel(MdcSubscriptionChannel)
                .AeronArchiveContext(CloneArchiveCtx().AeronClient(Aeron));

            var fragmentHandler = new BufferingFragmentHandler();
            var persistentSubscription = PersistentSubscription.Create(ctx);
            Tests.ExecuteUntil(persistentSubscription.IsLive,
                () => Poll(persistentSubscription, fragmentHandler, 10));

            persistentSubscription.Dispose();
            Assert.That(Aeron.IsClosed, Is.False);
        }

        [Test, Timeout(40_000)]
        public void ShouldRecoverFromReplayChannelNetworkProblems()
        {
            ShouldRecoverFromNetworkProblems(NetworkFlow.Replay);
        }

        [Test, Timeout(40_000)]
        public void ShouldRecoverFromLiveChannelNetworkProblems()
        {
            ShouldRecoverFromNetworkProblems(NetworkFlow.Live);
        }

        private enum NetworkFlow { Replay, Live }

        private void ShouldRecoverFromNetworkProblems(NetworkFlow victimFlow)
        {
            int controlPort;
            lock (Random) { controlPort = Random.Next(40_000, 45_000); }
            var pubChannel =
                $"aeron:udp?term-length=16m|control=localhost:{controlPort}|control-mode=dynamic|fc=min";
            var subChannel = $"aeron:udp?control=localhost:{controlPort}|group=true";

            using var lossDriver = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-loss-" + Guid.NewGuid().ToString("N")),
                withLossGenerators: true);
            using var lossAeron = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(lossDriver.AeronDirectoryName));
            using var lossController = new LossGenController(lossAeron);

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, pubChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            PersistentSubscriptionCtx
                .Aeron(lossAeron)
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(subChannel);

            const int ratePerSecond = 10_000;

            using var publisher = new BackgroundPublisher(persistentPublication, ratePerSecond);

            System.Threading.Thread.Sleep(1_000);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            var handler = new MessageVerifier();

            // matches EmbeddedMediaDriver -Daeron.image.liveness.timeout=2s
            const int imageLivenessTimeoutMs = 2_000;
            var lossState = LossState.NotStarted;
            long deadlineTicks = 0;

            while (!persistentSubscription.IsLive())
            {
                if (Poll(persistentSubscription, handler, 10) == 0)
                {
                    Tests.YieldingIdle("failed to transition to live");
                }

                if (victimFlow == NetworkFlow.Replay)
                {
                    if (lossState == LossState.NotStarted && persistentSubscription.IsReplaying())
                    {
                        lossState = LossState.WaitingToStart;
                        deadlineTicks = System.Diagnostics.Stopwatch.GetTimestamp()
                            + TicksFromMillis(500);
                    }

                    if (lossState == LossState.WaitingToStart
                        && System.Diagnostics.Stopwatch.GetTimestamp() - deadlineTicks >= 0)
                    {
                        lossState = LossState.InProgress;
                        deadlineTicks = System.Diagnostics.Stopwatch.GetTimestamp()
                            + TicksFromMillis(imageLivenessTimeoutMs + 200);
                        lossController.EnableStreamId(PersistentSubscriptionCtx.ReplayStreamId());
                    }

                    if (lossState == LossState.InProgress
                        && System.Diagnostics.Stopwatch.GetTimestamp() - deadlineTicks >= 0)
                    {
                        lossState = LossState.Finished;
                        lossController.DisableStreamId();
                    }
                }
            }

            Assert.That(Listener.LiveJoinedCount, Is.EqualTo(1));
            Assert.That(Listener.LiveLeftCount, Is.EqualTo(0));

            if (victimFlow == NetworkFlow.Live)
            {
                lossState = LossState.WaitingToStart;
                deadlineTicks = System.Diagnostics.Stopwatch.GetTimestamp() + TicksFromMillis(500);

                while (true)
                {
                    if (Poll(persistentSubscription, handler, 10) == 0)
                    {
                        Tests.YieldingIdle("interrupted while simulating live channel network problems");
                    }

                    if (lossState == LossState.WaitingToStart
                        && System.Diagnostics.Stopwatch.GetTimestamp() - deadlineTicks >= 0)
                    {
                        lossState = LossState.InProgress;
                        lossController.EnableStreamId(PersistentSubscriptionCtx.LiveStreamId());
                    }

                    if (lossState == LossState.InProgress && !persistentSubscription.IsLive())
                    {
                        Assert.That(Listener.LiveLeftCount, Is.EqualTo(1));
                        lossState = LossState.Finished;
                        lossController.DisableStreamId();
                    }

                    if (lossState == LossState.Finished && persistentSubscription.IsLive())
                    {
                        Assert.That(Listener.LiveJoinedCount, Is.EqualTo(2));
                        break;
                    }
                }
            }

            publisher.Dispose();
            var lastPosition = persistentPublication.Position;
            // Drain the stream so the verifier sees every fragment up to the last published position.
            Tests.ExecuteUntil(() => handler.Position >= lastPosition,
                () => Poll(persistentSubscription, handler, 10), 20_000);
        }

        private enum LossState { NotStarted, WaitingToStart, InProgress, Finished }

        private static long TicksFromMillis(long millis)
        {
            return millis * System.Diagnostics.Stopwatch.Frequency / 1_000L;
        }

        [Test, Timeout(90_000)]
        public void CanJoinLiveWhenLiveAndReplayAreAdvancing()
        {
            int controlPort;
            lock (Random) { controlPort = Random.Next(40_000, 45_000); }
            var pubChannel =
                $"aeron:udp?term-length=16m|control=localhost:{controlPort}|control-mode=dynamic|fc=min";
            var subChannel = $"aeron:udp?control=localhost:{controlPort}|group=true";

            using var driver2 = new EmbeddedMediaDriver(
                System.IO.Path.Combine(System.IO.Path.GetTempPath(), "aeron-d2-" + Guid.NewGuid().ToString("N")));
            using var aeron2 = AeronClient.Connect(
                new AeronClient.Context().AeronDirectoryName(driver2.AeronDirectoryName));

            var persistentPublication = PersistentPublication.Create(
                AeronArchive, pubChannel, TestContexts.StreamId);
            Closeables.Add(persistentPublication);

            using var controlSubscription = Aeron.AddSubscription(subChannel, TestContexts.StreamId);
            Tests.AwaitConnected(controlSubscription);

            PersistentSubscriptionCtx
                .Aeron(aeron2)
                .RecordingId(persistentPublication.RecordingId)
                .LiveChannel(subChannel)
                .Listener(null);

            const int ratePerSecond = 500; // matches Java upstream

            // Background control consumer: simulates a separate live-only subscriber driving
            // ImageAvailable/Unavailable signals, exactly as the Java test does.
            using var controlConsumerCts = new System.Threading.CancellationTokenSource();
            var controlTask = System.Threading.Tasks.Task.Run(() =>
            {
                IFragmentHandler controlHandler = new NoopFragmentHandler();
                while (!controlConsumerCts.IsCancellationRequested)
                {
                    controlSubscription.Poll(controlHandler, 10);
                }
            });

            using var publisher = new BackgroundPublisher(persistentPublication, ratePerSecond);

            System.Threading.Thread.Sleep(1_000);

            using var persistentSubscription = PersistentSubscription.Create(PersistentSubscriptionCtx);
            var handler = new MessageVerifier();

            // Tight poll loop matching the Java upstream.
            var deadline = DateTime.UtcNow.AddSeconds(70);
            while (!persistentSubscription.IsLive())
            {
                Poll(persistentSubscription, handler, 10);
                if (DateTime.UtcNow > deadline)
                {
                    Assert.Fail("PS failed to transition to live within 70s");
                }
            }

            publisher.Dispose();
            var lastPosition = persistentPublication.Position;

            var drainDeadline = DateTime.UtcNow.AddSeconds(20);
            while (handler.Position < lastPosition)
            {
                Poll(persistentSubscription, handler, 10);
                if (DateTime.UtcNow > drainDeadline)
                {
                    Assert.Fail($"failed to drain stream: handler={handler.Position} last={lastPosition}");
                }
            }

            controlConsumerCts.Cancel();
            try { controlTask.Wait(TimeSpan.FromSeconds(5)); } catch { }
        }

        private sealed class NoopFragmentHandler : IFragmentHandler
        {
            public void OnFragment(Adaptive.Agrona.IDirectBuffer buffer, int offset, int length, Header header)
            {
            }
        }
    }
}
