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
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver.Codecs;
using Adaptive.Archiver.IntegrationTests.Infrastructure;

namespace Adaptive.Archiver.IntegrationTests.Helpers
{
    /// <summary>
    /// Port of Java's inner <c>PersistentSubscriptionTest.PersistentPublication</c>:
    /// wraps an AeronArchive + ExclusivePublication + recording counter so tests can
    /// create a recording, offer messages, and wait for them to be archived.
    /// </summary>
    internal sealed class PersistentPublication : IDisposable
    {
        private readonly AeronArchive _aeronArchive;
        private readonly ExclusivePublication _publication;
        private readonly long _recordingId;
        private readonly CountersReader _countersReader;
        private readonly int _recordingCounterId;
        private int _publishedMessageCount;

        private PersistentPublication(
            AeronArchive aeronArchive,
            ExclusivePublication publication,
            long recordingId,
            CountersReader countersReader,
            int recordingCounterId)
        {
            _aeronArchive = aeronArchive;
            _publication = publication;
            _recordingId = recordingId;
            _countersReader = countersReader;
            _recordingCounterId = recordingCounterId;
        }

        public long RecordingId => _recordingId;
        public int MaxPayloadLength => _publication.MaxPayloadLength;
        public long Position => _publication.Position;
        public int PublishedMessageCount => _publishedMessageCount;
        public bool IsConnected => _publication.IsConnected;
        internal ExclusivePublication ExclusivePub => _publication;

        public bool PublicationCountersExist()
        {
            var counterId = _countersReader.FindByTypeIdAndRegistrationId(
                Adaptive.Aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID, _publication.RegistrationId);
            return counterId != Adaptive.Agrona.Concurrent.Status.CountersReader.NULL_COUNTER_ID;
        }

        public long ReceiverCount()
        {
            var counterId = _countersReader.FindByTypeIdAndRegistrationId(
                Adaptive.Aeron.AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID,
                _publication.RegistrationId);
            if (counterId == Adaptive.Agrona.Concurrent.Status.CountersReader.NULL_COUNTER_ID)
            {
                throw new System.InvalidOperationException(
                    "flow-control receivers counter not found for publication "
                    + _publication.RegistrationId);
            }
            return _countersReader.GetCounterValue(counterId);
        }

        public static PersistentPublication Create(AeronArchive aeronArchive, string channel, int streamId)
        {
            var publication = aeronArchive.AddRecordedExclusivePublication(channel, streamId);
            var countersReader = aeronArchive.Ctx().AeronClient().CountersReader;
            var recordingCounterId = Tests.AwaitRecordingCounterId(
                countersReader, publication.SessionId, aeronArchive.ArchiveId());
            var recordingId = RecordingPos.GetRecordingId(countersReader, recordingCounterId);

            return new PersistentPublication(
                aeronArchive, publication, recordingId, countersReader, recordingCounterId);
        }

        public static PersistentPublication Create(AeronArchive aeronArchive, ExclusivePublication publication)
        {
            var countersReader = aeronArchive.Ctx().AeronClient().CountersReader;
            var recordingCounterId = Tests.AwaitRecordingCounterId(
                countersReader, publication.SessionId, aeronArchive.ArchiveId());
            var recordingId = RecordingPos.GetRecordingId(countersReader, recordingCounterId);

            return new PersistentPublication(
                aeronArchive, publication, recordingId, countersReader, recordingCounterId);
        }

        public static PersistentPublication Resume(
            AeronArchive aeronArchive, string channel, int streamId, long recordingId)
        {
            var channelUriBuilder = new ChannelUriStringBuilder(channel);
            var descriptor = new ResumeDescriptor();
            aeronArchive.ListRecording(recordingId, descriptor);
            channelUriBuilder.InitialPosition(
                descriptor.StopPosition, descriptor.InitialTermId, descriptor.TermBufferLength);

            var aeron = aeronArchive.Ctx().AeronClient();
            var channelUri = channelUriBuilder.Build();
            var publication = aeron.AddExclusivePublication(channelUri, streamId);
            aeronArchive.ExtendRecording(recordingId, channelUri, streamId, SourceLocation.LOCAL);

            var countersReader = aeron.CountersReader;
            var recordingCounterId = Tests.AwaitRecordingCounterId(
                countersReader, publication.SessionId, aeronArchive.ArchiveId());

            return new PersistentPublication(
                aeronArchive, publication, recordingId, countersReader, recordingCounterId);
        }

        private sealed class ResumeDescriptor : IRecordingDescriptorConsumer
        {
            public long StopPosition { get; private set; }
            public int InitialTermId { get; private set; }
            public int TermBufferLength { get; private set; }

            public void OnRecordingDescriptor(
                long controlSessionId, long correlationId, long recordingId, long startTimestamp,
                long stopTimestamp, long startPosition, long stopPosition, int initialTermId,
                int segmentFileLength, int termBufferLength, int mtuLength, int sessionId,
                int streamId, string strippedChannel, string originalChannel, string sourceIdentity)
            {
                StopPosition = stopPosition;
                InitialTermId = initialTermId;
                TermBufferLength = termBufferLength;
            }
        }

        public long Offer(IDirectBuffer buffer, int offset, int length)
        {
            var result = _publication.Offer(buffer, offset, length);
            if (result > 0)
            {
                _publishedMessageCount++;
            }
            return result;
        }

        public void Persist(IList<byte[]> messages)
        {
            if (messages.Count == 0)
            {
                return;
            }
            var position = Publish(messages);
            Tests.AwaitPosition(_countersReader, _recordingCounterId, position);
        }

        public long Publish(IList<byte[]> messages)
        {
            var wrapper = new UnsafeBuffer();
            long position = _publication.Position;

            foreach (var message in messages)
            {
                wrapper.Wrap(message);
                while ((position = _publication.Offer(wrapper)) < 0)
                {
                    Tests.YieldingIdle("failed to offer due to " + Publication.ErrorString(position));
                }
            }

            _publishedMessageCount += messages.Count;
            return position;
        }

        public long Stop()
        {
            _aeronArchive.StopRecording(_publication);
            return _aeronArchive.GetStopPosition(_recordingId);
        }

        public void Dispose()
        {
            try { _aeronArchive.StopRecording(_publication); } catch { }
            _publication?.Dispose();
        }

        public void ClosePublicationOnly()
        {
            _publication?.Dispose();
        }
    }
}
