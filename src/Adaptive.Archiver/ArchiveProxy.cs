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
using Adaptive.Aeron;
using Adaptive.Aeron.Security;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    public class ArchiveProxy
    {
        /// <summary>
        /// Default number of retry attempts to be made when offering requests.
        /// </summary>
        public const int DEFAULT_RETRY_ATTEMPTS = 3;

        private readonly long _connectTimeoutNs;
        private readonly int _retryAttempts;
        private readonly IIdleStrategy _retryIdleStrategy;
        private readonly INanoClock _nanoClock;
        private readonly ICredentialsSupplier _credentialsSupplier;
        private readonly string _clientInfo;

        private readonly ExpandableArrayBuffer _buffer = new ExpandableArrayBuffer(256);
        private readonly ExclusivePublication _publication;
        private readonly MessageHeaderEncoder _messageHeader = new MessageHeaderEncoder();

        private readonly AuthConnectRequestEncoder _connectRequestEncoder = new AuthConnectRequestEncoder();
        private readonly KeepAliveRequestEncoder _keepAliveRequestEncoder = new KeepAliveRequestEncoder();
        private readonly CloseSessionRequestEncoder _closeSessionRequestEncoder = new CloseSessionRequestEncoder();
        private readonly ChallengeResponseEncoder _challengeResponseEncoder = new ChallengeResponseEncoder();
        private readonly StartRecordingRequestEncoder _startRecordingRequest = new StartRecordingRequestEncoder();
        private readonly StartRecordingRequest2Encoder _startRecordingRequest2 = new StartRecordingRequest2Encoder();
        private readonly StopRecordingRequestEncoder _stopRecordingRequest = new StopRecordingRequestEncoder();

        private readonly StopRecordingSubscriptionRequestEncoder _stopRecordingSubscriptionRequest =
            new StopRecordingSubscriptionRequestEncoder();

        private readonly StopRecordingByIdentityRequestEncoder _stopRecordingByIdentityRequest =
            new StopRecordingByIdentityRequestEncoder();

        private readonly ReplayRequestEncoder _replayRequest = new ReplayRequestEncoder();
        private readonly StopReplayRequestEncoder _stopReplayRequest = new StopReplayRequestEncoder();
        private readonly ListRecordingsRequestEncoder _listRecordingsRequest = new ListRecordingsRequestEncoder();

        private readonly ListRecordingsForUriRequestEncoder _listRecordingsForUriRequest =
            new ListRecordingsForUriRequestEncoder();

        private readonly ListRecordingRequestEncoder _listRecordingRequest = new ListRecordingRequestEncoder();
        private readonly ExtendRecordingRequestEncoder _extendRecordingRequest = new ExtendRecordingRequestEncoder();
        private readonly ExtendRecordingRequest2Encoder _extendRecordingRequest2 = new ExtendRecordingRequest2Encoder();

        private readonly RecordingPositionRequestEncoder _recordingPositionRequest =
            new RecordingPositionRequestEncoder();

        private readonly TruncateRecordingRequestEncoder _truncateRecordingRequest =
            new TruncateRecordingRequestEncoder();

        private readonly PurgeRecordingRequestEncoder _purgeRecordingRequest = new PurgeRecordingRequestEncoder();
        private readonly StopPositionRequestEncoder _stopPositionRequest = new StopPositionRequestEncoder();

        private readonly MaxRecordedPositionRequestEncoder _maxRecordedPositionRequestEncoder =
            new MaxRecordedPositionRequestEncoder();

        private readonly FindLastMatchingRecordingRequestEncoder _findLastMatchingRecordingRequest =
            new FindLastMatchingRecordingRequestEncoder();

        private readonly ListRecordingSubscriptionsRequestEncoder _listRecordingSubscriptionsRequest =
            new ListRecordingSubscriptionsRequestEncoder();

        private readonly BoundedReplayRequestEncoder _boundedReplayRequest = new BoundedReplayRequestEncoder();
        private readonly StopAllReplaysRequestEncoder _stopAllReplaysRequest = new StopAllReplaysRequestEncoder();
        private readonly ReplicateRequest2Encoder _replicateRequest = new ReplicateRequest2Encoder();
        private readonly StopReplicationRequestEncoder _stopReplicationRequest = new StopReplicationRequestEncoder();
        private readonly StartPositionRequestEncoder _startPositionRequest = new StartPositionRequestEncoder();
        private readonly DetachSegmentsRequestEncoder _detachSegmentsRequest = new DetachSegmentsRequestEncoder();

        private readonly DeleteDetachedSegmentsRequestEncoder _deleteDetachedSegmentsRequest =
            new DeleteDetachedSegmentsRequestEncoder();

        private readonly PurgeSegmentsRequestEncoder _purgeSegmentsRequest = new PurgeSegmentsRequestEncoder();
        private readonly AttachSegmentsRequestEncoder _attachSegmentsRequest = new AttachSegmentsRequestEncoder();
        private readonly MigrateSegmentsRequestEncoder _migrateSegmentsRequest = new MigrateSegmentsRequestEncoder();
        private readonly ArchiveIdRequestEncoder _archiveIdRequestEncoder = new ArchiveIdRequestEncoder();
        private readonly ReplayTokenRequestEncoder _replayTokenRequestEncoder = new ReplayTokenRequestEncoder();
        private readonly UpdateChannelRequestEncoder _updateChannelRequestEncoder = new UpdateChannelRequestEncoder();

        /// <summary>
        /// Create a proxy with a <seealso cref="ExclusivePublication"/> for sending control message requests.
        /// <para>
        /// This provides a default <seealso cref="IIdleStrategy"/> of a <seealso cref="YieldingIdleStrategy"/> when
        /// offers are back pressured with a defaults of
        /// <seealso cref="AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS"/> and
        /// <seealso cref="DEFAULT_RETRY_ATTEMPTS"/>.
        ///
        /// </para>
        /// </summary>
        /// <param name="publication"> publication for sending control messages to an archive. </param>
        public ArchiveProxy(ExclusivePublication publication)
            : this(
                publication,
                YieldingIdleStrategy.INSTANCE,
                SystemNanoClock.INSTANCE,
                AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS,
                DEFAULT_RETRY_ATTEMPTS,
                new NullCredentialsSupplier(),
                null
            ) { }

        /// <summary>
        /// Create a proxy with a <seealso cref="Pub"/> for sending control message requests.
        /// </summary>
        /// <param name="publication">         publication for sending control messages to an archive. </param>
        /// <param name="retryIdleStrategy"> for what should happen between retry attempts at offering messages.
        /// </param>
        /// <param name="nanoClock">           to be used for calculating checking deadlines. </param>
        /// <param name="connectTimeoutNs">    for connection requests. </param>
        /// <param name="retryAttempts">       for offering control messages before giving up. </param>
        /// <param name="credentialsSupplier"> for the AuthConnectRequest </param>
        public ArchiveProxy(
            ExclusivePublication publication,
            IIdleStrategy retryIdleStrategy,
            INanoClock nanoClock,
            long connectTimeoutNs,
            int retryAttempts,
            ICredentialsSupplier credentialsSupplier
        )
        {
            this._publication = publication;
            this._retryIdleStrategy = retryIdleStrategy;
            this._nanoClock = nanoClock;
            this._connectTimeoutNs = connectTimeoutNs;
            this._retryAttempts = retryAttempts;
            this._credentialsSupplier = credentialsSupplier;
        }

        /// <summary>
        /// Create a proxy with a <seealso cref="ExclusivePublication"/> for sending control message requests with
        /// specified client info.
        /// </summary>
        /// <param name="publication">         publication for sending control messages to an archive. </param>
        /// <param name="retryIdleStrategy"> for what should happen between retry attempts at offering messages.
        /// </param>
        /// <param name="nanoClock">           to be used for calculating checking deadlines. </param>
        /// <param name="connectTimeoutNs">    for connection requests. </param>
        /// <param name="retryAttempts">       for offering control messages before giving up. </param>
        /// <param name="credentialsSupplier"> for the <code>AuthConnectRequest</code>. </param>
        /// <param name="clientInfo">          for the <code>AuthConnectRequest</code>. </param>
        /// <remarks>Since 1.49.0</remarks>
        public ArchiveProxy(
            ExclusivePublication publication,
            IIdleStrategy retryIdleStrategy,
            INanoClock nanoClock,
            long connectTimeoutNs,
            int retryAttempts,
            ICredentialsSupplier credentialsSupplier,
            string clientInfo
        )
        {
            this._publication = publication;
            this._retryIdleStrategy = retryIdleStrategy;
            this._nanoClock = nanoClock;
            this._connectTimeoutNs = connectTimeoutNs;
            this._retryAttempts = retryAttempts;
            this._credentialsSupplier = credentialsSupplier;
            this._clientInfo = clientInfo;
        }

        /// <summary>
        /// Get the <seealso cref="Publication"/> used for sending control messages.
        /// </summary>
        /// <returns> the <seealso cref="Publication"/> used for sending control messages. </returns>
        public Publication Pub()
        {
            return _publication;
        }

        /// <summary>
        /// Connect to an archive on its control interface providing the response stream details.
        /// </summary>
        /// <param name="responseChannel">  for the control message responses. </param>
        /// <param name="responseStreamId"> for the control message responses. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool Connect(string responseChannel, int responseStreamId, long correlationId)
        {
            byte[] encodedCredentials = _credentialsSupplier.EncodedCredentials();

            _connectRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId)
                .Version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
                .ResponseChannel(responseChannel)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length)
                .ClientInfo(_clientInfo);

            return OfferWithTimeout(_connectRequestEncoder.EncodedLength(), null);
        }

        /// <summary>
        /// Try and connect to an archive on its control interface providing the response stream details. Only one
        /// attempt will be made to offer the request.
        /// </summary>
        /// <param name="responseChannel">  for the control message responses. </param>
        /// <param name="responseStreamId"> for the control message responses. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool TryConnect(string responseChannel, int responseStreamId, long correlationId)
        {
            byte[] encodedCredentials = _credentialsSupplier.EncodedCredentials();

            _connectRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId)
                .Version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
                .ResponseChannel(responseChannel)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length)
                .ClientInfo(_clientInfo);

            int length = MessageHeaderEncoder.ENCODED_LENGTH + _connectRequestEncoder.EncodedLength();

            return _publication.Offer(_buffer, 0, length) > 0;
        }

        /// <summary>
        /// Keep this archive session alive by notifying the archive.
        /// </summary>
        /// <param name="controlSessionId"> with the archive. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool KeepAlive(long controlSessionId, long correlationId)
        {
            _keepAliveRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId);

            return Offer(_keepAliveRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Close this control session with the archive.
        /// </summary>
        /// <param name="controlSessionId"> with the archive. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool CloseSession(long controlSessionId)
        {
            _closeSessionRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId);

            return Offer(_closeSessionRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Try and send a ChallengeResponse to an archive on its control interface providing the credentials. Only one
        /// attempt will be made to offer the request.
        /// </summary>
        /// <param name="encodedCredentials"> to send. </param>
        /// <param name="correlationId">      for this response. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool TryChallengeResponse(byte[] encodedCredentials, long correlationId, long controlSessionId)
        {
            _challengeResponseEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            int length = MessageHeaderEncoder.ENCODED_LENGTH + _challengeResponseEncoder.EncodedLength();

            return _publication.Offer(_buffer, 0, length) > 0;
        }

        /// <summary>
        /// Start recording streams for a given channel and stream id pairing.
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StartRecording(
            string channel,
            int streamId,
            SourceLocation sourceLocation,
            long correlationId,
            long controlSessionId
        )
        {
            _startRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .StreamId(streamId)
                .SourceLocation(sourceLocation)
                .Channel(channel);

            return Offer(_startRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Start recording streams for a given channel and stream id pairing.
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="autoStop">         if the recording should be automatically stopped when complete. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StartRecording(
            string channel,
            int streamId,
            SourceLocation sourceLocation,
            bool autoStop,
            long correlationId,
            long controlSessionId
        )
        {
            _startRecordingRequest2
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .StreamId(streamId)
                .SourceLocation(sourceLocation)
                .AutoStop(autoStop ? BooleanType.TRUE : BooleanType.FALSE)
                .Channel(channel);

            return Offer(_startRecordingRequest2.EncodedLength());
        }

        /// <summary>
        /// Stop an active recording.
        /// </summary>
        /// <param name="channel">          to be stopped. </param>
        /// <param name="streamId">         to be stopped. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StopRecording(string channel, int streamId, long correlationId, long controlSessionId)
        {
            _stopRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .StreamId(streamId)
                .Channel(channel);

            return Offer(_stopRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Stop a recording by the <seealso cref="Subscription.RegistrationId"/> it was registered with.
        /// </summary>
        /// <param name="subscriptionId"> that identifies the subscription in the archive doing the recording. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StopRecording(long subscriptionId, long correlationId, long controlSessionId)
        {
            _stopRecordingSubscriptionRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .SubscriptionId(subscriptionId);

            return Offer(_stopRecordingSubscriptionRequest.EncodedLength());
        }

        /// <summary>
        /// Stop an active recording by the recording id. This is not the <seealso cref="Subscription.RegistrationId"/>
        /// .
        /// </summary>
        /// <param name="recordingId">      that identifies a recording in the archive. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StopRecordingByIdentity(long recordingId, long correlationId, long controlSessionId)
        {
            _stopRecordingByIdentityRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_stopRecordingByIdentityRequest.EncodedLength());
        }

        /// <summary>
        /// Replay a recording from a given position. Supports specifying <seealso cref="ReplayParams"/> to change the
        /// behaviour of the replay. For example a bounded replay can be requested by specifying the
        /// boundingLimitCounterId. The ReplayParams is free to be reused after this call completes.
        /// </summary>
        /// <param name="recordingId">      to be replayed. </param>
        /// <param name="replayChannel">    to which the replay should be sent. </param>
        /// <param name="replayStreamId">   to which the replay should be sent. </param>
        /// <param name="replayParams">     optional parameters change the behaviour of the replay. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        /// <seealso cref="ReplayParams"/>
        public bool Replay(
            long recordingId,
            string replayChannel,
            int replayStreamId,
            ReplayParams replayParams,
            long correlationId,
            long controlSessionId
        )
        {
            if (replayParams.IsBounded())
            {
                return BoundedReplay(
                    recordingId,
                    replayParams.Position(),
                    replayParams.Length(),
                    replayParams.BoundingLimitCounterId(),
                    replayChannel,
                    replayStreamId,
                    correlationId,
                    controlSessionId,
                    replayParams.FileIoMaxLength(),
                    replayParams.ReplayToken()
                );
            }
            else
            {
                return Replay(
                    recordingId,
                    replayParams.Position(),
                    replayParams.Length(),
                    replayChannel,
                    replayStreamId,
                    correlationId,
                    controlSessionId,
                    replayParams.FileIoMaxLength(),
                    replayParams.ReplayToken()
                );
            }
        }

        /// <summary>
        /// Replay a recording from a given position.
        /// </summary>
        /// <param name="recordingId">      to be replayed. </param>
        /// <param name="position">         from which the replay should be started. </param>
        /// <param name="length"> of the stream to be replayed. Use <seealso cref="long.MaxValue"/> to follow a live
        /// stream. </param>
        /// <param name="replayChannel">    to which the replay should be sent. </param>
        /// <param name="replayStreamId">   to which the replay should be sent. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool Replay(
            long recordingId,
            long position,
            long length,
            string replayChannel,
            int replayStreamId,
            long correlationId,
            long controlSessionId
        )
        {
            return Replay(
                recordingId,
                position,
                length,
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE
            );
        }

        /// <summary>
        /// Replay a recording from a given position bounded by a position counter.
        /// </summary>
        /// <param name="recordingId">      to be replayed. </param>
        /// <param name="position">         from which the replay should be started. </param>
        /// <param name="length"> of the stream to be replayed. Use <seealso cref="long.MaxValue"/> to follow a live
        /// stream. </param>
        /// <param name="limitCounterId">   to use as the replay bound. </param>
        /// <param name="replayChannel">    to which the replay should be sent. </param>
        /// <param name="replayStreamId">   to which the replay should be sent. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool BoundedReplay(
            long recordingId,
            long position,
            long length,
            int limitCounterId,
            string replayChannel,
            int replayStreamId,
            long correlationId,
            long controlSessionId
        )
        {
            return BoundedReplay(
                recordingId,
                position,
                length,
                limitCounterId,
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE
            );
        }

        /// <summary>
        /// Stop an existing replay session.
        /// </summary>
        /// <param name="replaySessionId">  that should be stopped. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StopReplay(long replaySessionId, long correlationId, long controlSessionId)
        {
            _stopReplayRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .ReplaySessionId(replaySessionId);

            return Offer(_stopReplayRequest.EncodedLength());
        }

        /// <summary>
        /// Stop any existing replay sessions for recording id or all replay sessions regardless of recording id.
        /// </summary>
        /// <param name="recordingId">      that should be stopped. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StopAllReplays(long recordingId, long correlationId, long controlSessionId)
        {
            _stopAllReplaysRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_stopAllReplaysRequest.EncodedLength());
        }

        /// <summary>
        /// List a range of recording descriptors.
        /// </summary>
        /// <param name="fromRecordingId">  at which to begin listing. </param>
        /// <param name="recordCount">      for the number of descriptors to be listed. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ListRecordings(long fromRecordingId, int recordCount, long correlationId, long controlSessionId)
        {
            _listRecordingsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .FromRecordingId(fromRecordingId)
                .RecordCount(recordCount);

            return Offer(_listRecordingsRequest.EncodedLength());
        }

        /// <summary>
        /// List a range of recording descriptors which match a channel URI fragment and stream id.
        /// </summary>
        /// <param name="fromRecordingId">  at which to begin listing. </param>
        /// <param name="recordCount">      for the number of descriptors to be listed. </param>
        /// <param name="channelFragment"> to match recordings on from the original channel URI in the archive
        /// descriptor. </param>
        /// <param name="streamId">         to match recordings on. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ListRecordingsForUri(
            long fromRecordingId,
            int recordCount,
            string channelFragment,
            int streamId,
            long correlationId,
            long controlSessionId
        )
        {
            _listRecordingsForUriRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .FromRecordingId(fromRecordingId)
                .RecordCount(recordCount)
                .StreamId(streamId)
                .Channel(channelFragment);

            return Offer(_listRecordingsForUriRequest.EncodedLength());
        }

        /// <summary>
        /// List a recording descriptor for a given recording id.
        /// </summary>
        /// <param name="recordingId">      at which to begin listing. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ListRecording(long recordingId, long correlationId, long controlSessionId)
        {
            _listRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_listRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Extend an existing, non-active, recorded stream for the same channel and stream id.
        /// <para>
        /// The channel must be configured for the initial position from which it will be extended. This can be done
        /// with <seealso cref="ChannelUriStringBuilder.InitialPosition(long, int, int)"/> . The details required to
        /// initialise can be found by calling <seealso cref="ListRecording(long, long, long)"/> .
        ///
        /// </para>
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="recordingId">      to be extended. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ExtendRecording(
            string channel,
            int streamId,
            SourceLocation sourceLocation,
            long recordingId,
            long correlationId,
            long controlSessionId
        )
        {
            _extendRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .StreamId(streamId)
                .SourceLocation(sourceLocation)
                .Channel(channel);

            return Offer(_extendRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Extend an existing, non-active, recorded stream for a the same channel and stream id.
        /// <para>
        /// The channel must be configured for the initial position from which it will be extended. This can be done
        /// with <seealso cref="ChannelUriStringBuilder.InitialPosition(long, int, int)"/> . The details required to
        /// initialise can be found by calling <seealso cref="ListRecording(long, long, long)"/> .
        ///
        /// </para>
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="autoStop">         if the recording should be automatically stopped when complete. </param>
        /// <param name="recordingId">      to be extended. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ExtendRecording(
            string channel,
            int streamId,
            SourceLocation sourceLocation,
            bool autoStop,
            long recordingId,
            long correlationId,
            long controlSessionId
        )
        {
            _extendRecordingRequest2
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .StreamId(streamId)
                .SourceLocation(sourceLocation)
                .AutoStop(autoStop ? BooleanType.TRUE : BooleanType.FALSE)
                .Channel(channel);

            return Offer(_extendRecordingRequest2.EncodedLength());
        }

        /// <summary>
        /// Get the recorded position of an active recording.
        /// </summary>
        /// <param name="recordingId">      of the active recording that the position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool GetRecordingPosition(long recordingId, long correlationId, long controlSessionId)
        {
            _recordingPositionRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_recordingPositionRequest.EncodedLength());
        }

        /// <summary>
        /// Truncate a stopped recording to a given position that is less than the stopped position. The provided
        /// position must be on a fragment boundary. Truncating a recording to the start position effectively deletes
        /// the recording.
        ///
        /// </summary>
        /// <param name="recordingId">      of the stopped recording to be truncated. </param>
        /// <param name="position">         to which the recording will be truncated. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool TruncateRecording(long recordingId, long position, long correlationId, long controlSessionId)
        {
            _truncateRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Position(position);

            return Offer(_truncateRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Purge a stopped recording, i.e. mark recording as <seealso cref="RecordingState.INVALID"/> and delete the
        /// corresponding segment files. The space in the Catalog will be reclaimed upon compaction.
        /// </summary>
        /// <param name="recordingId">      of the stopped recording to be purged. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool PurgeRecording(long recordingId, long correlationId, long controlSessionId)
        {
            _purgeRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_purgeRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Get the start position of a recording.
        /// </summary>
        /// <param name="recordingId">      of the recording that the position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool GetStartPosition(long recordingId, long correlationId, long controlSessionId)
        {
            _startPositionRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_startPositionRequest.EncodedLength());
        }

        /// <summary>
        /// Get the stop position of a recording.
        /// </summary>
        /// <param name="recordingId">      of the recording that the stop position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool GetStopPosition(long recordingId, long correlationId, long controlSessionId)
        {
            _stopPositionRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_stopPositionRequest.EncodedLength());
        }

        /// <summary>
        /// Get the stop or active recorded position of a recording.
        /// </summary>
        /// <param name="recordingId"> of the recording that the stop of active recording position is being requested
        /// for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool GetMaxRecordedPosition(long recordingId, long correlationId, long controlSessionId)
        {
            _maxRecordedPositionRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_maxRecordedPositionRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Get the id of the Archive.
        /// </summary>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ArchiveId(long correlationId, long controlSessionId)
        {
            _archiveIdRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId);

            return Offer(_archiveIdRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Find the last recording that matches the given criteria.
        /// </summary>
        /// <param name="minRecordingId">   to search back to. </param>
        /// <param name="channelFragment"> for a contains match on the original channel stored with the archive
        /// descriptor. </param>
        /// <param name="streamId">         of the recording to match. </param>
        /// <param name="sessionId">        of the recording to match. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool FindLastMatchingRecording(
            long minRecordingId,
            string channelFragment,
            int streamId,
            int sessionId,
            long correlationId,
            long controlSessionId
        )
        {
            _findLastMatchingRecordingRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .MinRecordingId(minRecordingId)
                .SessionId(sessionId)
                .StreamId(streamId)
                .Channel(channelFragment);

            return Offer(_findLastMatchingRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// List registered subscriptions in the archive which have been used to record streams.
        /// </summary>
        /// <param name="pseudoIndex">       in the list of active recording subscriptions. </param>
        /// <param name="subscriptionCount"> for the number of descriptors to be listed. </param>
        /// <param name="channelFragment"> for a contains match on the stripped channel used with the registered
        /// subscription. </param>
        /// <param name="streamId">          for the subscription. </param>
        /// <param name="applyStreamId">     when matching. </param>
        /// <param name="correlationId">     for this request. </param>
        /// <param name="controlSessionId">  for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool ListRecordingSubscriptions(
            int pseudoIndex,
            int subscriptionCount,
            string channelFragment,
            int streamId,
            bool applyStreamId,
            long correlationId,
            long controlSessionId
        )
        {
            _listRecordingSubscriptionsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .PseudoIndex(pseudoIndex)
                .SubscriptionCount(subscriptionCount)
                .ApplyStreamId(applyStreamId ? BooleanType.TRUE : BooleanType.FALSE)
                .StreamId(streamId)
                .Channel(channelFragment);

            return Offer(_listRecordingSubscriptionsRequest.EncodedLength());
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream
        /// id. If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new
        /// destination recording is created, otherwise the provided destination recording id will be extended. The
        /// details of the source recording descriptor will be replicated.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with
        /// <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/> .
        ///
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId"> recording to extend in the destination, otherwise
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="srcControlChannel"> remote control channel for the source archive to instruct the replay on.
        /// </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on.
        /// </param>
        /// <param name="liveDestination"> destination for the live stream if merge is required. Empty or null for no
        /// merge. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool Replicate(
            long srcRecordingId,
            long dstRecordingId,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            long correlationId,
            long controlSessionId
        )
        {
            return Replicate(
                srcRecordingId,
                dstRecordingId,
                AeronArchive.NULL_POSITION,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE,
                srcControlStreamId,
                srcControlChannel,
                liveDestination,
                null,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE,
                NullCredentialsSupplier.NULL_CREDENTIAL,
                null
            );
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream
        /// id. If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new
        /// destination recording is created, otherwise the provided destination recording id will be extended. The
        /// details of the source recording descriptor will be replicated.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with
        /// <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/> .
        ///
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId"> recording to extend in the destination, otherwise
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="stopPosition"> position to stop the replication. <seealso cref="AeronArchive.NULL_POSITION"/>
        /// to stop at end of current recording. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on.
        /// </param>
        /// <param name="srcControlChannel"> remote control channel for the source archive to instruct the replay on.
        /// </param>
        /// <param name="liveDestination"> destination for the live stream if merge is required. Empty or null for no
        /// merge. </param>
        /// <param name="replicationChannel"> channel over which the replication will occur. Empty or null for default
        /// channel. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool Replicate(
            long srcRecordingId,
            long dstRecordingId,
            long stopPosition,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            string replicationChannel,
            long correlationId,
            long controlSessionId
        )
        {
            return Replicate(
                srcRecordingId,
                dstRecordingId,
                stopPosition,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE,
                srcControlStreamId,
                srcControlChannel,
                liveDestination,
                replicationChannel,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE,
                NullCredentialsSupplier.NULL_CREDENTIAL,
                null
            );
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream
        /// id. If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new
        /// destination recording is created, otherwise the provided destination recording id will be extended. The
        /// details of the source recording descriptor will be replicated. The subscription used in the archive will be
        /// tagged with the provided tags.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with
        /// <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/> .
        ///
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId"> recording to extend in the destination, otherwise
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="channelTagId">       used to tag the replication subscription. </param>
        /// <param name="subscriptionTagId">  used to tag the replication subscription. </param>
        /// <param name="srcControlChannel"> remote control channel for the source archive to instruct the replay on.
        /// </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on.
        /// </param>
        /// <param name="liveDestination"> destination for the live stream if merge is required. Empty or null for no
        /// merge. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool TaggedReplicate(
            long srcRecordingId,
            long dstRecordingId,
            long channelTagId,
            long subscriptionTagId,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            long correlationId,
            long controlSessionId
        )
        {
            return Replicate(
                srcRecordingId,
                dstRecordingId,
                AeronArchive.NULL_POSITION,
                channelTagId,
                subscriptionTagId,
                srcControlStreamId,
                srcControlChannel,
                liveDestination,
                null,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE,
                NullCredentialsSupplier.NULL_CREDENTIAL,
                null
            );
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream
        /// id. If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new
        /// destination recording is created, otherwise the provided destination recording id will be extended. The
        /// details of the source recording descriptor will be replicated. The subscription used in the archive will be
        /// tagged with the provided tags.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with
        /// <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/> .
        ///
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId"> recording to extend in the destination, otherwise
        /// <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="stopPosition"> position to stop the replication. <seealso cref="AeronArchive.NULL_POSITION"/>
        /// to stop at end of current recording. </param>
        /// <param name="channelTagId">       used to tag the replication subscription. </param>
        /// <param name="subscriptionTagId">  used to tag the replication subscription. </param>
        /// <param name="srcControlChannel"> remote control channel for the source archive to instruct the replay on.
        /// </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on.
        /// </param>
        /// <param name="liveDestination"> destination for the live stream if merge is required. Empty or null for no
        /// merge. </param>
        /// <param name="replicationChannel"> channel over which the replication will occur. Empty or null for default
        /// channel. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool TaggedReplicate(
            long srcRecordingId,
            long dstRecordingId,
            long stopPosition,
            long channelTagId,
            long subscriptionTagId,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            string replicationChannel,
            long correlationId,
            long controlSessionId
        )
        {
            return Replicate(
                srcRecordingId,
                dstRecordingId,
                stopPosition,
                channelTagId,
                subscriptionTagId,
                srcControlStreamId,
                srcControlChannel,
                liveDestination,
                replicationChannel,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE,
                Aeron.Aeron.NULL_VALUE,
                NullCredentialsSupplier.NULL_CREDENTIAL,
                null
            );
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The behaviour of the replication is controlled through the <seealso cref="ReplicationParams"/> .
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with
        /// <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/> .
        /// </para>
        /// <para>
        /// The ReplicationParams is free to be reused when this call completes.
        ///
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="srcControlChannel"> remote control channel for the source archive to instruct the replay on.
        /// </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on.
        /// </param>
        /// <param name="replicationParams">  optional parameters to control the behaviour of the replication. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        /// <seealso cref="ReplicationParams"/>
        public bool Replicate(
            long srcRecordingId,
            int srcControlStreamId,
            string srcControlChannel,
            ReplicationParams replicationParams,
            long correlationId,
            long controlSessionId
        )
        {
            if (
                null != replicationParams.LiveDestination()
                && Aeron.Aeron.NULL_VALUE != replicationParams.ReplicationSessionId()
            )
            {
                throw new ArgumentException(
                    "ReplicationParams.LiveDestination and "
                        + "ReplicationParams.ReplicationSessionId can not be specified together"
                );
            }

            return Replicate(
                srcRecordingId,
                replicationParams.DstRecordingId(),
                replicationParams.StopPosition(),
                replicationParams.ChannelTagId(),
                replicationParams.SubscriptionTagId(),
                srcControlStreamId,
                srcControlChannel,
                replicationParams.LiveDestination(),
                replicationParams.ReplicationChannel(),
                correlationId,
                controlSessionId,
                replicationParams.FileIoMaxLength(),
                replicationParams.ReplicationSessionId(),
                replicationParams.EncodedCredentials(),
                replicationParams.SrcResponseChannel()
            );
        }

        /// <summary>
        /// Stop an active replication by the registration id it was registered with.
        /// </summary>
        /// <param name="replicationId">    that identifies the session in the archive doing the replication. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool StopReplication(long replicationId, long correlationId, long controlSessionId)
        {
            _stopReplicationRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .ReplicationId(replicationId);

            return Offer(_stopReplicationRequest.EncodedLength());
        }

        /// <summary>
        /// Detach segments from the beginning of a recording up to the provided new start position.
        /// <para>
        /// The new start position must be first byte position of a segment after the existing start position.
        /// </para>
        /// <para>
        /// It is not possible to detach segments which are active for recording or being replayed.
        ///
        /// </para>
        /// </summary>
        /// <param name="recordingId">      to which the operation applies. </param>
        /// <param name="newStartPosition"> for the recording after the segments are detached. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        /// <seealso cref="AeronArchive.SegmentFileBasePosition(long, long, int, int)"></seealso>
        public bool DetachSegments(long recordingId, long newStartPosition, long correlationId, long controlSessionId)
        {
            _detachSegmentsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .NewStartPosition(newStartPosition);

            return Offer(_detachSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Delete segments which have been previously detached from a recording.
        /// </summary>
        /// <param name="recordingId">      to which the operation applies. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        /// <seealso cref="DetachSegments(long, long, long, long)"></seealso>
        public bool DeleteDetachedSegments(long recordingId, long correlationId, long controlSessionId)
        {
            _deleteDetachedSegmentsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_deleteDetachedSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Purge (detach and delete) segments from the beginning of a recording up to the provided new start position.
        /// <para>
        /// The new start position must be first byte position of a segment after the existing start position.
        /// </para>
        /// <para>
        /// It is not possible to purge segments which are active for recording or being replayed.
        ///
        /// </para>
        /// </summary>
        /// <param name="recordingId">      to which the operation applies. </param>
        /// <param name="newStartPosition"> for the recording after the segments are detached. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        /// <seealso cref="DetachSegments(long, long, long, long)"></seealso>
        /// <seealso cref="DeleteDetachedSegments(long, long, long)"></seealso>
        /// <seealso cref="AeronArchive.SegmentFileBasePosition(long, long, int, int)"></seealso>
        public bool PurgeSegments(long recordingId, long newStartPosition, long correlationId, long controlSessionId)
        {
            _purgeSegmentsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .NewStartPosition(newStartPosition);

            return Offer(_purgeSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Attach segments to the beginning of a recording to restore history that was previously detached.
        /// <para>
        /// Segment files must match the existing recording and join exactly to the start position of the recording they
        /// are being attached to.
        ///
        /// </para>
        /// </summary>
        /// <param name="recordingId">      to which the operation applies. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        /// <seealso cref="DetachSegments(long, long, long, long)"></seealso>
        public bool AttachSegments(long recordingId, long correlationId, long controlSessionId)
        {
            _attachSegmentsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(_attachSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Migrate segments from a source recording and attach them to the beginning or end of a destination recording.
        /// <para>
        /// The source recording must match the destination recording for segment length, term length, mtu length, and
        /// stream id. The source recording must join to the destination recording on a segment boundary and without
        /// gaps, i.e., the stop position and term id of one must match the start position and term id of the other.
        /// </para>
        /// <para>
        /// The source recording must be stopped. The destination recording must be stopped if migrating segments to the
        /// end of the destination recording.
        /// </para>
        /// <para>
        /// The source recording will be effectively truncated back to its start position after the migration.
        ///
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">   source recording from which the segments will be migrated. </param>
        /// <param name="dstRecordingId">   destination recording to which the segments will be attached. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> <code>true</code> if successfully offered otherwise <code>false</code>. </returns>
        public bool MigrateSegments(long srcRecordingId, long dstRecordingId, long correlationId, long controlSessionId)
        {
            _migrateSegmentsRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .SrcRecordingId(srcRecordingId)
                .DstRecordingId(dstRecordingId);

            return Offer(_migrateSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Request a token for this session that will allow a replay to be initiated from another image without
        /// re-authentication.
        /// </summary>
        /// <param name="lastCorrelationId"> for the request </param>
        /// <param name="controlSessionId">  for the request </param>
        /// <param name="recordingId">       that will be replayed. </param>
        /// <returns> true if successfully offered </returns>
        public bool RequestReplayToken(long lastCorrelationId, long controlSessionId, long recordingId)
        {
            _replayTokenRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(lastCorrelationId)
                .RecordingId(recordingId);

            return Offer(_replayTokenRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Update the channel for a recording.
        /// </summary>
        /// <param name="recordingId">       the recording id to update. </param>
        /// <param name="channel">           the new channel to include in the catalogue. </param>
        /// <param name="correlationId">     for the request. </param>
        /// <param name="controlSessionId">  for the request. </param>
        /// <returns> true if successfully offered. </returns>
        public bool UpdateChannel(long recordingId, string channel, long correlationId, long controlSessionId)
        {
            _updateChannelRequestEncoder
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Channel(channel);

            return Offer(_updateChannelRequestEncoder.EncodedLength());
        }

        private bool Offer(int length)
        {
            _retryIdleStrategy.Reset();

            int attempts = _retryAttempts;
            while (true)
            {
                long position = _publication.Offer(_buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
                if (position > 0)
                {
                    return true;
                }

                if (position == Publication.CLOSED)
                {
                    throw new ArchiveException("connection to the archive has been closed");
                }

                if (position == Publication.NOT_CONNECTED)
                {
                    throw new ArchiveException("connection to the archive is no longer available");
                }

                if (position == Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new ArchiveException(
                        "offer failed due to max position being reached: term-length=" + _publication.TermBufferLength
                    );
                }

                if (--attempts <= 0)
                {
                    return false;
                }

                _retryIdleStrategy.Idle();
            }
        }

        private bool OfferWithTimeout(int length, AgentInvoker aeronClientInvoker)
        {
            _retryIdleStrategy.Reset();

            long deadlineNs = _nanoClock.NanoTime() + _connectTimeoutNs;
            while (true)
            {
                long result = _publication.Offer(_buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
                if (result > 0)
                {
                    return true;
                }

                if (result == Aeron.Publication.CLOSED)
                {
                    throw new ArchiveException("connection to the archive has been closed");
                }

                if (result == Aeron.Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new ArchiveException("offer failed due to max position being reached");
                }

                if (deadlineNs - _nanoClock.NanoTime() < 0)
                {
                    return false;
                }

                if (null != aeronClientInvoker)
                {
                    aeronClientInvoker.Invoke();
                }

                _retryIdleStrategy.Idle();
            }
        }

        private bool Replay(
            long recordingId,
            long position,
            long length,
            string replayChannel,
            int replayStreamId,
            long correlationId,
            long controlSessionId,
            int fileIoMaxLength,
            long replayToken
        )
        {
            _replayRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Position(position)
                .Length(length)
                .ReplayStreamId(replayStreamId)
                .FileIoMaxLength(fileIoMaxLength)
                .ReplayToken(replayToken)
                .ReplayChannel(replayChannel);

            return Offer(_replayRequest.EncodedLength());
        }

        private bool BoundedReplay(
            long recordingId,
            long position,
            long length,
            int limitCounterId,
            string replayChannel,
            int replayStreamId,
            long correlationId,
            long controlSessionId,
            int fileIoMaxLength,
            long replayToken
        )
        {
            _boundedReplayRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Position(position)
                .Length(length)
                .LimitCounterId(limitCounterId)
                .ReplayStreamId(replayStreamId)
                .FileIoMaxLength(fileIoMaxLength)
                .ReplayToken(replayToken)
                .ReplayChannel(replayChannel);

            return Offer(_boundedReplayRequest.EncodedLength());
        }

        private bool Replicate(
            long srcRecordingId,
            long dstRecordingId,
            long stopPosition,
            long channelTagId,
            long subscriptionTagId,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            string replicationChannel,
            long correlationId,
            long controlSessionId,
            int fileIoMaxLength,
            int replicationSessionId,
            byte[] encodedCredentials,
            string srcResponseChannel
        )
        {
            _replicateRequest
                .WrapAndApplyHeader(_buffer, 0, _messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .SrcRecordingId(srcRecordingId)
                .DstRecordingId(dstRecordingId)
                .StopPosition(stopPosition)
                .ChannelTagId(channelTagId)
                .SubscriptionTagId(subscriptionTagId)
                .SrcControlStreamId(srcControlStreamId)
                .FileIoMaxLength(fileIoMaxLength)
                .SrcControlChannel(srcControlChannel)
                .LiveDestination(liveDestination)
                .ReplicationChannel(replicationChannel)
                .ReplicationSessionId(replicationSessionId)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length)
                .SrcResponseChannel(srcResponseChannel);

            return Offer(_replicateRequest.EncodedLength());
        }
    }
}
