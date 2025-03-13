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

        private readonly long connectTimeoutNs;
        private readonly int retryAttempts;
        private readonly IIdleStrategy retryIdleStrategy;
        private readonly INanoClock nanoClock;
        private readonly ICredentialsSupplier credentialsSupplier;

        private readonly UnsafeBuffer
            buffer = new UnsafeBuffer(BufferUtil.AllocateDirect(1024)); // Should be ExpandableArrayBuffer

        private readonly Publication publication;
        private readonly MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
        private StartRecordingRequestEncoder startRecordingRequest;
        private StartRecordingRequest2Encoder startRecordingRequest2;
        private StopRecordingRequestEncoder stopRecordingRequest;
        private StopRecordingSubscriptionRequestEncoder stopRecordingSubscriptionRequest;
        private StopRecordingByIdentityRequestEncoder stopRecordingByIdentityRequest;
        private ReplayRequestEncoder replayRequest;
        private StopReplayRequestEncoder stopReplayRequest;
        private ListRecordingsRequestEncoder listRecordingsRequest;
        private ListRecordingsForUriRequestEncoder listRecordingsForUriRequest;
        private ListRecordingRequestEncoder listRecordingRequest;
        private ExtendRecordingRequestEncoder extendRecordingRequest;
        private ExtendRecordingRequest2Encoder extendRecordingRequest2;
        private RecordingPositionRequestEncoder recordingPositionRequest;
        private TruncateRecordingRequestEncoder truncateRecordingRequest;
        private PurgeRecordingRequestEncoder purgeRecordingRequest;
        private StopPositionRequestEncoder stopPositionRequest;
        private FindLastMatchingRecordingRequestEncoder findLastMatchingRecordingRequest;
        private ListRecordingSubscriptionsRequestEncoder listRecordingSubscriptionsRequest;
        private BoundedReplayRequestEncoder boundedReplayRequest;
        private StopAllReplaysRequestEncoder stopAllReplaysRequest;
        private ReplicateRequest2Encoder replicateRequest;
        private StopReplicationRequestEncoder stopReplicationRequest;
        private StartPositionRequestEncoder startPositionRequest;
        private DetachSegmentsRequestEncoder detachSegmentsRequest;
        private DeleteDetachedSegmentsRequestEncoder deleteDetachedSegmentsRequest;
        private PurgeSegmentsRequestEncoder purgeSegmentsRequest;
        private AttachSegmentsRequestEncoder attachSegmentsRequest;
        private MigrateSegmentsRequestEncoder migrateSegmentsRequest;

        /// <summary>
        /// Create a proxy with a <seealso cref="Pub"/> for sending control message requests.
        /// <para>
        /// This provides a default <seealso cref="IIdleStrategy"/> of a <seealso cref="YieldingIdleStrategy"/> when offers are back pressured
        /// with a defaults of <seealso cref="AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS"/> and
        /// <seealso cref="DEFAULT_RETRY_ATTEMPTS"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="publication"> publication for sending control messages to an archive. </param>
        public ArchiveProxy(Publication publication) : this(publication, YieldingIdleStrategy.INSTANCE,
            SystemNanoClock.INSTANCE, AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS, DEFAULT_RETRY_ATTEMPTS,
            new NullCredentialsSupplier())
        {
        }

        /// <summary>
        /// Create a proxy with a <seealso cref="Pub"/> for sending control message requests.
        /// </summary>
        /// <param name="publication">         publication for sending control messages to an archive. </param>
        /// <param name="retryIdleStrategy">   for what should happen between retry attempts at offering messages. </param>
        /// <param name="nanoClock">           to be used for calculating checking deadlines. </param>
        /// <param name="connectTimeoutNs">    for connection requests. </param>
        /// <param name="retryAttempts">       for offering control messages before giving up. </param>
        /// <param name="credentialsSupplier"> for the AuthConnectRequest </param>
        public ArchiveProxy(Publication publication, IIdleStrategy retryIdleStrategy, INanoClock nanoClock,
            long connectTimeoutNs, int retryAttempts, ICredentialsSupplier credentialsSupplier)
        {
            this.publication = publication;
            this.retryIdleStrategy = retryIdleStrategy;
            this.nanoClock = nanoClock;
            this.connectTimeoutNs = connectTimeoutNs;
            this.retryAttempts = retryAttempts;
            this.credentialsSupplier = credentialsSupplier;
        }

        /// <summary>
        /// Get the <seealso cref="Publication"/> used for sending control messages.
        /// </summary>
        /// <returns> the <seealso cref="Publication"/> used for sending control messages. </returns>
        public Publication Pub()
        {
            return publication;
        }

        /// <summary>
        /// Connect to an archive on its control interface providing the response stream details.
        /// </summary>
        /// <param name="responseChannel">  for the control message responses. </param>
        /// <param name="responseStreamId"> for the control message responses. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool Connect(string responseChannel, int responseStreamId, long correlationId)
        {
            byte[] encodedCredentials = credentialsSupplier.EncodedCredentials();

            AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
            connectRequestEncoder.WrapAndApplyHeader(buffer, 0, messageHeader).CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId).Version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
                .ResponseChannel(responseChannel)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            return OfferWithTimeout(connectRequestEncoder.EncodedLength(), null);
        }

        /// <summary>
        /// Try and connect to an archive on its control interface providing the response stream details. Only one attempt will
        /// be made to offer the request.
        /// </summary>
        /// <param name="responseChannel">  for the control message responses. </param>
        /// <param name="responseStreamId"> for the control message responses. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool TryConnect(string responseChannel, int responseStreamId, long correlationId)
        {
            byte[] encodedCredentials = credentialsSupplier.EncodedCredentials();

            AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
            connectRequestEncoder.WrapAndApplyHeader(buffer, 0, messageHeader).CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId).Version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
                .ResponseChannel(responseChannel)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            int length = MessageHeaderEncoder.ENCODED_LENGTH + connectRequestEncoder.EncodedLength();

            return publication.Offer(buffer, 0, length) > 0;
        }

        /// <summary>
        /// Connect to an archive on its control interface providing the response stream details.
        /// </summary>
        /// <param name="responseChannel">    for the control message responses. </param>
        /// <param name="responseStreamId">   for the control message responses. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="aeronClientInvoker"> for aeron client conductor thread. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool Connect(string responseChannel, int responseStreamId, long correlationId,
            AgentInvoker aeronClientInvoker)
        {
            byte[] encodedCredentials = credentialsSupplier.EncodedCredentials();

            AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
            connectRequestEncoder.WrapAndApplyHeader(buffer, 0, messageHeader).CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId).Version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
                .ResponseChannel(responseChannel)
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            return OfferWithTimeout(connectRequestEncoder.EncodedLength(), aeronClientInvoker);
        }

        /// <summary>
        /// Keep this archive session alive by notifying the archive.
        /// </summary>
        /// <param name="controlSessionId"> with the archive. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool KeepAlive(long controlSessionId, long correlationId)
        {
            KeepAliveRequestEncoder keepAliveRequestEncoder = new KeepAliveRequestEncoder();
            keepAliveRequestEncoder.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId);

            return Offer(keepAliveRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Close this control session with the archive.
        /// </summary>
        /// <param name="controlSessionId"> with the archive. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool CloseSession(long controlSessionId)
        {
            CloseSessionRequestEncoder closeSessionRequestEncoder = new CloseSessionRequestEncoder();
            closeSessionRequestEncoder.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId);

            return Offer(closeSessionRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Try and send a ChallengeResponse to an archive on its control interface providing the credentials. Only one
        /// attempt will be made to offer the request.
        /// </summary>
        /// <param name="encodedCredentials"> to send. </param>
        /// <param name="correlationId">      for this response. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool TryChallengeResponse(byte[] encodedCredentials, long correlationId, long controlSessionId)
        {
            ChallengeResponseEncoder challengeResponseEncoder = new ChallengeResponseEncoder();
            challengeResponseEncoder.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            int length = MessageHeaderEncoder.ENCODED_LENGTH + challengeResponseEncoder.EncodedLength();

            return publication.Offer(buffer, 0, length) > 0;
        }

        /// <summary>
        /// Start recording streams for a given channel and stream id pairing.
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StartRecording(string channel, int streamId, SourceLocation sourceLocation, long correlationId,
            long controlSessionId)
        {
            if (null == startRecordingRequest)
            {
                startRecordingRequest = new StartRecordingRequestEncoder();
            }

            startRecordingRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).StreamId(streamId).SourceLocation(sourceLocation).Channel(channel);

            return Offer(startRecordingRequest.EncodedLength());
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
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StartRecording(string channel, int streamId, SourceLocation sourceLocation, bool autoStop,
            long correlationId, long controlSessionId)
        {
            if (null == startRecordingRequest2)
            {
                startRecordingRequest2 = new StartRecordingRequest2Encoder();
            }

            startRecordingRequest2.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).StreamId(streamId).SourceLocation(sourceLocation)
                .AutoStop(autoStop ? BooleanType.TRUE : BooleanType.FALSE).Channel(channel);

            return Offer(startRecordingRequest2.EncodedLength());
        }

        /// <summary>
        /// Stop an active recording.
        /// </summary>
        /// <param name="channel">          to be stopped. </param>
        /// <param name="streamId">         to be stopped. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopRecording(string channel, int streamId, long correlationId, long controlSessionId)
        {
            if (null == stopRecordingRequest)
            {
                stopRecordingRequest = new StopRecordingRequestEncoder();
            }

            stopRecordingRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).StreamId(streamId).Channel(channel);

            return Offer(stopRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Stop a recording by the <seealso cref="Subscription.RegistrationId"/> it was registered with.
        /// </summary>
        /// <param name="subscriptionId">   that identifies the subscription in the archive doing the recording. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopRecording(long subscriptionId, long correlationId, long controlSessionId)
        {
            if (null == stopRecordingSubscriptionRequest)
            {
                stopRecordingSubscriptionRequest = new StopRecordingSubscriptionRequestEncoder();
            }

            stopRecordingSubscriptionRequest.WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId).CorrelationId(correlationId).SubscriptionId(subscriptionId);

            return Offer(stopRecordingSubscriptionRequest.EncodedLength());
        }

        /// <summary>
        /// Stop an active recording by the recording id. This is not the <seealso cref="Subscription.RegistrationId"/>.
        /// </summary>
        /// <param name="recordingId">      that identifies a recording in the archive. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopRecordingByIdentity(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == stopRecordingByIdentityRequest)
            {
                stopRecordingByIdentityRequest = new StopRecordingByIdentityRequestEncoder();
            }

            stopRecordingByIdentityRequest.WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId).CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(stopRecordingByIdentityRequest.EncodedLength());
        }

        /// <summary>
        /// Replay a recording from a given position. Supports specifying <seealso cref="ReplayParams"/> to change the behaviour of the
        /// replay. For example a bounded replay can be requested by specifying the boundingLimitCounterId. The ReplayParams
        /// is free to be reused after this call completes.
        /// </summary>
        /// <param name="recordingId">      to be replayed. </param>
        /// <param name="replayChannel">    to which the replay should be sent. </param>
        /// <param name="replayStreamId">   to which the replay should be sent. </param>
        /// <param name="replayParams">     optional parameters change the behaviour of the replay. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        /// <seealso cref="ReplayParams"/>
        public bool Replay(
            long recordingId,
            string replayChannel,
            int replayStreamId,
            ReplayParams replayParams,
            long correlationId,
            long controlSessionId)
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
                    replayParams.FileIoMaxLength());
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
                    replayParams.FileIoMaxLength());
            }
        }

        /// <summary>
        /// Replay a recording from a given position.
        /// </summary>
        /// <param name="recordingId">      to be replayed. </param>
        /// <param name="position">         from which the replay should be started. </param>
        /// <param name="length">           of the stream to be replayed. Use <seealso cref="long.MaxValue"/> to follow a live stream. </param>
        /// <param name="replayChannel">    to which the replay should be sent. </param>
        /// <param name="replayStreamId">   to which the replay should be sent. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool Replay(
            long recordingId,
            long position,
            long length,
            string replayChannel,
            int replayStreamId,
            long correlationId,
            long controlSessionId)
        {
            return Replay(
                recordingId,
                position,
                length,
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId,
                Aeron.Aeron.NULL_VALUE);
        }

        /// <summary>
        /// Replay a recording from a given position bounded by a position counter.
        /// </summary>
        /// <param name="recordingId">      to be replayed. </param>
        /// <param name="position">         from which the replay should be started. </param>
        /// <param name="length">           of the stream to be replayed. Use <seealso cref="long.MaxValue"/> to follow a live stream. </param>
        /// <param name="limitCounterId">   to use as the replay bound. </param>
        /// <param name="replayChannel">    to which the replay should be sent. </param>
        /// <param name="replayStreamId">   to which the replay should be sent. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool BoundedReplay(
            long recordingId,
            long position,
            long length,
            int limitCounterId,
            string replayChannel,
            int replayStreamId,
            long correlationId,
            long controlSessionId)
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
                Aeron.Aeron.NULL_VALUE);
        }

        /// <summary>
        /// Stop an existing replay session.
        /// </summary>
        /// <param name="replaySessionId">  that should be stopped. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopReplay(long replaySessionId, long correlationId, long controlSessionId)
        {
            if (null == stopReplayRequest)
            {
                stopReplayRequest = new StopReplayRequestEncoder();
            }

            stopReplayRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).ReplaySessionId(replaySessionId);

            return Offer(stopReplayRequest.EncodedLength());
        }

        /// <summary>
        /// Stop any existing replay sessions for recording id or all replay sessions regardless of recording id.
        /// </summary>
        /// <param name="recordingId">      that should be stopped. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopAllReplays(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == stopAllReplaysRequest)
            {
                stopAllReplaysRequest = new StopAllReplaysRequestEncoder();
            }

            stopAllReplaysRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(stopAllReplaysRequest.EncodedLength());
        }

        /// <summary>
        /// List a range of recording descriptors.
        /// </summary>
        /// <param name="fromRecordingId">  at which to begin listing. </param>
        /// <param name="recordCount">      for the number of descriptors to be listed. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ListRecordings(long fromRecordingId, int recordCount, long correlationId, long controlSessionId)
        {
            if (null == listRecordingsRequest)
            {
                listRecordingsRequest = new ListRecordingsRequestEncoder();
            }

            listRecordingsRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).FromRecordingId(fromRecordingId).RecordCount(recordCount);

            return Offer(listRecordingsRequest.EncodedLength());
        }

        /// <summary>
        /// List a range of recording descriptors which match a channel URI fragment and stream id.
        /// </summary>
        /// <param name="fromRecordingId">  at which to begin listing. </param>
        /// <param name="recordCount">      for the number of descriptors to be listed. </param>
        /// <param name="channelFragment">  to match recordings on from the original channel URI in the archive descriptor. </param>
        /// <param name="streamId">         to match recordings on. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ListRecordingsForUri(long fromRecordingId, int recordCount, string channelFragment, int streamId,
            long correlationId, long controlSessionId)
        {
            if (null == listRecordingsForUriRequest)
            {
                listRecordingsForUriRequest = new ListRecordingsForUriRequestEncoder();
            }

            listRecordingsForUriRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).FromRecordingId(fromRecordingId).RecordCount(recordCount)
                .StreamId(streamId).Channel(channelFragment);

            return Offer(listRecordingsForUriRequest.EncodedLength());
        }

        /// <summary>
        /// List a recording descriptor for a given recording id.
        /// </summary>
        /// <param name="recordingId">      at which to begin listing. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ListRecording(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == listRecordingRequest)
            {
                listRecordingRequest = new ListRecordingRequestEncoder();
            }

            listRecordingRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(listRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Extend an existing, non-active, recorded stream for the same channel and stream id.
        /// <para>
        /// The channel must be configured for the initial position from which it will be extended. This can be done
        /// with <seealso cref="ChannelUriStringBuilder.InitialPosition(long, int, int)"/>. The details required to initialise can
        /// be found by calling <seealso cref="ListRecording(long, long, long)"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="recordingId">      to be extended. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ExtendRecording(string channel, int streamId, SourceLocation sourceLocation, long recordingId,
            long correlationId, long controlSessionId)
        {
            if (null == extendRecordingRequest)
            {
                extendRecordingRequest = new ExtendRecordingRequestEncoder();
            }

            extendRecordingRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId).StreamId(streamId).SourceLocation(sourceLocation)
                .Channel(channel);

            return Offer(extendRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Extend an existing, non-active, recorded stream for a the same channel and stream id.
        /// <para>
        /// The channel must be configured for the initial position from which it will be extended. This can be done
        /// with <seealso cref="ChannelUriStringBuilder.InitialPosition(long, int, int)"/>. The details required to initialise can
        /// be found by calling <seealso cref="ListRecording(long, long, long)"/>.
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
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ExtendRecording(string channel, int streamId, SourceLocation sourceLocation, bool autoStop,
            long recordingId, long correlationId, long controlSessionId)
        {
            if (null == extendRecordingRequest2)
            {
                extendRecordingRequest2 = new ExtendRecordingRequest2Encoder();
            }

            extendRecordingRequest2.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId).StreamId(streamId).SourceLocation(sourceLocation)
                .AutoStop(autoStop ? BooleanType.TRUE : BooleanType.FALSE).Channel(channel);

            return Offer(extendRecordingRequest2.EncodedLength());
        }

        /// <summary>
        /// Get the recorded position of an active recording.
        /// </summary>
        /// <param name="recordingId">      of the active recording that the position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool GetRecordingPosition(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == recordingPositionRequest)
            {
                recordingPositionRequest = new RecordingPositionRequestEncoder();
            }

            recordingPositionRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(recordingPositionRequest.EncodedLength());
        }

        /// <summary>
        /// Truncate a stopped recording to a given position that is less than the stopped position. The provided position
        /// must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
        ///
        /// </summary>
        /// <param name="recordingId">      of the stopped recording to be truncated. </param>
        /// <param name="position">         to which the recording will be truncated. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool TruncateRecording(long recordingId, long position, long correlationId, long controlSessionId)
        {
            if (null == truncateRecordingRequest)
            {
                truncateRecordingRequest = new TruncateRecordingRequestEncoder();
            }

            truncateRecordingRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId).Position(position);

            return Offer(truncateRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Purge a stopped recording, i.e. mark recording as <seealso cref="RecordingState.INVALID"/>
        /// and delete the corresponding segment files. The space in the Catalog will be reclaimed upon compaction.
        /// </summary>
        /// <param name="recordingId">      of the stopped recording to be purged. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool PurgeRecording(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == purgeRecordingRequest)
            {
                purgeRecordingRequest = new PurgeRecordingRequestEncoder();
            }

            purgeRecordingRequest
                .WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(purgeRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// Get the start position of a recording.
        /// </summary>
        /// <param name="recordingId">      of the recording that the position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool GetStartPosition(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == startPositionRequest)
            {
                startPositionRequest = new StartPositionRequestEncoder();
            }

            startPositionRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(startPositionRequest.EncodedLength());
        }

        /// <summary>
        /// Get the stop position of a recording.
        /// </summary>
        /// <param name="recordingId">      of the recording that the stop position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool GetStopPosition(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == stopPositionRequest)
            {
                stopPositionRequest = new StopPositionRequestEncoder();
            }

            stopPositionRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(stopPositionRequest.EncodedLength());
        }

        /// <summary>
        /// Find the last recording that matches the given criteria.
        /// </summary>
        /// <param name="minRecordingId">   to search back to. </param>
        /// <param name="channelFragment">  for a contains match on the original channel stored with the archive descriptor. </param>
        /// <param name="streamId">         of the recording to match. </param>
        /// <param name="sessionId">        of the recording to match. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool FindLastMatchingRecording(long minRecordingId, string channelFragment, int streamId, int sessionId,
            long correlationId, long controlSessionId)
        {
            if (null == findLastMatchingRecordingRequest)
            {
                findLastMatchingRecordingRequest = new FindLastMatchingRecordingRequestEncoder();
            }

            findLastMatchingRecordingRequest.WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId).CorrelationId(correlationId).MinRecordingId(minRecordingId)
                .SessionId(sessionId).StreamId(streamId).Channel(channelFragment);

            return Offer(findLastMatchingRecordingRequest.EncodedLength());
        }

        /// <summary>
        /// List registered subscriptions in the archive which have been used to record streams.
        /// </summary>
        /// <param name="pseudoIndex">       in the list of active recording subscriptions. </param>
        /// <param name="subscriptionCount"> for the number of descriptors to be listed. </param>
        /// <param name="channelFragment">   for a contains match on the stripped channel used with the registered subscription. </param>
        /// <param name="streamId">          for the subscription. </param>
        /// <param name="applyStreamId">     when matching. </param>
        /// <param name="correlationId">     for this request. </param>
        /// <param name="controlSessionId">  for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ListRecordingSubscriptions(int pseudoIndex, int subscriptionCount, string channelFragment,
            int streamId, bool applyStreamId, long correlationId, long controlSessionId)
        {
            if (null == listRecordingSubscriptionsRequest)
            {
                listRecordingSubscriptionsRequest = new ListRecordingSubscriptionsRequestEncoder();
            }

            listRecordingSubscriptionsRequest.WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId).CorrelationId(correlationId).PseudoIndex(pseudoIndex)
                .SubscriptionCount(subscriptionCount)
                .ApplyStreamId(applyStreamId ? BooleanType.TRUE : BooleanType.FALSE).StreamId(streamId)
                .Channel(channelFragment);

            return Offer(listRecordingSubscriptionsRequest.EncodedLength());
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream id.
        /// If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new destination recording is created,
        /// otherwise the provided destination recording id will be extended. The details of the source recording
        /// descriptor will be replicated.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool Replicate(
            long srcRecordingId,
            long dstRecordingId,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            long correlationId,
            long controlSessionId)
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
                NullCredentialsSupplier.NULL_CREDENTIAL);
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream id.
        /// If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new destination recording is created,
        /// otherwise the provided destination recording id will be extended. The details of the source recording
        /// descriptor will be replicated.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="stopPosition">       position to stop the replication. <seealso cref="AeronArchive.NULL_POSITION"/> to stop at end
        ///                           of current recording. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <param name="replicationChannel"> channel over which the replication will occur. Empty or null for default channel. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool Replicate(
            long srcRecordingId,
            long dstRecordingId,
            long stopPosition,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            string replicationChannel,
            long correlationId,
            long controlSessionId)
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
                NullCredentialsSupplier.NULL_CREDENTIAL);
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream id.
        /// If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new destination recording is created,
        /// otherwise the provided destination recording id will be extended. The details of the source recording
        /// descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="channelTagId">       used to tag the replication subscription. </param>
        /// <param name="subscriptionTagId">  used to tag the replication subscription. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool TaggedReplicate(
            long srcRecordingId,
            long dstRecordingId,
            long channelTagId,
            long subscriptionTagId,
            int srcControlStreamId,
            string srcControlChannel,
            string liveDestination,
            long correlationId,
            long controlSessionId)
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
                NullCredentialsSupplier.NULL_CREDENTIAL);
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The source recording will be replayed via the provided replay channel and use the original stream id.
        /// If the destination recording id is <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> then a new destination recording is created,
        /// otherwise the provided destination recording id will be extended. The details of the source recording
        /// descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="stopPosition">       position to stop the replication. <seealso cref="AeronArchive.NULL_POSITION"/> to stop at end
        ///                           of current recording. </param>
        /// <param name="channelTagId">       used to tag the replication subscription. </param>
        /// <param name="subscriptionTagId">  used to tag the replication subscription. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <param name="replicationChannel"> channel over which the replication will occur. Empty or null for default channel. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
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
            long controlSessionId)
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
                NullCredentialsSupplier.NULL_CREDENTIAL);
        }

        /// <summary>
        /// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
        /// archive. The behaviour of the replication is controlled through the <seealso cref="ReplicationParams"/>.
        /// <para>
        /// For a source recording that is still active the replay can merge with the live stream and then follow it
        /// directly and no longer require the replay from the source. This would require a multicast live destination.
        /// </para>
        /// <para>
        /// Errors will be reported asynchronously and can be checked for with <seealso cref="AeronArchive.PollForErrorResponse()"/>
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>.
        /// </para>
        /// <para>
        /// The ReplicationParams is free to be reused when this call completes.
        ///     
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="replicationParams">  optional parameters to control the behaviour of the replication. </param>
        /// <param name="correlationId">      for this request. </param>
        /// <param name="controlSessionId">   for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        /// <seealso cref="ReplicationParams"/>
        public bool Replicate(
            long srcRecordingId,
            int srcControlStreamId,
            string srcControlChannel,
            ReplicationParams replicationParams,
            long correlationId,
            long controlSessionId)
        {
            if (null != replicationParams.LiveDestination() &&
                Aeron.Aeron.NULL_VALUE != replicationParams.ReplicationSessionId())
            {
                throw new ArgumentException(
                    "ReplicationParams.LiveDestination and ReplicationParams.ReplicationSessionId can not be specified together");
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
                replicationParams.EncodedCredentials());
        }

        /// <summary>
        /// Stop an active replication by the registration id it was registered with.
        /// </summary>
        /// <param name="replicationId">    that identifies the session in the archive doing the replication. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopReplication(long replicationId, long correlationId, long controlSessionId)
        {
            if (null == stopReplicationRequest)
            {
                stopReplicationRequest = new StopReplicationRequestEncoder();
            }

            stopReplicationRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).ReplicationId(replicationId);

            return Offer(stopReplicationRequest.EncodedLength());
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
        /// <returns> true if successfully offered otherwise false. </returns>
        /// <seealso cref="AeronArchive.SegmentFileBasePosition(long, long, int, int)"></seealso>
        public bool DetachSegments(long recordingId, long newStartPosition, long correlationId, long controlSessionId)
        {
            if (null == detachSegmentsRequest)
            {
                detachSegmentsRequest = new DetachSegmentsRequestEncoder();
            }

            detachSegmentsRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId).NewStartPosition(newStartPosition);

            return Offer(detachSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Delete segments which have been previously detached from a recording.
        /// </summary>
        /// <param name="recordingId">      to which the operation applies. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        /// <seealso cref="DetachSegments(long, long, long, long)"></seealso>
        public bool DeleteDetachedSegments(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == deleteDetachedSegmentsRequest)
            {
                deleteDetachedSegmentsRequest = new DeleteDetachedSegmentsRequestEncoder();
            }

            deleteDetachedSegmentsRequest.WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId).CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(deleteDetachedSegmentsRequest.EncodedLength());
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
        /// <returns> true if successfully offered otherwise false. </returns>
        /// <seealso cref="DetachSegments(long, long, long, long)"></seealso>
        /// <seealso cref="DeleteDetachedSegments(long, long, long)"></seealso>
        /// <seealso cref="AeronArchive.SegmentFileBasePosition(long, long, int, int)"></seealso>
        public bool PurgeSegments(long recordingId, long newStartPosition, long correlationId, long controlSessionId)
        {
            if (null == purgeSegmentsRequest)
            {
                purgeSegmentsRequest = new PurgeSegmentsRequestEncoder();
            }

            purgeSegmentsRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId).NewStartPosition(newStartPosition);

            return Offer(purgeSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Attach segments to the beginning of a recording to restore history that was previously detached.
        /// <para>
        /// Segment files must match the existing recording and join exactly to the start position of the recording
        /// they are being attached to.
        /// 
        /// </para>
        /// </summary>
        /// <param name="recordingId">      to which the operation applies. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        /// <seealso cref="DetachSegments(long, long, long, long)"></seealso>
        public bool AttachSegments(long recordingId, long correlationId, long controlSessionId)
        {
            if (null == attachSegmentsRequest)
            {
                attachSegmentsRequest = new AttachSegmentsRequestEncoder();
            }

            attachSegmentsRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).RecordingId(recordingId);

            return Offer(attachSegmentsRequest.EncodedLength());
        }

        /// <summary>
        /// Migrate segments from a source recording and attach them to the beginning or end of a destination recording.
        /// <para>
        /// The source recording must match the destination recording for segment length, term length, mtu length, and
        /// stream id. The source recording must join to the destination recording on a segment boundary and without gaps,
        /// i.e., the stop position and term id of one must match the start position and term id of the other.
        /// </para>
        /// <para>
        /// The source recording must be stopped. The destination recording must be stopped if migrating segments
        /// to the end of the destination recording.
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
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool MigrateSegments(long srcRecordingId, long dstRecordingId, long correlationId, long controlSessionId)
        {
            if (null == migrateSegmentsRequest)
            {
                migrateSegmentsRequest = new MigrateSegmentsRequestEncoder();
            }

            migrateSegmentsRequest.WrapAndApplyHeader(buffer, 0, messageHeader).ControlSessionId(controlSessionId)
                .CorrelationId(correlationId).SrcRecordingId(srcRecordingId).DstRecordingId(dstRecordingId);

            return Offer(migrateSegmentsRequest.EncodedLength());
        }

        private bool Offer(int length)
        {
            retryIdleStrategy.Reset();

            int attempts = retryAttempts;
            while (true)
            {
                long result = publication.Offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
                if (result > 0)
                {
                    return true;
                }

                if (result == Aeron.Publication.CLOSED)
                {
                    throw new ArchiveException("connection to the archive has been closed");
                }

                if (result == Aeron.Publication.NOT_CONNECTED)
                {
                    throw new ArchiveException("connection to the archive is no longer available");
                }

                if (result == Aeron.Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new ArchiveException("offer failed due to max position being reached");
                }

                if (--attempts <= 0)
                {
                    return false;
                }

                retryIdleStrategy.Idle();
            }
        }

        private bool OfferWithTimeout(int length, AgentInvoker aeronClientInvoker)
        {
            retryIdleStrategy.Reset();

            long deadlineNs = nanoClock.NanoTime() + connectTimeoutNs;
            while (true)
            {
                long result = publication.Offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
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

                if (deadlineNs - nanoClock.NanoTime() < 0)
                {
                    return false;
                }

                if (null != aeronClientInvoker)
                {
                    aeronClientInvoker.Invoke();
                }

                retryIdleStrategy.Idle();
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
            int fileIoMaxLength)
        {
            if (null == replayRequest)
            {
                replayRequest = new ReplayRequestEncoder();
            }

            replayRequest
                .WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Position(position)
                .Length(length)
                .ReplayStreamId(replayStreamId)
                .FileIoMaxLength(fileIoMaxLength)
                .ReplayChannel(replayChannel);

            return Offer(replayRequest.EncodedLength());
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
            int fileIoMaxLength)
        {
            if (null == boundedReplayRequest)
            {
                boundedReplayRequest = new BoundedReplayRequestEncoder();
            }

            boundedReplayRequest
                .WrapAndApplyHeader(buffer, 0, messageHeader)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Position(position)
                .Length(length)
                .LimitCounterId(limitCounterId)
                .ReplayStreamId(replayStreamId)
                .FileIoMaxLength(fileIoMaxLength)
                .ReplayChannel(replayChannel);

            return Offer(boundedReplayRequest.EncodedLength());
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
            byte[] encodedCredentials)
        {
            if (null == replicateRequest)
            {
                replicateRequest = new ReplicateRequest2Encoder();
            }

            replicateRequest
                .WrapAndApplyHeader(buffer, 0, messageHeader)
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
                .PutEncodedCredentials(encodedCredentials, 0, encodedCredentials.Length);

            return Offer(replicateRequest.EncodedLength());
        }
    }
}