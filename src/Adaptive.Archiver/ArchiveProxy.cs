using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
    /// </summary>
    public class ArchiveProxy
    {
        /// <summary>
        /// Default number of retry attempts to be made at offering requests.
        /// </summary>
        public const int DEFAULT_RETRY_ATTEMPTS = 3;

        private readonly long connectTimeoutNs;
        private readonly int retryAttempts;
        private readonly IIdleStrategy retryIdleStrategy;
        private readonly INanoClock nanoClock;

        private readonly UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.AllocateDirect(1024)); // Should be ExpandableArrayBuffer
        private readonly Publication publication;
        private readonly MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        private readonly ConnectRequestEncoder connectRequestEncoder = new ConnectRequestEncoder();
        private readonly CloseSessionRequestEncoder closeSessionRequestEncoder = new CloseSessionRequestEncoder();
        private readonly StartRecordingRequestEncoder startRecordingRequestEncoder = new StartRecordingRequestEncoder();
        private readonly ReplayRequestEncoder replayRequestEncoder = new ReplayRequestEncoder();
        private readonly StopReplayRequestEncoder stopReplayRequestEncoder = new StopReplayRequestEncoder();
        private readonly StopRecordingRequestEncoder stopRecordingRequestEncoder = new StopRecordingRequestEncoder();

        private readonly StopRecordingSubscriptionRequestEncoder stopRecordingSubscriptionRequestEncoder =
            new StopRecordingSubscriptionRequestEncoder();

        private readonly ListRecordingsRequestEncoder listRecordingsRequestEncoder = new ListRecordingsRequestEncoder();
        private readonly ListRecordingsForUriRequestEncoder listRecordingsForUriRequestEncoder = new ListRecordingsForUriRequestEncoder();
        private readonly ListRecordingRequestEncoder listRecordingRequestEncoder = new ListRecordingRequestEncoder();
        private readonly ExtendRecordingRequestEncoder extendRecordingRequestEncoder = new ExtendRecordingRequestEncoder();
        private readonly RecordingPositionRequestEncoder recordingPositionRequestEncoder = new RecordingPositionRequestEncoder();
        private readonly TruncateRecordingRequestEncoder truncateRecordingRequestEncoder = new TruncateRecordingRequestEncoder();
        private readonly StopPositionRequestEncoder stopPositionRequestEncoder = new StopPositionRequestEncoder();
        private readonly FindLastMatchingRecordingRequestEncoder findLastMatchingRecordingRequestEncoder = new FindLastMatchingRecordingRequestEncoder();

        /// <summary>
        /// Create a proxy with a <seealso cref="Publication"/> for sending control message requests.
        /// <para>
        /// This provides a default <seealso cref="IIdleStrategy"/> of a <seealso cref="YieldingIdleStrategy"/> when offers are back pressured
        /// with a defaults of <seealso cref="AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS"/> and
        /// <seealso cref="DEFAULT_RETRY_ATTEMPTS"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="publication"> publication for sending control messages to an archive. </param>
        public ArchiveProxy(Publication publication) : this(publication, new YieldingIdleStrategy(), new SystemNanoClock(), AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS, DEFAULT_RETRY_ATTEMPTS)
        {
        }

        /// <summary>
        /// Create a proxy with a <seealso cref="Publication"/> for sending control message requests.
        /// </summary>
        /// <param name="publication">       publication for sending control messages to an archive. </param>
        /// <param name="retryIdleStrategy"> for what should happen between retry attempts at offering messages. </param>
        /// <param name="nanoClock">         to be used for calculating checking deadlines. </param>
        /// <param name="connectTimeoutNs">  for for connection requests. </param>
        /// <param name="retryAttempts">     for offering control messages before giving up. </param>
        public ArchiveProxy(Publication publication, IIdleStrategy retryIdleStrategy, INanoClock nanoClock, long connectTimeoutNs, int retryAttempts)
        {
            this.publication = publication;
            this.retryIdleStrategy = retryIdleStrategy;
            this.nanoClock = nanoClock;
            this.connectTimeoutNs = connectTimeoutNs;
            this.retryAttempts = retryAttempts;
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
            connectRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId)
                .ResponseChannel(responseChannel);

            return OfferWithTimeout(connectRequestEncoder.EncodedLength(), null);
        }

        /// <summary>
        /// Try Connect to an archive on its control interface providing the response stream details. Only one attempt will
        /// be made to offer the request.
        /// </summary>
        /// <param name="responseChannel">  for the control message responses. </param>
        /// <param name="responseStreamId"> for the control message responses. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool TryConnect(string responseChannel, int responseStreamId, long correlationId)
        {
            connectRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId)
                .ResponseChannel(responseChannel);

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
        public bool Connect(string responseChannel, int responseStreamId, long correlationId, AgentInvoker aeronClientInvoker)
        {
            connectRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .CorrelationId(correlationId)
                .ResponseStreamId(responseStreamId)
                .ResponseChannel(responseChannel);

            return OfferWithTimeout(connectRequestEncoder.EncodedLength(), aeronClientInvoker);
        }

        /// <summary>
        /// Close this control session with the archive.
        /// </summary>
        /// <param name="controlSessionId"> with the archive. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool CloseSession(long controlSessionId)
        {
            closeSessionRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId);

            return Offer(closeSessionRequestEncoder.EncodedLength());
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
        public bool StartRecording(string channel, int streamId, SourceLocation sourceLocation, long correlationId, long controlSessionId)
        {
            startRecordingRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .StreamId(streamId)
                .SourceLocation(sourceLocation)
                .Channel(channel);

            return Offer(startRecordingRequestEncoder.EncodedLength());
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
            stopRecordingRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .StreamId(streamId)
                .Channel(channel);

            return Offer(stopRecordingRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Stop an active recording by the <seealso cref="Subscription.RegistrationId"/> it was registered with.
        /// </summary>
        /// <param name="subscriptionId">   that identifies the subscription in the archive doing the recording. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool StopRecording(long subscriptionId, long correlationId, long controlSessionId)
        {
            stopRecordingSubscriptionRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .SubscriptionId(subscriptionId);

            return Offer(stopRecordingSubscriptionRequestEncoder.EncodedLength());
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
        public bool Replay(long recordingId, long position, long length, string replayChannel, int replayStreamId, long correlationId, long controlSessionId)
        {
            replayRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .Position(position)
                .Length(length)
                .ReplayStreamId(replayStreamId)
                .ReplayChannel(replayChannel);

            return Offer(replayRequestEncoder.EncodedLength());
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
            stopReplayRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .ReplaySessionId(replaySessionId);

            return Offer(replayRequestEncoder.EncodedLength());
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
            listRecordingsRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .FromRecordingId(fromRecordingId)
                .RecordCount(recordCount);

            return Offer(listRecordingsRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// List a range of recording descriptors which match a channel and stream id.
        /// </summary>
        /// <param name="fromRecordingId">  at which to begin listing. </param>
        /// <param name="recordCount">      for the number of descriptors to be listed. </param>
        /// <param name="channel">          to match recordings on. </param>
        /// <param name="streamId">         to match recordings on. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ListRecordingsForUri(long fromRecordingId, int recordCount, string channel, int streamId, long correlationId, long controlSessionId)
        {
            listRecordingsForUriRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .FromRecordingId(fromRecordingId)
                .RecordCount(recordCount)
                .StreamId(streamId)
                .Channel(channel);

            return Offer(listRecordingsForUriRequestEncoder.EncodedLength());
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
            listRecordingRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(listRecordingRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Extend a recorded stream for a given channel and stream id pairing.
        /// </summary>
        /// <param name="channel">          to be recorded. </param>
        /// <param name="streamId">         to be recorded. </param>
        /// <param name="sourceLocation">   of the publication to be recorded. </param>
        /// <param name="recordingId">      to be extended. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool ExtendRecording(string channel, int streamId, SourceLocation sourceLocation, long recordingId, long correlationId, long controlSessionId)
        {
            extendRecordingRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId)
                .StreamId(streamId)
                .SourceLocation(sourceLocation)
                .Channel(channel);

            return Offer(extendRecordingRequestEncoder.EncodedLength());
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
            recordingPositionRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(recordingPositionRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Truncate a stopped recording to a given position that is less than the stopped position. The provided position
        /// must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
        /// </summary>
        /// <param name="recordingId">      of the stopped recording to be truncated. </param>
        /// <param name="position">         to which the recording will be truncated. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool TruncateRecording(long recordingId, long position, long correlationId, long controlSessionId)
        {
            truncateRecordingRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId).Position(position);

            return Offer(truncateRecordingRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Get the stop position of a recording.
        /// </summary>
        /// <param name="recordingId">      of the recording that the stop position is being requested for. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool GetStopPosition(
            long recordingId,
            long correlationId,
            long controlSessionId)
        {
            stopPositionRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .RecordingId(recordingId);

            return Offer(stopPositionRequestEncoder.EncodedLength());
        }

        /// <summary>
        /// Truncate a stopped recording to a given position that is less than the stopped position. The provided position
        /// must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
        /// <para>
        /// Find the last recording that matches the given criteria.
        /// 
        /// </para>
        /// </summary>
        /// <param name="minRecordingId">   to search back to. </param>
        /// <param name="channel">          for a contains match on the stripped channel stored with the archive descriptor. </param>
        /// <param name="streamId">         of the recording to match. </param>
        /// <param name="sessionId">        of the recording to match. </param>
        /// <param name="correlationId">    for this request. </param>
        /// <param name="controlSessionId"> for this request. </param>
        /// <returns> true if successfully offered otherwise false. </returns>
        public bool FindLastMatchingRecording(
            long minRecordingId,
            string channel,
            int streamId,
            int sessionId,
            long correlationId,
            long controlSessionId)
        {
            findLastMatchingRecordingRequestEncoder
                .WrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
                .ControlSessionId(controlSessionId)
                .CorrelationId(correlationId)
                .MinRecordingId(minRecordingId)
                .SessionId(sessionId)
                .StreamId(streamId)
                .Channel(channel);

            return Offer(findLastMatchingRecordingRequestEncoder.EncodedLength());
        }

        private bool Offer(int length)
        {
            retryIdleStrategy.Reset();

            int attempts = retryAttempts;
            while (true)
            {
                long result;
                if ((result = publication.Offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length)) > 0)
                {
                    return true;
                }

                if (result == Publication.CLOSED)
                {
                    throw new ArchiveException("connection to the archive has been closed");
                }

                if (result == Publication.NOT_CONNECTED)
                {
                    throw new ArchiveException("connection to the archive is no longer available");
                }

                if (result == Publication.MAX_POSITION_EXCEEDED)
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
                long result;
                if ((result = publication.Offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length)) > 0)
                {
                    return true;
                }

                if (null != aeronClientInvoker)
                {
                    aeronClientInvoker.Invoke();
                }

                if (result == Publication.CLOSED)
                {
                    throw new ArchiveException("connection to the archive has been closed");
                }

                if (result == Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new ArchiveException("offer failed due to max position being reached");
                }

                if (deadlineNs - nanoClock.NanoTime() < 0)
                {
                    return false;
                }

                retryIdleStrategy.Idle();
            }
        }
    }
}