using System;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Archiver.Codecs;

namespace Adaptive.Archiver
{
    /// <summary>
    /// Client for interacting with a local or remote Aeron Archive that records and replays message streams.
    /// <para>
    /// This client provides a simple interaction model which is mostly synchronous and may not be optimal.
    /// The underlying components such as the <seealso cref="ArchiveProxy"/> and the <seealso cref="ControlResponsePoller"/> or
    /// <seealso cref="RecordingDescriptorPoller"/> may be used directly if a more asynchronous interaction is required.
    /// </para>
    /// <para>
    /// Note: This class is threadsafe but the lock can be elided for single threaded access via <seealso cref="Adaptive.Aeron.Aeron.Context.ClientLock(Adaptive.Agrona.Concurrent.ILock)"/>
    /// being set to <seealso cref="NoOpLock"/>.
    /// </para>
    /// </summary>
    public class AeronArchive : IDisposable
    {
        /// <summary>
        /// Represents a timestamp that has not been set. Can be used when the time is not known.
        /// </summary>
        public const long NULL_TIMESTAMP = Aeron.Aeron.NULL_VALUE;

        /// <summary>
        /// Represents a position that has not been set. Can be used when the position is not known.
        /// </summary>
        public const long NULL_POSITION = Aeron.Aeron.NULL_VALUE;

        /// <summary>
        /// Represents a length that has not been set. If null length is provided then replay the whole recorded stream.
        /// </summary>
        public const long NULL_LENGTH = Aeron.Aeron.NULL_VALUE;

        private const int FRAGMENT_LIMIT = 10;

        private bool isClosed = false;
        private readonly long controlSessionId;
        private readonly long messageTimeoutNs;
        private readonly Context context;
        private readonly Aeron.Aeron aeron;
        private readonly ArchiveProxy archiveProxy;
        private readonly IIdleStrategy idleStrategy;
        private readonly ControlResponsePoller controlResponsePoller;
        private readonly RecordingDescriptorPoller recordingDescriptorPoller;
        private readonly ILock _lock;
        private readonly INanoClock nanoClock;
        private readonly AgentInvoker aeronClientInvoker;

        internal AeronArchive(Context ctx)
        {
            Subscription subscription = null;
            Publication publication = null;
            try
            {
                ctx.Conclude();

                context = ctx;
                aeron = ctx.AeronClient();
                aeronClientInvoker = aeron.ConductorAgentInvoker;
                idleStrategy = ctx.IdleStrategy();
                messageTimeoutNs = ctx.MessageTimeoutNs();
                _lock = ctx.Lock();
                nanoClock = aeron.Ctx.NanoClock();

                subscription = aeron.AddSubscription(ctx.ControlResponseChannel(), ctx.ControlResponseStreamId());
                controlResponsePoller = new ControlResponsePoller(subscription);

                publication = aeron.AddExclusivePublication(ctx.ControlRequestChannel(), ctx.ControlRequestStreamId());
                archiveProxy = new ArchiveProxy(publication, idleStrategy, nanoClock, messageTimeoutNs,
                    ArchiveProxy.DEFAULT_RETRY_ATTEMPTS);

                long correlationId = aeron.NextCorrelationId();
                if (!archiveProxy.Connect(ctx.ControlResponseChannel(), ctx.ControlResponseStreamId(), correlationId,
                    aeronClientInvoker))
                {
                    throw new ArchiveException(
                        "cannot connect to aeron archive: " + ctx.ControlRequestChannel());
                }

                controlSessionId = AwaitSessionOpened(correlationId);
                recordingDescriptorPoller =
                    new RecordingDescriptorPoller(subscription, ctx.ErrorHandler(), controlSessionId, FRAGMENT_LIMIT);
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    CloseHelper.QuietDispose(subscription);
                    CloseHelper.QuietDispose(publication);
                }

                ctx.Dispose();

                throw;
            }
        }

        public AeronArchive(
            Context context,
            ControlResponsePoller controlResponsePoller,
            ArchiveProxy archiveProxy,
            RecordingDescriptorPoller recordingDescriptorPoller,
            long controlSessionId)
        {
            this.controlSessionId = controlSessionId;
            this.context = context;
            this.archiveProxy = archiveProxy;
            this.controlResponsePoller = controlResponsePoller;
            this.recordingDescriptorPoller = recordingDescriptorPoller;
        }

        /// <summary>
        /// Notify the archive that this control session is closed so it can promptly release resources then close the
        /// local resources associated with the client.
        /// </summary>
        public void Dispose()
        {
            _lock.Lock();
            try
            {
                if (!isClosed)
                {
                    isClosed = true;
                    archiveProxy.CloseSession(controlSessionId);

                    if (!context.OwnsAeronClient())
                    {
                        controlResponsePoller.Subscription()?.Dispose();
                        archiveProxy.Pub()?.Dispose();
                    }

                    context.Dispose();
                }
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Connect to an Aeron archive using a default <seealso cref="Adaptive.Aeron.Aeron.Context"/>. This will create a control session.
        /// </summary>
        /// <returns> the newly created Aeron Archive client. </returns>
        public static AeronArchive Connect()
        {
            return new AeronArchive(new Context());
        }

        /// <summary>
        /// Connect to an Aeron archive by providing a <seealso cref="Adaptive.Aeron.Aeron.Context"/>. This will create a control session.
        /// <para>
        /// Before connecting <seealso cref="Adaptive.Aeron.Aeron.Context.Conclude()"/> will be called.
        /// If an exception occurs then <seealso cref="Adaptive.Aeron.Aeron.Context.Dispose()"/> will be called.
        /// 
        /// </para>
        /// </summary>
        /// <param name="context"> for connection configuration. </param>
        /// <returns> the newly created Aeron Archive client. </returns>
        public static AeronArchive Connect(Context context)
        {
            return new AeronArchive(context);
        }

        /// <summary>
        /// Begin an attempt at creating a connection which can be completed by calling <seealso cref="AsyncConnect.Poll()"/>.
        /// </summary>
        /// <returns> the <seealso cref="AsyncConnect"/> that cannot be polled for completion. </returns>
        public static AsyncConnect ConnectAsync()
        {
            return ConnectAsync(new Context());
        }

        /// <summary>
        /// Begin an attempt at creating a connection which can be completed by calling <seealso cref="AsyncConnect.Poll()"/>.
        /// </summary>
        /// <param name="ctx"> for the archive connection. </param>
        /// <returns> the <seealso cref="AsyncConnect"/> that cannot be polled for completion. </returns>
        public static AsyncConnect ConnectAsync(Context ctx)
        {
            Subscription subscription = null;
            Publication publication = null;
            try
            {
                ctx.Conclude();

                Aeron.Aeron aeron = ctx.AeronClient();
                long messageTimeoutNs = ctx.MessageTimeoutNs();

                subscription = aeron.AddSubscription(ctx.ControlResponseChannel(), ctx.ControlResponseStreamId());
                ControlResponsePoller controlResponsePoller = new ControlResponsePoller(subscription);

                publication = aeron.AddExclusivePublication(ctx.ControlRequestChannel(), ctx.ControlRequestStreamId());
                ArchiveProxy archiveProxy = new ArchiveProxy(publication,
                    ctx.IdleStrategy(),
                    aeron.Ctx.NanoClock(),
                    messageTimeoutNs,
                    ArchiveProxy.DEFAULT_RETRY_ATTEMPTS);

                return new AsyncConnect(ctx, controlResponsePoller, archiveProxy);
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    CloseHelper.QuietDispose(subscription);
                    CloseHelper.QuietDispose(publication);
                }

                ctx.Dispose();

                throw;
            }
        }


        /// <summary>
        /// Get the <seealso cref="Adaptive.Aeron.Aeron.Context"/> used to connect this archive client.
        /// </summary>
        /// <returns> the <seealso cref="Adaptive.Aeron.Aeron.Context"/> used to connect this archive client. </returns>
        public Context Ctx()
        {
            return context;
        }

        /// <summary>
        /// The control session id allocated for this connection to the archive.
        /// </summary>
        /// <returns> control session id allocated for this connection to the archive. </returns>
        public long ControlSessionId()
        {
            return controlSessionId;
        }

        /// <summary>
        /// The <seealso cref="ArchiveProxy"/> for send asynchronous messages to the connected archive.
        /// </summary>
        /// <returns> the <seealso cref="ArchiveProxy"/> for send asynchronous messages to the connected archive. </returns>
        public ArchiveProxy Proxy()
        {
            return archiveProxy;
        }

        /// <summary>
        /// Get the <seealso cref="ControlResponsePoller"/> for polling additional events on the control channel.
        /// </summary>
        /// <returns> the <seealso cref="ControlResponsePoller"/> for polling additional events on the control channel. </returns>
        public ControlResponsePoller ControlResponsePoller()
        {
            return controlResponsePoller;
        }

        /// <summary>
        /// Get the <seealso cref="RecordingDescriptorPoller"/> for polling recording descriptors on the control channel.
        /// </summary>
        /// <returns> the <seealso cref="RecordingDescriptorPoller"/> for polling recording descriptors on the control channel. </returns>
        public RecordingDescriptorPoller RecordingDescriptorPoller()
        {
            return recordingDescriptorPoller;
        }

        /// <summary>
        /// Poll the response stream once for an error. If another message is present then it will be skipped over
        /// so only call when not expecting another response.
        /// </summary>
        /// <returns> the error String otherwise null if no error is found. </returns>
        public string PollForErrorResponse()
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                if (controlResponsePoller.Poll() != 0 && controlResponsePoller.IsPollComplete())
                {
                    if (controlResponsePoller.ControlSessionId() == controlSessionId &&
                        controlResponsePoller.TemplateId() == ControlResponseDecoder.TEMPLATE_ID &&
                        controlResponsePoller.Code() == ControlResponseCode.ERROR)
                    {
                        return controlResponsePoller.ErrorMessage();
                    }
                }

                return null;
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Check if an error has been returned for the control session and throw a <seealso cref="ArchiveException"/> if
        /// <see cref="Context.ErrorHandler(Adaptive.Agrona.ErrorHandler)"/> is not set.
        /// 
        /// To check for an error response without raising an exception then try <seealso cref="PollForErrorResponse()"/>.
        /// </summary>
        ///  <seealso cref="PollForErrorResponse()"/>
        public void CheckForErrorResponse()
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                if (controlResponsePoller.Poll() != 0 && controlResponsePoller.IsPollComplete())
                {
                    if (controlResponsePoller.ControlSessionId() == controlSessionId &&
                        controlResponsePoller.TemplateId() == ControlResponseDecoder.TEMPLATE_ID &&
                        controlResponsePoller.Code() == ControlResponseCode.ERROR)
                    {
                        var ex = new ArchiveException(controlResponsePoller.ErrorMessage(),
                            (int) controlResponsePoller.RelevantId());

                        if (null != context.ErrorHandler())
                        {
                            context.ErrorHandler()(ex);
                        }
                        else
                        {
                            throw ex;
                        }
                    }
                }
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Add a <seealso cref="Publication"/> and set it up to be recorded. If this is not the first,
        /// i.e. <seealso cref="Publication.IsOriginal"/> is true,  then an <seealso cref="ArchiveException"/>
        /// will be thrown and the recording not initiated.
        /// <para>
        /// This is a sessionId specific recording.
        /// 
        /// </para>
        /// </summary>
        /// <param name="channel">  for the publication. </param>
        /// <param name="streamId"> for the publication. </param>
        /// <returns> the <seealso cref="Publication"/> ready for use. </returns>
        public Publication AddRecordedPublication(string channel, int streamId)
        {
            Publication publication = null;
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                publication = aeron.AddPublication(channel, streamId);
                if (!publication.IsOriginal)
                {
                    publication.Dispose();

                    throw new ArchiveException(
                        "publication already added for channel=" + channel + " streamId=" + streamId);
                }

                StartRecording(ChannelUri.AddSessionId(channel, publication.SessionId), streamId, SourceLocation.LOCAL);
            }
            catch (Exception)
            {
                CloseHelper.QuietDispose(publication);
                throw;
            }
            finally
            {
                _lock.Unlock();
            }

            return publication;
        }

        /// <summary>
        /// Add a <seealso cref="ExclusivePublication"/> and set it up to be recorded.
        /// <para>
        /// This is a sessionId specific recording.
        /// 
        /// </para>
        /// </summary>
        /// <param name="channel">  for the publication. </param>
        /// <param name="streamId"> for the publication. </param>
        /// <returns> the <seealso cref="ExclusivePublication"/> ready for use. </returns>
        public ExclusivePublication AddRecordedExclusivePublication(string channel, int streamId)
        {
            ExclusivePublication publication = null;
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                publication = aeron.AddExclusivePublication(channel, streamId);
                StartRecording(ChannelUri.AddSessionId(channel, publication.SessionId), streamId, SourceLocation.LOCAL);
            }
            catch (Exception)
            {
                CloseHelper.QuietDispose(publication);
                throw;
            }
            finally
            {
                _lock.Unlock();
            }

            return publication;
        }

        /// <summary>
        /// Start recording a channel and stream pairing.
        /// <para>
        /// Channels that include sessionId parameters are considered different than channels without sessionIds. If a
        /// publication matches both a sessionId specific channel recording and a non-sessionId specific recording, it will
        /// be recorded twice.
        /// 
        /// </para>
        /// </summary>
        /// <param name="channel">        to be recorded. </param>
        /// <param name="streamId">       to be recorded. </param>
        /// <param name="sourceLocation"> of the publication to be recorded. </param>
        /// <returns> the subscriptionId, i.e. <see cref="Subscription.RegistrationId"/>, of the recording. </returns>
        public long StartRecording(string channel, int streamId, SourceLocation sourceLocation)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.StartRecording(channel, streamId, sourceLocation, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send start recording request");
                }

                return PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Extend an existing, non-active recording of a channel and stream pairing.
        /// <para>
        /// Channel must be session specific and include the existing recording sessionId.
        /// 
        /// </para>
        /// </summary>
        /// <param name="recordingId">    of the existing recording. </param>
        /// <param name="channel">        to be recorded. </param>
        /// <param name="streamId">       to be recorded. </param>
        /// <param name="sourceLocation"> of the publication to be recorded. </param>
        /// <returns> the subscriptionId, i.e. <see cref="Subscription.RegistrationId"/>, of the recording. </returns>
        public long ExtendRecording(long recordingId, string channel, int streamId,
            SourceLocation sourceLocation)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.ExtendRecording(channel, streamId, sourceLocation, recordingId, correlationId,
                    controlSessionId))
                {
                    throw new ArchiveException("failed to send extend recording request");
                }

                return PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Stop recording for a channel and stream pairing.
        /// <para>
        /// Channels that include sessionId parameters are considered different than channels without sessionIds. Stopping
        /// a recording on a channel without a sessionId parameter will not stop the recording of any sessionId specific
        /// recordings that use the same channel and streamId.
        /// 
        /// </para>
        /// </summary>
        /// <param name="channel">  to stop recording for. </param>
        /// <param name="streamId"> to stop recording for. </param>
        public void StopRecording(string channel, int streamId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecording(channel, streamId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Stop recording a sessionId specific recording that pertains to the given <seealso cref="Publication"/>.
        /// </summary>
        /// <param name="publication"> to stop recording for. </param>
        public void StopRecording(Publication publication)
        {
            string recordingChannel = ChannelUri.AddSessionId(publication.Channel, publication.SessionId);

            StopRecording(recordingChannel, publication.StreamId);
        }

        /// <summary>
        /// Stop recording for a subscriptionId that has been returned from
        /// <seealso cref="StartRecording(String, int, SourceLocation)"/> or
        /// <seealso cref="ExtendRecording(long, String, int, SourceLocation)"/>.
        /// </summary>
        /// <param name="subscriptionId"> is the <see cref="Subscription.RegistrationId"/> was registered with for the recording. </param>
        public void StopRecording(long subscriptionId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecording(subscriptionId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }


        /// <summary>
        /// Start a replay for a length in bytes of a recording from a position. If the position is <seealso cref="NULL_POSITION"/>
        /// then the stream will be replayed from the start.
        ///
        /// The lower 32-bits of the returned value contain the <see cref="Image.SessionId"/> of the received replay. All
        /// 64-bits are required to uniquely identify the replay when calling <see cref="StopReplay(long)"/>. The lower 32-bits
        /// can be obtained by casting the <see cref="long"/> value to an <see cref="int"/>.
        /// 
        /// </summary>
        /// <param name="recordingId">    to be replayed. </param>
        /// <param name="position">       from which the replay should begin or <seealso cref="NULL_POSITION"/> if from the start. </param>
        /// <param name="length">         of the stream to be replayed. Use <seealso cref="long.MaxValue"/> to follow a live recording or <see cref="NULL_LENGTH"/> to replay the whole stream of unknown length. </param>
        /// <param name="replayChannel">  to which the replay should be sent. </param>
        /// <param name="replayStreamId"> to which the replay should be sent. </param>
        /// <returns> the id of the replay session which will be the same as the <seealso cref="Image.SessionId"/> of the received
        /// replay for correlation with the matching channel and stream id in the lower 32 bits. </returns>
        public long StartReplay(long recordingId, long position, long length, string replayChannel,
            int replayStreamId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replay(recordingId, position, length, replayChannel, replayStreamId, correlationId,
                    controlSessionId))
                {
                    throw new ArchiveException("failed to send replay request");
                }

                return PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Stop a replay session.
        /// </summary>
        /// <param name="replaySessionId"> to stop replay for. </param>
        public void StopReplay(long replaySessionId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopReplay(replaySessionId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Replay a length in bytes of a recording from a position and for convenience create a <seealso cref="Subscription"/>
        /// to receive the replay. If the position is <seealso cref="NULL_POSITION"/> then the stream will be replayed from the start.
        /// </summary>
        /// <param name="recordingId">    to be replayed. </param>
        /// <param name="position">       from which the replay should begin or <seealso cref="NULL_POSITION"/> if from the start. </param>
        /// <param name="length">         of the stream to be replayed or <seealso cref="long.MaxValue"/> to follow a live recording. </param>
        /// <param name="replayChannel">  to which the replay should be sent. </param>
        /// <param name="replayStreamId"> to which the replay should be sent. </param>
        /// <returns> the <seealso cref="Subscription"/> for consuming the replay. </returns>
        public Subscription Replay(long recordingId, long position, long length, string replayChannel,
            int replayStreamId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                ChannelUri replayChannelUri = ChannelUri.Parse(replayChannel);
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replay(recordingId, position, length, replayChannel, replayStreamId, correlationId,
                    controlSessionId))
                {
                    throw new ArchiveException("failed to send replay request");
                }

                int replaySessionId = (int) PollForResponse(correlationId);
                replayChannelUri.Put(Aeron.Aeron.Context.SESSION_ID_PARAM_NAME, Convert.ToString(replaySessionId));

                return aeron.AddSubscription(replayChannelUri.ToString(), replayStreamId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Replay a length in bytes of a recording from a position and for convenience create a <seealso cref="Subscription"/>
        /// to receive the replay. If the position is <seealso cref="NULL_POSITION"/> then the stream will be replayed from the start.
        /// </summary>
        /// <param name="recordingId">             to be replayed. </param>
        /// <param name="position">                from which the replay should begin or <seealso cref="NULL_POSITION"/> if from the start. </param>
        /// <param name="length">                  of the stream to be replayed or <seealso cref="long.MaxValue"/> to follow a live recording. </param>
        /// <param name="replayChannel">           to which the replay should be sent. </param>
        /// <param name="replayStreamId">          to which the replay should be sent. </param>
        /// <param name="availableImageHandler">   to be called when the replay image becomes available. </param>
        /// <param name="unavailableImageHandler"> to be called when the replay image goes unavailable. </param>
        /// <returns> the <seealso cref="Subscription"/> for consuming the replay. </returns>
        public Subscription Replay(long recordingId, long position, long length, string replayChannel,
            int replayStreamId, AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                ChannelUri replayChannelUri = ChannelUri.Parse(replayChannel);
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replay(recordingId, position, length, replayChannel, replayStreamId, correlationId,
                    controlSessionId))
                {
                    throw new ArchiveException("failed to send replay request");
                }

                int replaySessionId = (int) PollForResponse(correlationId);
                replayChannelUri.Put(Aeron.Aeron.Context.SESSION_ID_PARAM_NAME, Convert.ToString(replaySessionId));

                return aeron.AddSubscription(replayChannelUri.ToString(), replayStreamId, availableImageHandler,
                    unavailableImageHandler);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// List all recording descriptors from a recording id with a limit of record count.
        /// <para>
        /// If the recording id is greater than the largest known id then nothing is returned.
        /// 
        /// </para>
        /// </summary>
        /// <param name="fromRecordingId"> at which to begin the listing. </param>
        /// <param name="recordCount">     to limit for each query. </param>
        /// <param name="consumer">        to which the descriptors are dispatched. </param>
        /// <returns> the number of descriptors found and consumed. </returns>
        public int ListRecordings(long fromRecordingId, int recordCount, IRecordingDescriptorConsumer consumer)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecordings(fromRecordingId, recordCount, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send list recordings request");
                }

                return PollForDescriptors(correlationId, recordCount, consumer);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// List recording descriptors from a recording id with a limit of record count for a given channelFragment and stream id.
        /// <para>
        /// If the recording id is greater than the largest known id then nothing is returned.
        /// 
        /// </para>
        /// </summary>
        /// <param name="fromRecordingId"> at which to begin the listing. </param>
        /// <param name="recordCount">     to limit for each query. </param>
        /// <param name="channelFragment"> for a contains match on the original channel stored with the archive descriptor. </param>
        /// <param name="streamId">        to match. </param>
        /// <param name="consumer">        to which the descriptors are dispatched. </param>
        /// <returns> the number of descriptors found and consumed. </returns>
        public int ListRecordingsForUri(
            long fromRecordingId,
            int recordCount,
            string channelFragment,
            int streamId,
            IRecordingDescriptorConsumer consumer)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecordingsForUri(fromRecordingId, recordCount, channelFragment, streamId, correlationId,
                    controlSessionId))
                {
                    throw new ArchiveException("failed to send list recordings request");
                }

                return PollForDescriptors(correlationId, recordCount, consumer);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// List all recording descriptors from a recording id with a limit of record count.
        /// <para>
        /// If the recording id is greater than the largest known id then nothing is returned.
        /// 
        /// </para>
        /// </summary>
        /// <param name="recordingId"> at which to begin the listing. </param>
        /// <param name="consumer">    to which the descriptors are dispatched. </param>
        /// <returns> the number of descriptors found and consumed. </returns>
        public int ListRecording(long recordingId, IRecordingDescriptorConsumer consumer)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecording(recordingId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send list recording request");
                }

                return PollForDescriptors(correlationId, 1, consumer);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Get the position recorded for an active recording. If no active recording the return <see cref="NULL_POSITION"/>
        /// </summary>
        /// <param name="recordingId"> of the active recording for which the position is required. </param>
        /// <returns> the recorded position for the active recording or <seealso cref="NULL_POSITION"/> if recording not active. </returns>
        /// <seealso cref="GetStopPosition"/>
        public long GetRecordingPosition(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.GetRecordingPosition(recordingId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send get recording position request");
                }

                return PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Get the stop position for a recording.
        /// </summary>
        /// <param name="recordingId"> of the active recording for which the position is required. </param>
        /// <returns> the stop position, or <seealso cref="AeronArchive.NULL_POSITION"/> if still active. </returns>
        /// <seealso cref="GetRecordingPosition"/>
        public long GetStopPosition(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.GetStopPosition(recordingId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send get stop position request");
                }

                return PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Find the last recording that matches the given criteria.
        /// </summary>
        /// <param name="minRecordingId">  to search back to. </param>
        /// <param name="channelFragment"> for a contains match on the stripped channel stored with the archive descriptor </param>
        /// <param name="streamId">        of the recording to match. </param>
        /// <param name="sessionId">       of the recording to match. </param>
        /// <returns> the recordingId if found otherwise <see cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not found. </returns>
        public long FindLastMatchingRecording(long minRecordingId, string channelFragment, int streamId, int sessionId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.FindLastMatchingRecording(
                    minRecordingId, channelFragment, streamId, sessionId, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send find last matching request");
                }

                return PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Truncate a stopped recording to a given position that is less than the stopped position. The provided position
        /// must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
        /// </summary>
        /// <param name="recordingId"> of the stopped recording to be truncated. </param>
        /// <param name="position">    to which the recording will be truncated. </param>
        public void TruncateRecording(long recordingId, long position)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                
                long correlationId = aeron.NextCorrelationId();

                if (!archiveProxy.TruncateRecording(recordingId, position, correlationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send truncate recording request");
                }

                PollForResponse(correlationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        private void CheckDeadline(long deadlineNs, string errorMessage, long correlationId)
        {
            Thread.Sleep(0); // allow thread to be interrupted

            if (deadlineNs - nanoClock.NanoTime() < 0)
            {
                throw new TimeoutException(errorMessage + " - correlationId=" + correlationId);
            }
        }

        private long AwaitSessionOpened(long correlationId)
        {
            long deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
            ControlResponsePoller poller = controlResponsePoller;

            AwaitConnection(deadlineNs, poller, correlationId);

            while (true)
            {
                PollNextResponse(correlationId, deadlineNs, poller);

                if (poller.CorrelationId() != correlationId ||
                    poller.TemplateId() != ControlResponseDecoder.TEMPLATE_ID)
                {
                    InvokeAeronClient();
                    continue;
                }

                ControlResponseCode code = poller.Code();
                if (code != ControlResponseCode.OK)
                {
                    if (code == ControlResponseCode.ERROR)
                    {
                        throw new ArchiveException("error: " + poller.ErrorMessage(), (int) poller.RelevantId());
                    }

                    throw new ArchiveException("unexpected response: code=" + code);
                }

                return poller.ControlSessionId();
            }
        }

        private void AwaitConnection(long deadlineNs, ControlResponsePoller poller, long correlationId)
        {
            idleStrategy.Reset();

            while (!poller.Subscription().IsConnected)
            {
                CheckDeadline(deadlineNs, "failed to establish response connection", correlationId);
                idleStrategy.Idle();
                InvokeAeronClient();
            }
        }

        private long PollForResponse(long correlationId)
        {
            long deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
            ControlResponsePoller poller = controlResponsePoller;

            while (true)
            {
                PollNextResponse(correlationId, deadlineNs, poller);

                if (poller.ControlSessionId() != controlSessionId ||
                    poller.TemplateId() != ControlResponseDecoder.TEMPLATE_ID)
                {
                    InvokeAeronClient();
                    continue;
                }

                var code = poller.Code();
                if (code == ControlResponseCode.ERROR)
                {
                    var ex = new ArchiveException("response for correlationId=" + correlationId + ", error: " +
                                                  poller.ErrorMessage(), (int) poller.RelevantId());

                    if (poller.CorrelationId() == correlationId)
                    {
                        throw ex;
                    }

                    if (context.ErrorHandler() != null)
                    {
                        context.ErrorHandler().Invoke(ex);
                    }
                }
                else if (poller.CorrelationId() == correlationId)
                {
                    if (ControlResponseCode.OK != code)
                    {
                        throw new ArchiveException("unexpected response code: " + code);
                    }

                    return poller.RelevantId();
                }
            }
        }

        private void PollNextResponse(long correlationId, long deadlineNs, ControlResponsePoller poller)
        {
            idleStrategy.Reset();

            while (true)
            {
                int fragments = poller.Poll();

                if (poller.IsPollComplete())
                {
                    break;
                }

                if (fragments > 0)
                {
                    continue;
                }

                if (!poller.Subscription().IsConnected)
                {
                    throw new ArchiveException("subscription to archive is not connected");
                }

                CheckDeadline(deadlineNs, "awaiting response", correlationId);

                idleStrategy.Idle();
                InvokeAeronClient();
            }
        }

        private int PollForDescriptors(long correlationId, int recordCount, IRecordingDescriptorConsumer consumer)
        {
            int existingRemainCount = recordCount;
            long deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
            RecordingDescriptorPoller poller = recordingDescriptorPoller;
            poller.Reset(correlationId, recordCount, consumer);
            idleStrategy.Reset();

            while (true)
            {
                int fragments = poller.Poll();
                int remainingRecordCount = poller.RemainingRecordCount();

                if (poller.IsDispatchComplete())
                {
                    return recordCount - remainingRecordCount;
                }

                if (remainingRecordCount != existingRemainCount)
                {
                    existingRemainCount = remainingRecordCount;
                    deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
                }

                InvokeAeronClient();

                if (fragments > 0)
                {
                    continue;
                }

                if (!poller.Subscription().IsConnected)
                {
                    throw new ArchiveException("subscription to archive is not connected");
                }

                CheckDeadline(deadlineNs, "awaiting recording descriptors", correlationId);
                idleStrategy.Idle();
            }
        }

        private void InvokeAeronClient()
        {
            if (null != aeronClientInvoker)
            {
                aeronClientInvoker.Invoke();
            }
        }

        private void EnsureOpen()
        {
            if (isClosed)
            {
                throw new ArchiveException("client is closed");
            }
        }
        
        /// <summary>
        /// Common configuration properties for communicating with an Aeron archive.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Timeout in nanoseconds when waiting on a message to be sent or received.
            /// </summary>
            public const string MESSAGE_TIMEOUT_PROP_NAME = "aeron.archive.message.timeout";

            /// <summary>
            /// Timeout when waiting on a message to be sent or received.
            /// </summary>
            public static readonly long MESSAGE_TIMEOUT_DEFAULT_NS = 5000000000;

            /// <summary>
            /// Channel for sending control messages to an archive.
            /// </summary>
            public const string CONTROL_CHANNEL_PROP_NAME = "aeron.archive.control.channel";

            /// <summary>
            /// Channel for sending control messages to an archive.
            /// </summary>
            public const string CONTROL_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8010";

            /// <summary>
            /// Stream id within a channel for sending control messages to an archive.
            /// </summary>
            public const string CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.control.stream.id";

            /// <summary>
            /// Stream id within a channel for sending control messages to an archive.
            /// </summary>
            public const int CONTROL_STREAM_ID_DEFAULT = 10;

            /// <summary>
            /// Channel for sending control messages to a driver local archive.
            /// </summary>
            public const string LOCAL_CONTROL_CHANNEL_PROP_NAME = "aeron.archive.local.control.channel";

            /// <summary>
            /// Channel for sending control messages to a driver local archive. Default to IPC.
            /// </summary>
            public static readonly string LOCAL_CONTROL_CHANNEL_DEFAULT = Aeron.Aeron.Context.IPC_CHANNEL;

            /// <summary>
            /// Stream id within a channel for sending control messages to a driver local archive.
            /// </summary>
            public const string LOCAL_CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.local.control.stream.id";

            /// <summary>
            /// Stream id within a channel for sending control messages to a driver local archive.
            /// </summary>
            public const int LOCAL_CONTROL_STREAM_ID_DEFAULT = 11;

            /// <summary>
            /// Channel for receiving control response messages from an archive.
            /// </summary>
            public const string CONTROL_RESPONSE_CHANNEL_PROP_NAME = "aeron.archive.control.response.channel";

            /// <summary>
            /// Channel for receiving control response messages from an archive.
            /// </summary>
            public const string CONTROL_RESPONSE_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8020";

            /// <summary>
            /// Stream id within a channel for receiving control messages from an archive.
            /// </summary>
            public const string CONTROL_RESPONSE_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

            /// <summary>
            /// Stream id within a channel for receiving control messages from an archive.
            /// </summary>
            public const int CONTROL_RESPONSE_STREAM_ID_DEFAULT = 20;

            /// <summary>
            /// Channel for receiving progress events of recordings from an archive.
            /// </summary>
            public const string RECORDING_EVENTS_CHANNEL_PROP_NAME = "aeron.archive.recording.events.channel";

            /// <summary>
            /// Channel for receiving progress events of recordings from an archive.
            /// For production it is recommended that multicast or dynamic multi-destination-cast (MDC) is used to allow
            /// for dynamic subscribers.
            /// </summary>
            public const string RECORDING_EVENTS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8030";

            /// <summary>
            /// Stream id within a channel for receiving progress of recordings from an archive.
            /// </summary>
            public const string RECORDING_EVENTS_STREAM_ID_PROP_NAME = "aeron.archive.recording.events.stream.id";

            /// <summary>
            /// Stream id within a channel for receiving progress of recordings from an archive.
            /// </summary>
            public const int RECORDING_EVENTS_STREAM_ID_DEFAULT = 30;

            /// <summary>
            /// Sparse term buffer indicator for control streams.
            /// </summary>
            public const string CONTROL_TERM_BUFFER_SPARSE_PARAM_NAME = "aeron.archive.control.term.buffer.sparse";

            /// <summary>
            /// Overrides driver's sparse term buffer indicator for control streams.
            /// </summary>
            public const bool CONTROL_TERM_BUFFER_SPARSE_DEFAULT = true;

            /// <summary>
            /// Term length for control streams.
            /// </summary>
            public const string CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME = "aeron.archive.control.term.buffer.length";

            /// <summary>
            /// Low term length for control channel reflects expected low bandwidth usage.
            /// </summary>
            public const int CONTROL_TERM_BUFFER_LENGTH_DEFAULT = 64 * 1024;

            /// <summary>
            /// MTU length for control streams.
            /// </summary>
            public const string CONTROL_MTU_LENGTH_PARAM_NAME = "aeron.archive.control.mtu.length";

            /// <summary>
            ///  MTU to reflect default for the control streams.
            /// </summary>
            public const int CONTROL_MTU_LENGTH_DEFAULT = 4 * 1024;

            /// <summary>
            /// The timeout in nanoseconds to wait for a message.
            /// </summary>
            /// <returns> timeout in nanoseconds to wait for a message. </returns>
            /// <seealso cref="MESSAGE_TIMEOUT_PROP_NAME"></seealso>
            public static long MessageTimeoutNs()
            {
                return Config.GetDurationInNanos(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
            }

            /// <summary>
            /// Should term buffer files be sparse for control request and response streams.
            /// </summary>
            /// <returns> true if term buffer files should be sparse for control request and response streams. </returns>
            /// <seealso cref="CONTROL_TERM_BUFFER_SPARSE_PARAM_NAME"/>
            public static bool ControlTermBufferSparse()
            {
                string propValue = Config.GetProperty(CONTROL_TERM_BUFFER_SPARSE_PARAM_NAME);
                return null != propValue ? "true".Equals(propValue) : CONTROL_TERM_BUFFER_SPARSE_DEFAULT;
            }

            /// <summary>
            /// Term buffer length to be used for control request and response streams.
            /// </summary>
            /// <returns> term buffer length to be used for control request and response streams. </returns>
            /// <seealso cref="CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME"></seealso>
            public static int ControlTermBufferLength()
            {
                return Config.GetSizeAsInt(CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME, CONTROL_TERM_BUFFER_LENGTH_DEFAULT);
            }

            /// <summary>
            /// MTU length to be used for control request and response streams.
            /// </summary>
            /// <returns> MTU length to be used for control request and response streams. </returns>
            /// <seealso cref="CONTROL_MTU_LENGTH_PARAM_NAME"></seealso>
            public static int ControlMtuLength()
            {
                return Config.GetSizeAsInt(CONTROL_MTU_LENGTH_PARAM_NAME, CONTROL_MTU_LENGTH_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ControlChannel()
            {
                return Config.GetProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="CONTROL_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CONTROL_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ControlStreamId()
            {
                return Config.GetInteger(CONTROL_STREAM_ID_PROP_NAME, CONTROL_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="LOCAL_CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="LOCAL_CONTROL_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="LOCAL_CONTROL_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string LocalControlChannel()
            {
                return Config.GetProperty(LOCAL_CONTROL_CHANNEL_PROP_NAME, LOCAL_CONTROL_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="LOCAL_CONTROL_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="LOCAL_CONTROL_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="LOCAL_CONTROL_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="LOCAL_CONTROL_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int LocalControlStreamId()
            {
                return Config.GetInteger(LOCAL_CONTROL_STREAM_ID_PROP_NAME, LOCAL_CONTROL_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="CONTROL_RESPONSE_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_RESPONSE_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CONTROL_RESPONSE_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_RESPONSE_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ControlResponseChannel()
            {
                return Config.GetProperty(CONTROL_RESPONSE_CHANNEL_PROP_NAME, CONTROL_RESPONSE_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="CONTROL_RESPONSE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_RESPONSE_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="CONTROL_RESPONSE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="CONTROL_RESPONSE_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ControlResponseStreamId()
            {
                return Config.GetInteger(CONTROL_RESPONSE_STREAM_ID_PROP_NAME, CONTROL_RESPONSE_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="RECORDING_EVENTS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="RECORDING_EVENTS_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="RECORDING_EVENTS_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="RECORDING_EVENTS_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string RecordingEventsChannel()
            {
                return Config.GetProperty(RECORDING_EVENTS_CHANNEL_PROP_NAME, RECORDING_EVENTS_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="RECORDING_EVENTS_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="RECORDING_EVENTS_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="RECORDING_EVENTS_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="RECORDING_EVENTS_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int RecordingEventsStreamId()
            {
                return Config.GetInteger(RECORDING_EVENTS_STREAM_ID_PROP_NAME, RECORDING_EVENTS_STREAM_ID_DEFAULT);
            }
        }

        /// <summary>
        /// Specialised configuration options for communicating with an Aeron Archive.
        ///
        /// The context will be owned by <see cref="AeronArchive"/> after a successful
        /// <see cref="AeronArchive.Connect(Context)"/> and closed via <see cref="AeronArchive.Dispose"/>
        /// </summary>
        public class Context
        {
            internal long messageTimeoutNs = Configuration.MessageTimeoutNs();
            internal string recordingEventsChannel = Configuration.RecordingEventsChannel();
            internal int recordingEventsStreamId = Configuration.RecordingEventsStreamId();
            internal string controlRequestChannel = Configuration.ControlChannel();
            internal int controlRequestStreamId = Configuration.ControlStreamId();
            internal string controlResponseChannel = Configuration.ControlResponseChannel();
            internal int controlResponseStreamId = Configuration.ControlResponseStreamId();
            internal bool controlTermBufferSparse = Configuration.ControlTermBufferSparse();
            internal int controlTermBufferLength = Configuration.ControlTermBufferLength();
            internal int controlMtuLength = Configuration.ControlMtuLength();

            internal IIdleStrategy idleStrategy;
            internal ILock _lock;
            internal string aeronDirectoryName = Aeron.Aeron.Context.GetAeronDirectoryName();
            internal Aeron.Aeron aeron;
            private ErrorHandler errorHandler;
            internal bool ownsAeronClient = false;

            public Context Clone()
            {
                return (Context) MemberwiseClone();
            }

            /// <summary>
            /// Conclude configuration by setting up defaults when specifics are not provided.
            /// </summary>
            public void Conclude()
            {
                if (null == aeron)
                {
                    aeron = Aeron.Aeron.Connect(new Aeron.Aeron.Context().AeronDirectoryName(aeronDirectoryName));

                    ownsAeronClient = true;
                }

                if (null == idleStrategy)
                {
                    idleStrategy = new BackoffIdleStrategy(
                        Agrona.Concurrent.Configuration.IDLE_MAX_SPINS,
                        Agrona.Concurrent.Configuration.IDLE_MAX_YIELDS,
                        Agrona.Concurrent.Configuration.IDLE_MIN_PARK_MS,
                        Agrona.Concurrent.Configuration.IDLE_MAX_PARK_MS);
                }

                if (null == _lock)
                {
                    _lock = new ReentrantLock();
                }

                ChannelUri uri = ChannelUri.Parse(controlRequestChannel);
                uri.Put(Aeron.Aeron.Context.TERM_LENGTH_PARAM_NAME, Convert.ToString(controlTermBufferLength));
                uri.Put(Aeron.Aeron.Context.MTU_LENGTH_PARAM_NAME, Convert.ToString(controlMtuLength));
                uri.Put(Aeron.Aeron.Context.SPARSE_PARAM_NAME, Convert.ToString(controlTermBufferSparse));
                controlRequestChannel = uri.ToString();
            }

            /// <summary>
            /// Set the message timeout in nanoseconds to wait for sending or receiving a message.
            /// </summary>
            /// <param name="messageTimeoutNs"> to wait for sending or receiving a message. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.MESSAGE_TIMEOUT_PROP_NAME"/>
            public Context MessageTimeoutNs(long messageTimeoutNs)
            {
                this.messageTimeoutNs = messageTimeoutNs;
                return this;
            }

            /// <summary>
            /// The message timeout in nanoseconds to wait for sending or receiving a message.
            /// </summary>
            /// <returns> the message timeout in nanoseconds to wait for sending or receiving a message. </returns>
            /// <seealso cref="Configuration.MESSAGE_TIMEOUT_PROP_NAME"/>
            public long MessageTimeoutNs()
            {
                return messageTimeoutNs;
            }

            /// <summary>
            /// Get the channel URI on which the recording events publication will publish.
            /// </summary>
            /// <returns> the channel URI on which the recording events publication will publish. </returns>
            public string RecordingEventsChannel()
            {
                return recordingEventsChannel;
            }

            /// <summary>
            /// Set the channel URI on which the recording events publication will publish.
            /// <para>
            /// To support dynamic subscribers then this can be set to multicast or MDC (Multi-Destination-Cast) if
            /// multicast cannot be supported for on the available the network infrastructure.
            /// 
            /// </para>
            /// </summary>
            /// <param name="recordingEventsChannel"> channel URI on which the recording events publication will publish. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Adaptive.Aeron.Aeron.Context.MDC_CONTROL_PARAM_NAME"/>
            public Context RecordingEventsChannel(string recordingEventsChannel)
            {
                this.recordingEventsChannel = recordingEventsChannel;
                return this;
            }

            /// <summary>
            /// Get the stream id on which the recording events publication will publish.
            /// </summary>
            /// <returns> the stream id on which the recording events publication will publish. </returns>
            public int RecordingEventsStreamId()
            {
                return recordingEventsStreamId;
            }

            /// <summary>
            /// Set the stream id on which the recording events publication will publish.
            /// </summary>
            /// <param name="recordingEventsStreamId"> stream id on which the recording events publication will publish. </param>
            /// <returns> this for a fluent API. </returns>
            public Context RecordingEventsStreamId(int recordingEventsStreamId)
            {
                this.recordingEventsStreamId = recordingEventsStreamId;
                return this;
            }

            /// <summary>
            /// Set the channel parameter for the control request channel.
            /// </summary>
            /// <param name="channel"> parameter for the control request channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_CHANNEL_PROP_NAME"></seealso>
            public Context ControlRequestChannel(string channel)
            {
                controlRequestChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the control request channel.
            /// </summary>
            /// <returns> the channel parameter for the control request channel. </returns>
            /// <seealso cref="Configuration.CONTROL_CHANNEL_PROP_NAME"></seealso>
            public string ControlRequestChannel()
            {
                return controlRequestChannel;
            }

            /// <summary>
            /// Set the stream id for the control request channel.
            /// </summary>
            /// <param name="streamId"> for the control request channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.CONTROL_STREAM_ID_PROP_NAME"></seealso>
            public Context ControlRequestStreamId(int streamId)
            {
                controlRequestStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the control request channel.
            /// </summary>
            /// <returns> the stream id for the control request channel. </returns>
            /// <seealso cref="Configuration.CONTROL_STREAM_ID_PROP_NAME"></seealso>
            public int ControlRequestStreamId()
            {
                return controlRequestStreamId;
            }

            /// <summary>
            /// Set the channel parameter for the control response channel.
            /// </summary>
            /// <param name="channel"> parameter for the control response channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_RESPONSE_CHANNEL_PROP_NAME"></seealso>s
            public Context ControlResponseChannel(string channel)
            {
                controlResponseChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the control response channel.
            /// </summary>
            /// <returns> the channel parameter for the control response channel. </returns>
            /// <seealso cref="Configuration.CONTROL_RESPONSE_CHANNEL_PROP_NAME"></seealso>
            public string ControlResponseChannel()
            {
                return controlResponseChannel;
            }

            /// <summary>
            /// Set the stream id for the control response channel.
            /// </summary>
            /// <param name="streamId"> for the control response channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref="Configuration.CONTROL_RESPONSE_STREAM_ID_PROP_NAME"></seealso>
            public Context ControlResponseStreamId(int streamId)
            {
                controlResponseStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the control response channel.
            /// </summary>
            /// <returns> the stream id for the control response channel. </returns>
            /// <seealso cref="Configuration.CONTROL_RESPONSE_STREAM_ID_PROP_NAME"></seealso>
            public int ControlResponseStreamId()
            {
                return controlResponseStreamId;
            }

            /// <summary>
            /// Should the control streams use sparse file term buffers.
            /// </summary>
            /// <param name="controlTermBufferSparse"> for the control stream. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_SPARSE_PARAM_NAME"></seealso>
            public Context ControlTermBufferSparse(bool controlTermBufferSparse)
            {
                this.controlTermBufferSparse = controlTermBufferSparse;
                return this;
            }

            /// <summary>
            /// Should the control streams use sparse file term buffers.
            /// </summary>
            /// <returns> true if the control stream should use sparse file term buffers. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_SPARSE_PARAM_NAME"></seealso>
            public bool ControlTermBufferSparse()
            {
                return controlTermBufferSparse;
            }

            /// <summary>
            /// Set the term buffer length for the control streams.
            /// </summary>
            /// <param name="controlTermBufferLength"> for the control stream. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME"></seealso>
            public Context ControlTermBufferLength(int controlTermBufferLength)
            {
                this.controlTermBufferLength = controlTermBufferLength;
                return this;
            }

            /// <summary>
            /// Get the term buffer length for the control streams.
            /// </summary>
            /// <returns> the term buffer length for the control streams. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME"></seealso>
            public int ControlTermBufferLength()
            {
                return controlTermBufferLength;
            }

            /// <summary>
            /// Set the MTU length for the control streams.
            /// </summary>
            /// <param name="controlMtuLength"> for the control streams. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_MTU_LENGTH_PARAM_NAME"></seealso>
            public Context ControlMtuLength(int controlMtuLength)
            {
                this.controlMtuLength = controlMtuLength;
                return this;
            }

            /// <summary>
            /// Get the MTU length for the control steams.
            /// </summary>
            /// <returns> the MTU length for the control steams. </returns>
            /// <seealso cref="Configuration.CONTROL_MTU_LENGTH_PARAM_NAME"></seealso>
            public int ControlMtuLength()
            {
                return controlMtuLength;
            }

            /// <summary>
            /// Set the <seealso cref="IIdleStrategy"/> used when waiting for responses.
            /// </summary>
            /// <param name="idleStrategy"> used when waiting for responses. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IdleStrategy(IIdleStrategy idleStrategy)
            {
                this.idleStrategy = idleStrategy;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="IIdleStrategy"/> used when waiting for responses.
            /// </summary>
            /// <returns> the <seealso cref="IIdleStrategy"/> used when waiting for responses. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return idleStrategy;
            }

            /// <summary>
            /// Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <param name="aeronDirectoryName"> the top level Aeron directory. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AeronDirectoryName(string aeronDirectoryName)
            {
                this.aeronDirectoryName = aeronDirectoryName;
                return this;
            }

            /// <summary>
            /// Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <returns> The top level Aeron directory. </returns>
            public string AeronDirectoryName()
            {
                return aeronDirectoryName;
            }

            /// <summary>
            /// <seealso cref="Adaptive.Aeron.Aeron"/> client for communicating with the local Media Driver.
            /// <para>
            /// This client will be closed when the <seealso cref="AeronArchive.Dispose()"/> or <seealso cref="Dispose()"/> methods are called if
            /// <seealso cref="OwnsAeronClient()"/> is true.
            /// 
            /// </para>
            /// </summary>
            /// <param name="aeron"> client for communicating with the local Media Driver. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Adaptive.Aeron.Aeron.Connect()"></seealso>
            public Context AeronClient(Aeron.Aeron aeron)
            {
                this.aeron = aeron;
                return this;
            }

            /// <summary>
            /// <seealso cref="Adaptive.Aeron.Aeron"/> client for communicating with the local Media Driver.
            /// <para>
            /// If not provided then a default will be established during <seealso cref="Conclude()"/> by calling
            /// <seealso cref="Adaptive.Aeron.Aeron.Connect()"/>.
            /// 
            /// </para>
            /// </summary>
            /// <returns> client for communicating with the local Media Driver. </returns>
            public Aeron.Aeron AeronClient()
            {
                return aeron;
            }

            /// <summary>
            /// Does this context own the <seealso cref="AeronClient()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="AeronClient()"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                this.ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="AeronClient()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="AeronClient()"/> client and this takes responsibility for closing it? </returns>
            public bool OwnsAeronClient()
            {
                return ownsAeronClient;
            }

            /// <summary>
            /// The <seealso cref="Lock()"/> that is used to provide mutual exclusion in the <seealso cref="AeronArchive"/> client.
            /// <para>
            /// If the <seealso cref="AeronArchive"/> is used from only a single thread then the lock can be set to
            /// <seealso cref="NoOpLock"/> to elide the lock overhead.
            /// 
            /// </para>
            /// </summary>
            /// <param name="lock"> that is used to provide mutual exclusion in the <seealso cref="AeronArchive"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context Lock(ILock @lock)
            {
                _lock = @lock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Lock()"/> that is used to provide mutual exclusion in the <seealso cref="AeronArchive"/> client.
            /// </summary>
            /// <returns> the <seealso cref="Lock()"/> that is used to provide mutual exclusion in the <seealso cref="AeronArchive"/> client. </returns>
            public ILock Lock()
            {
                return _lock;
            }

            /// <summary>
            /// Handle errors returned asynchronously from the archive for a control session.
            /// </summary>
            /// <param name="errorHandler"> method to handle objects of type Throwable. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                this.errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the error handler that will be called for asynchronous errors.
            /// </summary>
            /// <returns> the error handler that will be called for asynchronous errors. </returns>
            public ErrorHandler ErrorHandler()
            {
                return errorHandler;
            }

            /// <summary>
            /// Close the context and free applicable resources.
            /// <para>
            /// If <seealso cref="OwnsAeronClient()"/> is true then the <seealso cref="AeronClient()"/> client will be closed.
            /// </para>
            /// </summary>
            public void Dispose()
            {
                if (ownsAeronClient)
                {
                    aeron?.Dispose();
                }
            }
        }

        /// <summary>
        /// Allows for the async establishment of a archive session.
        /// </summary>
        public class AsyncConnect : IDisposable
        {
            private readonly Context ctx;
            private readonly ControlResponsePoller controlResponsePoller;
            private readonly ArchiveProxy archiveProxy;
            private long connectCorrelationId = Aeron.Aeron.NULL_VALUE;
            private int step = 0;

            internal AsyncConnect(Context ctx, ControlResponsePoller controlResponsePoller, ArchiveProxy archiveProxy)
            {
                this.ctx = ctx;
                this.controlResponsePoller = controlResponsePoller;
                this.archiveProxy = archiveProxy;
            }

            /// <summary>
            /// Close any allocated resources.
            /// </summary>
            public void Dispose()
            {
                controlResponsePoller.Subscription()?.Dispose();
                archiveProxy.Pub()?.Dispose();
                ctx.Dispose();
            }

            /// <summary>
            /// Poll for a complete connection.
            /// </summary>
            /// <exception cref="InvalidOperationException"></exception>
            /// <returns> a new <seealso cref="AeronArchive"/> if successfully connected otherwise null. </returns>
            public AeronArchive Poll()
            {
                if (0 == step)
                {
                    if (!archiveProxy.Pub().IsConnected)
                    {
                        return null;
                    }

                    step = 1;
                }

                if (1 == step)
                {
                    connectCorrelationId = ctx.aeron.NextCorrelationId();

                    step = 2;
                }

                if (2 == step)
                {
                    if (!archiveProxy.TryConnect(ctx.ControlResponseChannel(), ctx.ControlResponseStreamId(),
                        connectCorrelationId))
                    {
                        return null;
                    }

                    step = 3;
                }

                if (3 == step)
                {
                    if (!controlResponsePoller.Subscription().IsConnected)
                    {
                        return null;
                    }

                    step = 4;
                }

                controlResponsePoller.Poll();
                if (controlResponsePoller.IsPollComplete() &&
                    controlResponsePoller.CorrelationId() == connectCorrelationId &&
                    controlResponsePoller.TemplateId() == ControlResponseDecoder.TEMPLATE_ID)
                {
                    ControlResponseCode code = controlResponsePoller.Code();
                    if (code != ControlResponseCode.OK)
                    {
                        if (code == ControlResponseCode.ERROR)
                        {
                            throw new ArchiveException("error: " + controlResponsePoller.ErrorMessage(),
                                (int) controlResponsePoller.RelevantId());
                        }

                        throw new ArchiveException("unexpected response: code=" + code);
                    }

                    long controlSessionId = controlResponsePoller.ControlSessionId();
                    Subscription subscription = controlResponsePoller.Subscription();
                    return new AeronArchive(ctx, controlResponsePoller, archiveProxy,
                        new RecordingDescriptorPoller(subscription, ctx.ErrorHandler(), controlSessionId, FRAGMENT_LIMIT),
                        controlSessionId);
                }

                return null;
            }
        }
    }
}