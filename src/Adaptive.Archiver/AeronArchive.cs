﻿using System;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.Security;
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

        /// <summary>
        /// Indicates the client is no longer connected to an archive.
        /// </summary>
        public const string NOT_CONNECTED_MSG = "not connected";

        private const int FRAGMENT_LIMIT = 10;

        private bool isClosed = false;
        private bool isInCallback = false;
        private long lastCorrelationId = Aeron.Aeron.NULL_VALUE;
        private readonly long controlSessionId;
        private readonly long messageTimeoutNs;
        private readonly Context context;
        private readonly Aeron.Aeron aeron;
        private readonly ArchiveProxy archiveProxy;
        private readonly IIdleStrategy idleStrategy;
        private readonly ControlResponsePoller controlResponsePoller;
        private readonly ILock _lock;
        private readonly INanoClock nanoClock;
        private readonly AgentInvoker aeronClientInvoker;
        private RecordingDescriptorPoller recordingDescriptorPoller;
        private RecordingSubscriptionDescriptorPoller recordingSubscriptionDescriptorPoller;

        internal AeronArchive(
            Context context,
            ControlResponsePoller controlResponsePoller,
            ArchiveProxy archiveProxy,
            long controlSessionId)
        {
            this.context = context;
            aeron = context.AeronClient();
            aeronClientInvoker = aeron.ConductorAgentInvoker;
            idleStrategy = context.IdleStrategy();
            messageTimeoutNs = context.MessageTimeoutNs();
            _lock = context.Lock();
            nanoClock = aeron.Ctx.NanoClock();
            this.controlResponsePoller = controlResponsePoller;
            this.archiveProxy = archiveProxy;
            this.controlSessionId = controlSessionId;
        }

        /// <summary>
        /// Position of the recorded stream at the base of a segment file. If a recording starts within a term
        /// then the base position can be before the recording started.
        /// </summary>
        /// <param name="startPosition">     of the stream. </param>
        /// <param name="position">          of the stream to calculate the segment base position from. </param>
        /// <param name="termBufferLength">  of the stream. </param>
        /// <param name="segmentFileLength"> which is a multiple of term length. </param>
        /// <returns> the position of the recorded stream at the beginning of a segment file. </returns>
        public static long SegmentFileBasePosition(long startPosition, long position, int termBufferLength,
            int segmentFileLength)
        {
            long startTermBasePosition = startPosition - (startPosition & (termBufferLength - 1));
            long lengthFromBasePosition = position - startTermBasePosition;
            long segments = (lengthFromBasePosition - (lengthFromBasePosition & (segmentFileLength - 1)));

            return startTermBasePosition + segments;
        }

        /// <summary>
        /// Notify the archive that this control session is closed, so it can promptly release resources then close the
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
                    ErrorHandler errorHandler = context.ErrorHandler();

                    if (archiveProxy.Pub().IsConnected)
                    {
                        CloseHelper.Dispose(errorHandler, () => archiveProxy.CloseSession(controlSessionId));
                    }

                    if (!context.OwnsAeronClient())
                    {
                        CloseHelper.Dispose(errorHandler, archiveProxy.Pub());
                        CloseHelper.Dispose(errorHandler, controlResponsePoller.Subscription());
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
            return Connect(new Context());
        }

        /// <summary>
        /// Connect to an Aeron archive by providing a <seealso cref="Adaptive.Aeron.Aeron.Context"/>. This will create a control session.
        /// <para>
        /// Before connecting <seealso cref="Adaptive.Aeron.Aeron.Context.Conclude()"/> will be called.
        /// If an exception occurs then <seealso cref="Adaptive.Aeron.Aeron.Context.Dispose()"/> will be called.
        /// 
        /// </para>
        /// </summary>
        /// <param name="ctx"> for connection configuration. </param>
        /// <returns> the newly created Aeron Archive client. </returns>
        public static AeronArchive Connect(Context ctx)
        {
            Subscription subscription = null;
            Publication publication = null;
            AsyncConnect asyncConnect = null;

            try
            {
                ctx.Conclude();

                var aeron = ctx.AeronClient();
                subscription = aeron.AddSubscription(ctx.ControlResponseChannel(), ctx.ControlResponseStreamId());
                publication = aeron.AddExclusivePublication(ctx.ControlRequestChannel(), ctx.ControlRequestStreamId());
                var controlResponsePoller = new ControlResponsePoller(subscription);

                var archiveProxy = new ArchiveProxy(
                    publication,
                    ctx.IdleStrategy(),
                    aeron.Ctx.NanoClock(),
                    ctx.MessageTimeoutNs(),
                    ArchiveProxy.DEFAULT_RETRY_ATTEMPTS,
                    ctx.CredentialsSupplier());

                asyncConnect = new AsyncConnect(ctx, controlResponsePoller, archiveProxy);
                var idleStrategy = ctx.IdleStrategy();
                var aeronClientInvoker = aeron.ConductorAgentInvoker;

                AeronArchive aeronArchive;

                while (null == (aeronArchive = asyncConnect.Poll()))
                {
                    if (null != aeronClientInvoker)
                    {
                        aeronClientInvoker.Invoke();
                    }

                    idleStrategy.Idle();
                }

                return aeronArchive;
            }
            catch (ConcurrentConcludeException)
            {
                throw;
            }
            catch (Exception)
            {
                if (!ctx.OwnsAeronClient())
                {
                    CloseHelper.QuietDispose(subscription);
                    CloseHelper.QuietDispose(publication);
                }

                CloseHelper.QuietDispose(asyncConnect);
                CloseHelper.QuietDispose(ctx.Dispose);

                throw;
            }
        }

        /// <summary>
        /// Begin an attempt at creating a connection which can be completed by calling <seealso cref="AsyncConnect.Poll()"/> until
        /// it returns the client, before complete it will return null.
        /// </summary>
        /// <returns> the <seealso cref="AsyncConnect"/> that can be polled for completion. </returns>
        public static AsyncConnect ConnectAsync()
        {
            return ConnectAsync(new Context());
        }

        /// <summary>
        /// Begin an attempt at creating a connection which can be completed by calling <seealso cref="AsyncConnect.Poll()"/> until
        /// it returns the client, before complete it will return null.
        /// </summary>
        /// <param name="ctx"> for the archive connection. </param>
        /// <returns> the <seealso cref="AsyncConnect"/> that can be polled for completion. </returns>
        public static AsyncConnect ConnectAsync(Context ctx)
        {
            try
            {
                ctx.Conclude();

                return new AsyncConnect(ctx);
            }
            catch (ConcurrentConcludeException)
            {
                throw;
            }
            catch (Exception)
            {
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
        /// The last correlation id used for sending a request to the archive via method on this class.
        /// </summary>
        /// <returns> last correlation id used for sending a request to the archive. </returns>
        public long LastCorrelationId()
        {
            return lastCorrelationId;
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
            if (null == recordingDescriptorPoller)
            {
                recordingDescriptorPoller = new RecordingDescriptorPoller(
                    controlResponsePoller.Subscription(),
                    context.ErrorHandler(),
                    context.RecordingSignalConsumer(),
                    controlSessionId,
                    FRAGMENT_LIMIT);
            }

            return recordingDescriptorPoller;
        }

        /// <summary>
        /// The <seealso cref="RecordingSubscriptionDescriptorPoller"/> for polling subscription descriptors on the control channel.
        /// </summary>
        /// <returns> the <seealso cref="RecordingSubscriptionDescriptorPoller"/> for polling subscription descriptors on the control
        /// channel. </returns>
        public RecordingSubscriptionDescriptorPoller RecordingSubscriptionDescriptorPoller()
        {
            if (null == recordingSubscriptionDescriptorPoller)
            {
                recordingSubscriptionDescriptorPoller = new RecordingSubscriptionDescriptorPoller(
                    controlResponsePoller.Subscription(),
                    context.ErrorHandler(),
                    context.RecordingSignalConsumer(),
                    controlSessionId,
                    FRAGMENT_LIMIT);
            }

            return recordingSubscriptionDescriptorPoller;
        }

        /// <summary>
        /// Poll the response stream once for an error. If another message is present then it will be skipped over
        /// so only call when not expecting another response. If not connected then return <see cref="NOT_CONNECTED_MSG"/>.
        /// </summary>
        /// <returns> the error String otherwise null if no error is found. </returns>
        public string PollForErrorResponse()
        {
            _lock.Lock();
            try
            {
                EnsureOpen();

                if (!controlResponsePoller.Subscription().IsConnected)
                {
                    return NOT_CONNECTED_MSG;
                }

                if (controlResponsePoller.Poll() != 0 && controlResponsePoller.PollComplete)
                {
                    if (controlResponsePoller.ControlSessionId() == controlSessionId)
                    {
                        if (controlResponsePoller.Code() == ControlResponseCode.ERROR)
                        {
                            return controlResponsePoller.ErrorMessage();
                        }
                        else if (controlResponsePoller.TemplateId() == RecordingSignalEventDecoder.TEMPLATE_ID)
                        {
                            DispatchRecordingSignal();
                        }
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
        /// Check if an error has been returned for the control session, or if it is no longer connected, and then throw
        /// a <seealso cref="ArchiveException"/> if <seealso cref="Context.ErrorHandler(ErrorHandler)"/> is not set.
        /// <para>
        /// To check for an error response without raising an exception then try <seealso cref="PollForErrorResponse()"/>.
        ///    
        /// </para>
        /// </summary>
        /// <seealso cref="PollForErrorResponse()"></seealso>
        public void CheckForErrorResponse()
        {
            _lock.Lock();
            try
            {
                EnsureOpen();

                if (!controlResponsePoller.Subscription().IsConnected)
                {
                    if (null != context.ErrorHandler())
                    {
                        context.ErrorHandler()(new ArchiveException(NOT_CONNECTED_MSG));
                    }
                    else
                    {
                        throw new ArchiveException(NOT_CONNECTED_MSG);
                    }
                }
                else if (controlResponsePoller.Poll() != 0 && controlResponsePoller.PollComplete)
                {
                    if (controlResponsePoller.ControlSessionId() == controlSessionId)
                    {
                        if (controlResponsePoller.Code() == ControlResponseCode.ERROR)
                        {
                            ArchiveException ex = new ArchiveException(controlResponsePoller.ErrorMessage(),
                                (int)controlResponsePoller.RelevantId(), controlResponsePoller.CorrelationId());

                            if (null != context.ErrorHandler())
                            {
                                context.ErrorHandler()(ex);
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                        else if (controlResponsePoller.TemplateId() == RecordingSignalEventDecoder.TEMPLATE_ID)
                        {
                            DispatchRecordingSignal();
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
        /// Poll for <seealso cref="RecordingSignal"/>s for this session which will be dispatched to
        /// <seealso cref="Context.RecordingSignalConsumer()"/>.
        /// </summary>
        /// <returns> positive value if signals dispatched otherwise 0. </returns>
        public int PollForRecordingSignals()
        {
            _lock.Lock();
            try
            {
                EnsureOpen();

                if (controlResponsePoller.Poll() != 0 && controlResponsePoller.PollComplete)
                {
                    if (controlResponsePoller.ControlSessionId() == controlSessionId)
                    {
                        if (controlResponsePoller.Code() == ControlResponseCode.ERROR)
                        {
                            ArchiveException ex = new ArchiveException(controlResponsePoller.ErrorMessage(),
                                (int)controlResponsePoller.RelevantId(), controlResponsePoller.CorrelationId());

                            if (null != context.ErrorHandler())
                            {
                                context.ErrorHandler()(ex);
                            }
                            else
                            {
                                throw ex;
                            }
                        }
                        else if (controlResponsePoller.TemplateId() == RecordingSignalEventDecoder.TEMPLATE_ID)
                        {
                            DispatchRecordingSignal();
                            return 1;
                        }
                    }
                }

                return 0;
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Add a <seealso cref="Publication"/> and set it up to be recorded. If this is not the first,
        /// i.e. <seealso cref="Publication.IsOriginal"/> is true, then an <seealso cref="ArchiveException"/>
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
                EnsureNotReentrant();

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
        /// Add an <seealso cref="ExclusivePublication"/> and set it up to be recorded.
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
                EnsureNotReentrant();

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
        /// Channels that include sessionId parameters are considered different from channels without sessionIds. If a
        /// publication matches both a sessionId specific channel recording and a non-sessionId specific recording,
        /// it will be recorded twice.
        ///   
        /// </para>
        /// </summary>
        /// <param name="channel">        to be recorded. </param>
        /// <param name="streamId">       to be recorded. </param>
        /// <param name="sourceLocation"> of the publication to be recorded. </param>
        /// <returns> the subscriptionId, i.e. <seealso cref="Subscription.RegistrationId"/>, of the recording. This can be
        /// passed to <seealso cref="StopRecording(long)"/>. </returns>
        public long StartRecording(string channel, int streamId, SourceLocation sourceLocation)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StartRecording(channel, streamId, sourceLocation, lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send start recording request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Start recording a channel and stream pairing.
        /// <para>
        /// Channels that include sessionId parameters are considered different fom channels without sessionIds. If a
        /// publication matches both a sessionId specific channel recording and a non-sessionId specific recording,
        /// it will be recorded twice.
        ///    
        /// </para>
        /// </summary>
        /// <param name="channel">        to be recorded. </param>
        /// <param name="streamId">       to be recorded. </param>
        /// <param name="sourceLocation"> of the publication to be recorded. </param>
        /// <param name="autoStop">       if the recording should be automatically stopped when complete. </param>
        /// <returns> the subscriptionId, i.e. <seealso cref="Subscription.RegistrationId"/>, of the recording. This can be
        /// passed to <seealso cref="StopRecording(long)"/>. However, if is autoStop is true then no need to stop the recording
        /// unless you want to abort early. </returns>
        public long StartRecording(string channel, int streamId, SourceLocation sourceLocation, bool autoStop)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StartRecording(channel, streamId, sourceLocation, autoStop, lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send start recording request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Extend an existing, non-active recording of a channel and stream pairing.
        /// <para>
        /// The channel must be configured for the initial position from which it will be extended. This can be done
        /// with <seealso cref="ChannelUriStringBuilder.InitialPosition(long, int, int)"/>. The details required to initialise can
        /// be found by calling <seealso cref="ListRecording(long, IRecordingDescriptorConsumer)"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="recordingId">    of the existing recording. </param>
        /// <param name="channel">        to be recorded. </param>
        /// <param name="streamId">       to be recorded. </param>
        /// <param name="sourceLocation"> of the publication to be recorded. </param>
        /// <returns> the subscriptionId, i.e. <seealso cref="Subscription.RegistrationId"/>, of the recording. This can be
        /// passed to <seealso cref="StopRecording(long)"/>. </returns>
        public long ExtendRecording(long recordingId, string channel, int streamId, SourceLocation sourceLocation)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.ExtendRecording(channel, streamId, sourceLocation, recordingId, lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send extend recording request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Extend an existing, non-active recording of a channel and stream pairing.
        /// <para>
        /// The channel must be configured for the initial position from which it will be extended. This can be done
        /// with <seealso cref="ChannelUriStringBuilder.InitialPosition(long, int, int)"/>. The details required to initialise can
        /// be found by calling <seealso cref="ListRecording(long, IRecordingDescriptorConsumer)"/>.
        ///    
        /// </para>
        /// </summary>
        /// <param name="recordingId">    of the existing recording. </param>
        /// <param name="channel">        to be recorded. </param>
        /// <param name="streamId">       to be recorded. </param>
        /// <param name="sourceLocation"> of the publication to be recorded. </param>
        /// <param name="autoStop">       if the recording should be automatically stopped when complete. </param>
        /// <returns> the subscriptionId, i.e. <seealso cref="Subscription.RegistrationId()"/>, of the recording. This can be
        /// passed to <seealso cref="StopRecording(long)"/>. However, if is autoStop is true then no need to stop the recording
        /// unless you want to abort early. </returns>
        public long ExtendRecording(long recordingId, string channel, int streamId, SourceLocation sourceLocation,
            bool autoStop)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.ExtendRecording(channel, streamId, sourceLocation, autoStop, recordingId,
                        lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send extend recording request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Stop recording for a channel and stream pairing.
        /// <para>
        /// Channels that include sessionId parameters are considered different from channels without sessionIds. Stopping
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecording(channel, streamId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                PollForResponseAllowingError(lastCorrelationId, ArchiveException.UNKNOWN_SUBSCRIPTION);
            }
            finally
            {
                _lock.Unlock();
            }
        }


        /// <summary>
        /// Try to stop a recording for a channel and stream pairing.
        /// <para>
        /// Channels that include sessionId parameters are considered different than channels without sessionIds. Stopping
        /// a recording on a channel without a sessionId parameter will not stop the recording of any sessionId specific
        /// recordings that use the same channel and streamId.
        ///    
        /// </para>
        /// </summary>
        /// <param name="channel">  to stop recording for. </param>
        /// <param name="streamId"> to stop recording for. </param>
        /// <returns> true if the recording was stopped or false if the subscription is not currently active. </returns>
        public bool TryStopRecording(string channel, int streamId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecording(channel, streamId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                return PollForResponseAllowingError(lastCorrelationId, ArchiveException.UNKNOWN_SUBSCRIPTION);
            }
            finally
            {
                _lock.Unlock();
            }
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecording(subscriptionId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Try stop a recording for a subscriptionId that has been returned from
        /// <seealso cref="StartRecording(String, int, SourceLocation)"/> or
        /// <seealso cref="ExtendRecording(long, String, int, SourceLocation)"/>.
        /// </summary>
        /// <param name="subscriptionId"> is the <seealso cref="Subscription.RegistrationId()"/> for the recording in the archive. </param>
        /// <returns> true if the recording was stopped or false if the subscription is not currently active. </returns>
        public bool TryStopRecording(long subscriptionId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecording(subscriptionId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                return PollForResponseAllowingError(lastCorrelationId, ArchiveException.UNKNOWN_SUBSCRIPTION);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Try stop an active recording by its recording id.
        /// </summary>
        /// <param name="recordingId"> for which active recording should be stopped. </param>
        /// <returns> true if the recording was stopped or false if the recording is not currently active. </returns>
        public bool TryStopRecordingByIdentity(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopRecordingByIdentity(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                return PollForResponse(lastCorrelationId) != 0;
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
            StopRecording(ChannelUri.AddSessionId(publication.Channel, publication.SessionId), publication.StreamId);
        }


        /// <summary>
        /// Start a replay for a length in bytes of a recording from a position. If the position is <seealso cref="NULL_POSITION"/>
        /// then the stream will be replayed from the start.
        ///
        /// The lower 32-bits of the returned value contains the <see cref="Image.SessionId"/> of the received replay. All
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replay(recordingId, position, length, replayChannel, replayStreamId,
                        lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send replay request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Start a replay for a length in bytes of a recording from a position bounded by a position counter.
        /// If the position is <seealso cref="NULL_POSITION"/> then the stream will be replayed from the start.
        /// <para>
        /// The lower 32-bits of the returned value contains the <seealso cref="Image.SessionId"/> of the received replay. All
        /// 64-bits are required to uniquely identify the replay when calling <seealso cref="StopReplay(long)"/>. The lower 32-bits
        /// can be obtained by casting the {@code long} value to an {@code int}.
        ///     
        /// </para>
        /// </summary>
        /// <param name="recordingId">       to be replayed. </param>
        /// <param name="position">          from which the replay should begin or <seealso cref="NULL_POSITION"/> if from the start. </param>
        /// <param name="length">            of the stream to be replayed. Use <seealso cref="long.MaxValue"/> to follow a live recording or
        ///                          <seealso cref="NULL_LENGTH"/> to replay the whole stream of unknown length. </param>
        /// <param name="limitCounterId">    to use to bound replay. </param>
        /// <param name="replayChannel">     to which the replay should be sent. </param>
        /// <param name="replayStreamId">    to which the replay should be sent. </param>
        /// <returns> the id of the replay session which will be the same as the <seealso cref="Image.SessionId"/> of the received
        /// replay for correlation with the matching channel and stream id in the lower 32 bits. </returns>
        public long StartBoundedReplay(long recordingId, long position, long length, int limitCounterId,
            string replayChannel, int replayStreamId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.BoundedReplay(recordingId, position, length, limitCounterId, replayChannel,
                        replayStreamId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send bounded replay request");
                }

                return PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopReplay(replaySessionId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop recording request");
                }

                PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Stop all replay sessions for a given recording Id or all replays in general.
        /// </summary>
        /// <param name="recordingId"> to stop replay for or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> for all replays. </param>
        public void StopAllReplays(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopAllReplays(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop all replays request");
                }

                PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                ChannelUri replayChannelUri = ChannelUri.Parse(replayChannel);
                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replay(recordingId, position, length, replayChannel, replayStreamId,
                        lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send replay request");
                }

                int replaySessionId = (int)PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                ChannelUri replayChannelUri = ChannelUri.Parse(replayChannel);
                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replay(recordingId, position, length, replayChannel, replayStreamId,
                        lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send replay request");
                }

                int replaySessionId = (int)PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                isInCallback = true;
                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecordings(fromRecordingId, recordCount, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send list recordings request");
                }

                return PollForDescriptors(lastCorrelationId, recordCount, consumer);
            }
            finally
            {
                isInCallback = false;
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
                EnsureNotReentrant();

                isInCallback = true;
                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecordingsForUri(fromRecordingId, recordCount, channelFragment, streamId,
                        lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send list recordings request");
                }

                return PollForDescriptors(lastCorrelationId, recordCount, consumer);
            }
            finally
            {
                isInCallback = false;
                _lock.Unlock();
            }
        }

        /// <summary>
        /// List a recording descriptor for a single recording id.
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
                EnsureNotReentrant();

                isInCallback = true;
                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecording(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send list recording request");
                }

                return PollForDescriptors(lastCorrelationId, 1, consumer);
            }
            finally
            {
                isInCallback = false;
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Get the start position for a recording.
        /// </summary>
        /// <param name="recordingId"> of the recording for which the position is required. </param>
        /// <returns> the start position of a recording. </returns>
        /// <seealso cref="GetStopPosition(long)"></seealso>
        public long GetStartPosition(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.GetStartPosition(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send get start position request");
                }

                return PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.GetRecordingPosition(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send get recording position request");
                }

                return PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.GetStopPosition(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send get stop position request");
                }

                return PollForResponse(lastCorrelationId);
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
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.FindLastMatchingRecording(
                        minRecordingId, channelFragment, streamId, sessionId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send find last matching recording request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Truncate a stopped recording to a given position that is less than the stopped position. The provided position
        /// must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
        /// 
        /// If the truncate operation will result in deleting segments then this will occur asynchronously. Before extending
        /// a truncated recording which has segments being asynchronously being deleted then you should await completion
        /// on the <seealso cref="RecordingSignal.DELETE"/>.
        /// </summary>
        /// <param name="recordingId"> of the stopped recording to be truncated. </param>
        /// <param name="position">    to which the recording will be truncated. </param>
        public void TruncateRecording(long recordingId, long position)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.TruncateRecording(recordingId, position, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send truncate recording request");
                }

                PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Purge a stopped recording, i.e. mark recording as <seealso cref="RecordingState.INVALID"/>
        /// and delete the corresponding segment files. The space in the Catalog will be reclaimed upon compaction.
        /// </summary>
        /// <param name="recordingId"> of the stopped recording to be purged. </param>
        public void PurgeRecording(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.PurgeRecording(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send invalidate recording request");
                }

                PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// List active recording subscriptions in the archive. These are the result of requesting one of
        /// <seealso cref="StartRecording(String, int, SourceLocation)"/> or a
        /// <seealso cref="ExtendRecording(long, String, int, SourceLocation)"/>. The returned subscription id can be used for
        /// passing to <seealso cref="StopRecording(long)"/>.
        /// </summary>
        /// <param name="pseudoIndex">       in the active list at which to begin for paging. </param>
        /// <param name="subscriptionCount"> to get in a listing. </param>
        /// <param name="channelFragment">   to do a contains match on the stripped channel URI. Empty string is match all. </param>
        /// <param name="streamId">          to match on the subscription. </param>
        /// <param name="applyStreamId">     true if the stream id should be matched. </param>
        /// <param name="consumer">          for the matched subscription descriptors. </param>
        /// <returns> the count of matched subscriptions. </returns>
        public int ListRecordingSubscriptions(
            int pseudoIndex,
            int subscriptionCount,
            string channelFragment,
            int streamId,
            bool applyStreamId,
            IRecordingSubscriptionDescriptorConsumer consumer)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                isInCallback = true;
                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.ListRecordingSubscriptions(pseudoIndex, subscriptionCount, channelFragment, streamId,
                        applyStreamId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send list recording subscriptions request");
                }

                return PollForSubscriptionDescriptors(lastCorrelationId, subscriptionCount, consumer);
            }
            finally
            {
                isInCallback = false;
                _lock.Unlock();
            }
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
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>. Follow progress with <seealso cref="RecordingSignalAdapter"/>.
        ///   
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <returns> return the replication session id which can be passed later to <seealso cref="StopReplication(long)"/>. </returns>
        public long Replicate(long srcRecordingId, long dstRecordingId, int srcControlStreamId,
            string srcControlChannel, string liveDestination)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replicate(srcRecordingId, dstRecordingId, srcControlStreamId, srcControlChannel,
                        liveDestination, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send replicate request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
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
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>. Follow progress with <seealso cref="RecordingSignalAdapter"/>.
        /// </para>
        /// <para>
        /// Stop recording this stream when the position of the destination reaches the specified stop position.
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
        /// <returns> return the replication session id which can be passed later to <seealso cref="StopReplication(long)"/>. </returns>
        public long Replicate(long srcRecordingId, long dstRecordingId, long stopPosition, int srcControlStreamId,
            string srcControlChannel, string liveDestination, string replicationChannel)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.Replicate(
                        srcRecordingId,
                        dstRecordingId,
                        stopPosition,
                        srcControlStreamId,
                        srcControlChannel,
                        liveDestination,
                        replicationChannel,
                        lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send replicate request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
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
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>. Follow progress with <seealso cref="RecordingSignalAdapter"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="channelTagId">       used to tag the replication subscription. </param>
        /// <param name="subscriptionTagId">  used to tag the replication subscription. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <returns> return the replication session id which can be passed later to <seealso cref="StopReplication(long)"/>. </returns>
        public long TaggedReplicate(long srcRecordingId, long dstRecordingId, long channelTagId, long subscriptionTagId,
            int srcControlStreamId, string srcControlChannel, string liveDestination)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.TaggedReplicate(srcRecordingId, dstRecordingId, channelTagId, subscriptionTagId,
                        srcControlStreamId, srcControlChannel, liveDestination, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send tagged replicate request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
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
        /// or <seealso cref="AeronArchive.CheckForErrorResponse()"/>. Follow progress with <seealso cref="RecordingSignalAdapter"/>.
        ///     
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId">     recording id which must exist in the source archive. </param>
        /// <param name="dstRecordingId">     recording to extend in the destination, otherwise <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/>. </param>
        /// <param name="stopPosition">       position to stop the replication. <seealso cref="AeronArchive.NULL_POSITION"/> to stop at end
        ///                           of current recording. </param>
        /// <param name="channelTagId">       used to tag the replication subscription. </param>
        /// <param name="subscriptionTagId">  used to tag the replication subscription. </param>
        /// <param name="srcControlStreamId"> remote control stream id for the source archive to instruct the replay on. </param>
        /// <param name="srcControlChannel">  remote control channel for the source archive to instruct the replay on. </param>
        /// <param name="liveDestination">    destination for the live stream if merge is required. Empty or null for no merge. </param>
        /// <param name="replicationChannel"> channel over which the replication will occur. Empty or null for default channel. </param>
        /// <returns> return the replication session id which can be passed later to <seealso cref="StopReplication(long)"/>. </returns>
        public long TaggedReplicate(long srcRecordingId, long dstRecordingId, long stopPosition, long channelTagId,
            long subscriptionTagId, int srcControlStreamId, string srcControlChannel, string liveDestination,
            string replicationChannel)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.TaggedReplicate(
                        srcRecordingId,
                        dstRecordingId,
                        stopPosition,
                        channelTagId,
                        subscriptionTagId,
                        srcControlStreamId,
                        srcControlChannel,
                        liveDestination,
                        replicationChannel,
                        lastCorrelationId,
                        controlSessionId))
                {
                    throw new ArchiveException("failed to send tagged replicate request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }


        /// <summary>
        /// Stop a replication session by id returned from <seealso cref="Replicate(long, long, int, String, String)"/>.
        /// </summary>
        /// <param name="replicationId"> to stop replication for. </param>
        /// <seealso cref="Replicate(long, long, int, String, String)"></seealso>
        public void StopReplication(long replicationId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopReplication(replicationId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop replication request");
                }

                PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Attempt to stop a replication session by id returned from <seealso cref="Replicate(long, long, int, string, string)"/>.
        /// </summary>
        /// <param name="replicationId"> to stop replication for. </param>
        /// <returns> true if the replication was stopped, false if the replication is not active. </returns>
        /// <seealso cref="Replicate(long,long,int,string,string)"/>
        public bool TryStopReplication(long replicationId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.StopReplication(replicationId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send stop replication request");
                }

                return PollForResponseAllowingError(lastCorrelationId, ArchiveException.UNKNOWN_REPLICATION);
            }
            finally
            {
                _lock.Unlock();
            }
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
        /// <seealso cref="SegmentFileBasePosition(long, long, int, int)"></seealso>
        public void DetachSegments(long recordingId, long newStartPosition)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.DetachSegments(recordingId, newStartPosition, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send detach segments request");
                }

                PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Delete segments which have been previously detached from a recording.
        /// </summary>
        /// <param name="recordingId"> to which the operation applies. </param>
        /// <returns> count of deleted segment files. </returns>
        /// <seealso cref="DetachSegments(long, long)"></seealso>
        public long DeleteDetachedSegments(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.DeleteDetachedSegments(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send delete detached segments request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Purge (detach and delete) segments from the beginning of a recording up to the provided new start position.
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
        /// <returns> count of deleted segment files. </returns>
        /// <seealso cref="DetachSegments(long, long)"></seealso>
        /// <seealso cref="DeleteDetachedSegments(long)"></seealso>
        /// <seealso cref="SegmentFileBasePosition(long, long, int, int)"></seealso>
        public long PurgeSegments(long recordingId, long newStartPosition)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.PurgeSegments(recordingId, newStartPosition, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send purge segments request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Attach segments to the beginning of a recording to restore history that was previously detached.
        /// <para>
        /// Segment files must match the existing recording and join exactly to the start position of the recording
        /// they are being attached to.
        /// 
        /// </para>
        /// </summary>
        /// <param name="recordingId"> to which the operation applies. </param>
        /// <returns> count of attached segment files. </returns>
        /// <seealso cref="DetachSegments(long, long)"></seealso>
        public long AttachSegments(long recordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.AttachSegments(recordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send attach segments request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }

        /// <summary>
        /// Migrate segments from a source recording and attach them to the beginning of a destination recording.
        /// <para>
        /// The source recording must match the destination recording for segment length, term length, mtu length,
        /// stream id, plus the stop position and term id of the source must join with the start position of the
        /// destination and be on a segment boundary.
        /// </para>
        /// <para>
        /// The source recording will be effectively truncated back to its start position after the migration.
        /// 
        /// </para>
        /// </summary>
        /// <param name="srcRecordingId"> source recording from which the segments will be migrated. </param>
        /// <param name="dstRecordingId"> destination recording to which the segments will be attached. </param>
        /// <returns> count of attached segment files. </returns>
        public long MigrateSegments(long srcRecordingId, long dstRecordingId)
        {
            _lock.Lock();
            try
            {
                EnsureOpen();
                EnsureNotReentrant();

                lastCorrelationId = aeron.NextCorrelationId();

                if (!archiveProxy.MigrateSegments(srcRecordingId, dstRecordingId, lastCorrelationId, controlSessionId))
                {
                    throw new ArchiveException("failed to send migrate segments request");
                }

                return PollForResponse(lastCorrelationId);
            }
            finally
            {
                _lock.Unlock();
            }
        }


        private void CheckDeadline(long deadlineNs, string errorMessage, long correlationId)
        {
            if (deadlineNs - nanoClock.NanoTime() < 0)
            {
                throw new AeronTimeoutException(errorMessage + " - correlationId=" + correlationId, Category.ERROR);
            }

            try
            {
                Thread.Sleep(0); // allow thread to be interrupted
            }
            catch (ThreadInterruptedException)
            {
                throw new AeronException("unexpected interrupt");
            }
        }

        private void PollNextResponse(long correlationId, long deadlineNs, ControlResponsePoller poller)
        {
            idleStrategy.Reset();

            while (true)
            {
                int fragments = poller.Poll();

                if (poller.PollComplete)
                {
                    if (controlResponsePoller.TemplateId() == RecordingSignalEventDecoder.TEMPLATE_ID &&
                        controlResponsePoller.ControlSessionId() == controlSessionId)
                    {
                        DispatchRecordingSignal();
                        continue;
                    }

                    break;
                }

                if (fragments > 0)
                {
                    continue;
                }

                if (!poller.Subscription().IsConnected)
                {
                    throw new ArchiveException("response channel from archive is not connected");
                }

                CheckDeadline(deadlineNs, "awaiting response", correlationId);
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

                if (poller.ControlSessionId() != controlSessionId)
                {
                    InvokeAeronClient();
                    continue;
                }

                var code = poller.Code();
                if (code == ControlResponseCode.ERROR)
                {
                    var ex = new ArchiveException("response for correlationId=" + correlationId + ", error: " +
                                                  poller.ErrorMessage(), (int)poller.RelevantId(),
                        poller.CorrelationId());

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

        private bool PollForResponseAllowingError(long correlationId, int allowedErrorCode)
        {
            long deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
            ControlResponsePoller poller = controlResponsePoller;

            while (true)
            {
                PollNextResponse(correlationId, deadlineNs, poller);

                if (poller.ControlSessionId() != controlSessionId)
                {
                    InvokeAeronClient();
                    continue;
                }

                ControlResponseCode code = poller.Code();
                if (ControlResponseCode.ERROR == code)
                {
                    long relevantId = poller.RelevantId();
                    if (poller.CorrelationId() == correlationId)
                    {
                        if (relevantId == allowedErrorCode)
                        {
                            return false;
                        }

                        throw new ArchiveException(
                            "response for correlationId=" + correlationId + ", error: " + poller.ErrorMessage(),
                            (int)relevantId, poller.CorrelationId());
                    }
                    else if (context.ErrorHandler() != null)
                    {
                        context.ErrorHandler()(new ArchiveException(
                            "response for correlationId=" + correlationId + ", error: " + poller.ErrorMessage(),
                            (int)relevantId, poller.CorrelationId()));
                    }
                }
                else if (poller.CorrelationId() == correlationId)
                {
                    if (ControlResponseCode.OK != code)
                    {
                        throw new ArchiveException("unexpected response code: " + code);
                    }

                    return true;
                }
            }
        }

        private int PollForDescriptors(long correlationId, int count, IRecordingDescriptorConsumer consumer)
        {
            int existingRemainCount = count;
            long deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
            RecordingDescriptorPoller poller = RecordingDescriptorPoller();
            poller.Reset(correlationId, count, consumer);
            idleStrategy.Reset();

            while (true)
            {
                int fragments = poller.Poll();
                int remainingRecordCount = poller.RemainingRecordCount();

                if (poller.IsDispatchComplete())
                {
                    return count - remainingRecordCount;
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
                    throw new ArchiveException("response channel from archive is not connected");
                }

                CheckDeadline(deadlineNs, "awaiting recording descriptors", correlationId);
                idleStrategy.Idle();
            }
        }

        private int PollForSubscriptionDescriptors(long correlationId, int count,
            IRecordingSubscriptionDescriptorConsumer consumer)
        {
            int existingRemainCount = count;
            long deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
            RecordingSubscriptionDescriptorPoller poller = RecordingSubscriptionDescriptorPoller();
            poller.Reset(correlationId, count, consumer);
            idleStrategy.Reset();

            while (true)
            {
                int fragments = poller.Poll();
                int remainingSubscriptionCount = poller.RemainingSubscriptionCount();

                if (poller.DispatchComplete)
                {
                    return count - remainingSubscriptionCount;
                }

                if (remainingSubscriptionCount != existingRemainCount)
                {
                    existingRemainCount = remainingSubscriptionCount;
                    deadlineNs = nanoClock.NanoTime() + messageTimeoutNs;
                }

                InvokeAeronClient();

                if (fragments > 0)
                {
                    continue;
                }

                if (!poller.Subscription().IsConnected)
                {
                    throw new ArchiveException("response channel from archive is not connected");
                }

                CheckDeadline(deadlineNs, "awaiting subscription descriptors", correlationId);
                idleStrategy.Idle();
            }
        }

        private void DispatchRecordingSignal()
        {
            context.RecordingSignalConsumer().OnSignal(
                controlResponsePoller.ControlSessionId(),
                controlResponsePoller.CorrelationId(),
                controlResponsePoller.RecordingId(),
                controlResponsePoller.SubscriptionId(),
                controlResponsePoller.Position(),
                controlResponsePoller.RecordingSignal());
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

        private void EnsureNotReentrant()
        {
            if (isInCallback)
            {
                throw new AeronException("reentrant calls not permitted during callbacks");
            }
        }

        /// <summary>
        /// Common configuration properties for communicating with an Aeron archive.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Major version of the network protocol from client to archive. If these don't match then client and archive
            /// are not compatible.
            /// </summary>
            public const int PROTOCOL_MAJOR_VERSION = 1;

            /// <summary>
            /// Minor version of the network protocol from client to archive. If these don't match then some features may
            /// not be available.
            /// </summary>
            public const int PROTOCOL_MINOR_VERSION = 7;

            /// <summary>
            /// Patch version of the network protocol from client to archive. If these don't match then bug fixes may not
            /// have been applied.
            /// </summary>
            public const int PROTOCOL_PATCH_VERSION = 0;

            /// <summary>
            /// Combined semantic version for the archive control protocol.
            /// </summary>
            /// <seealso cref="SemanticVersion"/>
            public static readonly int PROTOCOL_SEMANTIC_VERSION = SemanticVersion.Compose(
                PROTOCOL_MAJOR_VERSION, PROTOCOL_MINOR_VERSION, PROTOCOL_PATCH_VERSION);

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
            public const int LOCAL_CONTROL_STREAM_ID_DEFAULT = CONTROL_STREAM_ID_DEFAULT;

            /// <summary>
            /// Channel for receiving control response messages from an archive.
            /// 
            /// <para>
            /// Channel's <em>endpoint</em> can be specified explicitly (i.e. by providing address and port pair) or
            /// by using zero as a port number. Here is an example of valid response channels:
            /// <ul>
            ///     <li>{@code aeron:udp?endpoint=localhost:8020} - listen on port {@code 8020} on localhost.</li>
            ///     <li>{@code aeron:udp?endpoint=192.168.10.10:8020} - listen on port {@code 8020} on
            ///     {@code 192.168.10.10}.</li>
            ///     <li>{@code aeron:udp?endpoint=localhost:0} - in this case the port is unspecified and the OS
            ///     will assign a free port from the
            ///     <a href="https://en.wikipedia.org/wiki/Ephemeral_port">ephemeral port range</a>.</li>
            /// </ul>
            /// </para>
            /// </summary>
            public const string CONTROL_RESPONSE_CHANNEL_PROP_NAME = "aeron.archive.control.response.channel";

            /// <summary>
            /// Default channel for receiving control response messages from an archive.
            /// </summary>
            public const string CONTROL_RESPONSE_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:0";

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
            /// for dynamic subscribers, an endpoint can be added to the subscription side for controlling port usage.
            /// </summary>
            public const string RECORDING_EVENTS_CHANNEL_DEFAULT =
                "aeron:udp?control-mode=dynamic|control=localhost:8030";

            /// <summary>
            /// Stream id within a channel for receiving progress of recordings from an archive.
            /// </summary>
            public const string RECORDING_EVENTS_STREAM_ID_PROP_NAME = "aeron.archive.recording.events.stream.id";

            /// <summary>
            /// Stream id within a channel for receiving progress of recordings from an archive.
            /// </summary>
            public const int RECORDING_EVENTS_STREAM_ID_DEFAULT = 30;

            /// <summary>
            /// Is channel enabled for recording progress events of recordings from an archive.
            /// </summary>
            public const string RECORDING_EVENTS_ENABLED_PROP_NAME = "aeron.archive.recording.events.enabled";

            /// <summary>
            /// Channel enabled for recording progress events of recordings from an archive which defaults to true.
            /// </summary>
            public const bool RECORDING_EVENTS_ENABLED_DEFAULT = true;

            /// <summary>
            /// Sparse term buffer indicator for control streams.
            /// </summary>
            public const string CONTROL_TERM_BUFFER_SPARSE_PROP_NAME = "aeron.archive.control.term.buffer.sparse";

            /// <summary>
            /// Overrides driver's sparse term buffer indicator for control streams.
            /// </summary>
            public const bool CONTROL_TERM_BUFFER_SPARSE_DEFAULT = true;

            /// <summary>
            /// Term length for control streams.
            /// </summary>
            public const string CONTROL_TERM_BUFFER_LENGTH_PROP_NAME = "aeron.archive.control.term.buffer.length";

            /// <summary>
            /// Low term length for control channel reflects expected low bandwidth usage.
            /// </summary>
            public const int CONTROL_TERM_BUFFER_LENGTH_DEFAULT = 64 * 1024;

            /// <summary>
            /// MTU length for control streams.
            /// </summary>
            public const string CONTROL_MTU_LENGTH_PROP_NAME = "aeron.archive.control.mtu.length";

            /// <summary>
            ///  MTU to reflect default for the control streams.
            /// </summary>
            public const int CONTROL_MTU_LENGTH_DEFAULT = 1408;

            private sealed class NoOpRecordingSignalConsumer : IRecordingSignalConsumer
            {
                public void OnSignal(long controlSessionId, long correlationId, long recordingId, long subscriptionId,
                    long position,
                    RecordingSignal signal)
                {
                }
            }

            /// <summary>
            /// Default no operation <seealso cref="IRecordingSignalConsumer"/> to be used when not set explicitly.
            /// </summary>
            public static readonly IRecordingSignalConsumer NO_OP_RECORDING_SIGNAL_CONSUMER =
                new NoOpRecordingSignalConsumer();

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
            /// <seealso cref="CONTROL_TERM_BUFFER_SPARSE_PROP_NAME"/>
            public static bool ControlTermBufferSparse()
            {
                string propValue = Config.GetProperty(CONTROL_TERM_BUFFER_SPARSE_PROP_NAME);
                return null != propValue ? "true".Equals(propValue) : CONTROL_TERM_BUFFER_SPARSE_DEFAULT;
            }

            /// <summary>
            /// Term buffer length to be used for control request and response streams.
            /// </summary>
            /// <returns> term buffer length to be used for control request and response streams. </returns>
            /// <seealso cref="CONTROL_TERM_BUFFER_LENGTH_PROP_NAME"></seealso>
            public static int ControlTermBufferLength()
            {
                return Config.GetSizeAsInt(CONTROL_TERM_BUFFER_LENGTH_PROP_NAME, CONTROL_TERM_BUFFER_LENGTH_DEFAULT);
            }

            /// <summary>
            /// MTU length to be used for control request and response streams.
            /// </summary>
            /// <returns> MTU length to be used for control request and response streams. </returns>
            /// <seealso cref="CONTROL_MTU_LENGTH_PROP_NAME"></seealso>
            public static int ControlMtuLength()
            {
                return Config.GetSizeAsInt(CONTROL_MTU_LENGTH_PROP_NAME, CONTROL_MTU_LENGTH_DEFAULT);
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
            /// <seealso cref="LOCAL_CONTROL_CHANNEL_PROP_NAME"/> if set.
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

            /// <summary>
            /// Should the recording events stream be enabled.
            /// </summary>
            /// <returns> true if the recording events stream be enabled. </returns>
            /// <seealso cref="RECORDING_EVENTS_ENABLED_PROP_NAME"></seealso>
            public static bool RecordingEventsEnabled()
            {
                string propValue = Config.GetProperty(RECORDING_EVENTS_ENABLED_PROP_NAME);
                return null != propValue ? "true".Equals(propValue) : RECORDING_EVENTS_ENABLED_DEFAULT;
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
            private int _isConcluded = 0;

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
            private ICredentialsSupplier credentialsSupplier;
            private IRecordingSignalConsumer recordingSignalConsumer = Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER;
            internal bool ownsAeronClient = false;

            public Context Clone()
            {
                return (Context)MemberwiseClone();
            }

            /// <summary>
            /// Conclude configuration by setting up defaults when specifics are not provided.
            /// </summary>
            public void Conclude()
            {
                if (0 != Interlocked.Exchange(ref _isConcluded, 1))
                {
                    throw new ConcurrentConcludeException();
                }

                if (null == aeron)
                {
                    aeron = Aeron.Aeron.Connect(
                        new Aeron.Aeron.Context()
                            .AeronDirectoryName(aeronDirectoryName)
                            .ErrorHandler(errorHandler)
                    );

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

                if (null == credentialsSupplier)
                {
                    credentialsSupplier = new NullCredentialsSupplier();
                }

                if (null == _lock)
                {
                    _lock = new ReentrantLock();
                }

                controlRequestChannel = ApplyDefaultParams(controlRequestChannel);
                controlResponseChannel = ApplyDefaultParams(controlResponseChannel);
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
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_SPARSE_PROP_NAME"></seealso>
            public Context ControlTermBufferSparse(bool controlTermBufferSparse)
            {
                this.controlTermBufferSparse = controlTermBufferSparse;
                return this;
            }

            /// <summary>
            /// Should the control streams use sparse file term buffers.
            /// </summary>
            /// <returns> true if the control stream should use sparse file term buffers. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_SPARSE_PROP_NAME"></seealso>
            public bool ControlTermBufferSparse()
            {
                return controlTermBufferSparse;
            }

            /// <summary>
            /// Set the term buffer length for the control streams.
            /// </summary>
            /// <param name="controlTermBufferLength"> for the control stream. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_LENGTH_PROP_NAME"></seealso>
            public Context ControlTermBufferLength(int controlTermBufferLength)
            {
                this.controlTermBufferLength = controlTermBufferLength;
                return this;
            }

            /// <summary>
            /// Get the term buffer length for the control streams.
            /// </summary>
            /// <returns> the term buffer length for the control streams. </returns>
            /// <seealso cref="Configuration.CONTROL_TERM_BUFFER_LENGTH_PROP_NAME"></seealso>
            public int ControlTermBufferLength()
            {
                return controlTermBufferLength;
            }

            /// <summary>
            /// Set the MTU length for the control streams.
            /// </summary>
            /// <param name="controlMtuLength"> for the control streams. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref="Configuration.CONTROL_MTU_LENGTH_PROP_NAME"></seealso>
            public Context ControlMtuLength(int controlMtuLength)
            {
                this.controlMtuLength = controlMtuLength;
                return this;
            }

            /// <summary>
            /// Get the MTU length for the control steams.
            /// </summary>
            /// <returns> the MTU length for the control steams. </returns>
            /// <seealso cref="Configuration.CONTROL_MTU_LENGTH_PROP_NAME"></seealso>
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
            /// Does this context own the <seealso cref="AeronClient()"/> client and thus take responsibility for closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="AeronClient()"/> client? </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                this.ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="AeronClient()"/> client and thus take responsibility for closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="AeronClient()"/> client and thus take responsibility for closing it? </returns>
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
            /// Set the <seealso cref="ICredentialsSupplier"/> to be used for authentication with the archive.
            /// </summary>
            /// <param name="credentialsSupplier"> to be used for authentication with the archive. </param>
            /// <returns> this for fluent API. </returns>
            public Context CredentialsSupplier(ICredentialsSupplier credentialsSupplier)
            {
                this.credentialsSupplier = credentialsSupplier;
                return this;
            }

            /// <summary>
            /// Set the <seealso cref="IRecordingSignalConsumer"/> to will be called  when polling for responses from an Archive.
            /// </summary>
            /// <param name="recordingSignalConsumer"> to called with recording signal events. </param>
            /// <returns> this for a fluent API. </returns>
            public Context RecordingSignalConsumer(IRecordingSignalConsumer recordingSignalConsumer)
            {
                this.recordingSignalConsumer = recordingSignalConsumer;
                return this;
            }

            /// <summary>
            /// Set the <seealso cref="IRecordingSignalConsumer"/> to will be called  when polling for responses from an Archive.
            /// </summary>
            /// <returns> a recording signal consumer. </returns>
            public IRecordingSignalConsumer RecordingSignalConsumer()
            {
                return recordingSignalConsumer;
            }

            /// <summary>
            /// Get the <seealso cref="ICredentialsSupplier"/> to be used for authentication with the archive.
            /// </summary>
            /// <returns> the <seealso cref="ICredentialsSupplier"/> to be used for authentication with the archive. </returns>
            public ICredentialsSupplier CredentialsSupplier()
            {
                return credentialsSupplier;
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

            /// <summary>
            /// {@inheritDoc}
            /// </summary>
            public override string ToString()
            {
                return "AeronArchive.Context" +
                       "\n{" +
                       "\n    isConcluded=" + (1 == _isConcluded) +
                       "\n    ownsAeronClient=" + ownsAeronClient +
                       "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                       "\n    aeron=" + aeron +
                       "\n    messageTimeoutNs=" + messageTimeoutNs +
                       "\n    recordingEventsChannel='" + recordingEventsChannel + '\'' +
                       "\n    recordingEventsStreamId=" + recordingEventsStreamId +
                       "\n    controlRequestChannel='" + controlRequestChannel + '\'' +
                       "\n    controlRequestStreamId=" + controlRequestStreamId +
                       "\n    controlResponseChannel='" + controlResponseChannel + '\'' +
                       "\n    controlResponseStreamId=" + controlResponseStreamId +
                       "\n    controlTermBufferSparse=" + controlTermBufferSparse +
                       "\n    controlTermBufferLength=" + controlTermBufferLength +
                       "\n    controlMtuLength=" + controlMtuLength +
                       "\n    idleStrategy=" + idleStrategy +
                       "\n    lock=" + _lock +
                       "\n    errorHandler=" + errorHandler +
                       "\n    credentialsSupplier=" + credentialsSupplier +
                       "\n}";
            }

            private string ApplyDefaultParams(string channel)
            {
                ChannelUri channelUri = ChannelUri.Parse(channel);

                if (!channelUri.ContainsKey(Aeron.Aeron.Context.TERM_LENGTH_PARAM_NAME))
                {
                    channelUri.Put(Aeron.Aeron.Context.TERM_LENGTH_PARAM_NAME,
                        Convert.ToString(controlTermBufferLength));
                }

                if (!channelUri.ContainsKey(Aeron.Aeron.Context.MTU_LENGTH_PARAM_NAME))
                {
                    channelUri.Put(Aeron.Aeron.Context.MTU_LENGTH_PARAM_NAME, Convert.ToString(controlMtuLength));
                }

                if (!channelUri.ContainsKey(Aeron.Aeron.Context.SPARSE_PARAM_NAME))
                {
                    channelUri.Put(Aeron.Aeron.Context.SPARSE_PARAM_NAME, Convert.ToString(controlTermBufferSparse));
                }

                return channelUri.ToString();
            }
        }

        /// <summary>
        /// Allows for the async establishment of a archive session.
        /// </summary>
        public class AsyncConnect : IDisposable
        {
            private readonly Context ctx;
            private readonly ControlResponsePoller controlResponsePoller;
            private readonly INanoClock nanoClock;
            private readonly long deadlineNs;
            private ArchiveProxy archiveProxy;
            private long correlationId = Aeron.Aeron.NULL_VALUE;
            private long challengeControlSessionId = Aeron.Aeron.NULL_VALUE;
            private byte[] encodedCredentialsFromChallenge = null;
            private int _step = 0;

            internal AsyncConnect(Context ctx)
            {
                this.ctx = ctx;

                Aeron.Aeron aeron = ctx.AeronClient();
                nanoClock = aeron.Ctx.NanoClock();
                controlResponsePoller =
                    new ControlResponsePoller(aeron.AddSubscription(ctx.ControlResponseChannel(),
                        ctx.ControlResponseStreamId()));

                correlationId =
                    aeron.AsyncAddExclusivePublication(ctx.ControlRequestChannel(), ctx.ControlRequestStreamId());
                deadlineNs = nanoClock.NanoTime() + ctx.MessageTimeoutNs();
            }


            internal AsyncConnect(
                Context ctx,
                ControlResponsePoller controlResponsePoller,
                ArchiveProxy archiveProxy)
            {
                this.ctx = ctx;
                this.controlResponsePoller = controlResponsePoller;
                this.archiveProxy = archiveProxy;
                this.nanoClock = ctx.AeronClient().Ctx.NanoClock();

                deadlineNs = nanoClock.NanoTime() + ctx.MessageTimeoutNs();
                _step = 1;
            }

            /// <summary>
            /// Close any allocated resources.
            /// </summary>
            public void Dispose()
            {
                if (5 != _step)
                {
                    ErrorHandler errorHandler = ctx.ErrorHandler();
                    CloseHelper.Dispose(errorHandler, controlResponsePoller.Subscription());

                    if (null != archiveProxy)
                    {
                        CloseHelper.Dispose(errorHandler, archiveProxy.Pub());
                    }

                    ctx.Dispose();
                }
            }

            /// <summary>
            /// Get the <seealso cref="AeronArchive.Context"/> used for this client.
            /// </summary>
            /// <returns> the <seealso cref="AeronArchive.Context"/> used for this client. </returns>
            public Context Ctx()
            {
                return ctx;
            }

            /// <summary>
            /// Get the index of the current step.
            /// </summary>
            /// <returns> the index of the current step. </returns>
            public int Step()
            {
                return _step;
            }

            /// <summary>
            /// Poll for a complete connection.
            /// </summary>
            /// <exception cref="InvalidOperationException"></exception>
            /// <returns> a new <seealso cref="AeronArchive"/> if successfully connected otherwise null. </returns>
            public AeronArchive Poll()
            {
                CheckDeadline();
                AeronArchive aeronArchive = null;

                if (0 == _step)
                {
                    ExclusivePublication publication = ctx.AeronClient().GetExclusivePublication(correlationId);
                    if (null != publication)
                    {
                        archiveProxy = new ArchiveProxy(
                            publication,
                            ctx.IdleStrategy(),
                            ctx.AeronClient().Ctx.NanoClock(),
                            ctx.MessageTimeoutNs(),
                            ArchiveProxy.DEFAULT_RETRY_ATTEMPTS,
                            ctx.CredentialsSupplier());

                        Step(1);
                    }
                }

                if (1 == _step)
                {
                    if (!archiveProxy.Pub().IsConnected)
                    {
                        return null;
                    }

                    Step(2);
                }

                if (2 == _step)
                {
                    string responseChannel = controlResponsePoller.Subscription().TryResolveChannelEndpointPort();
                    if (null == responseChannel)
                    {
                        return null;
                    }

                    correlationId = ctx.AeronClient().NextCorrelationId();

                    if (!archiveProxy.TryConnect(responseChannel, ctx.ControlResponseStreamId(), correlationId))
                    {
                        return null;
                    }

                    Step(3);
                }

                if (3 == _step)
                {
                    if (!controlResponsePoller.Subscription().IsConnected)
                    {
                        return null;
                    }

                    Step(4);
                }

                if (6 == _step)
                {
                    if (!archiveProxy.TryChallengeResponse(encodedCredentialsFromChallenge, correlationId,
                            challengeControlSessionId))
                    {
                        return null;
                    }

                    Step(7);
                }

                controlResponsePoller.Poll();

                if (controlResponsePoller.PollComplete && controlResponsePoller.CorrelationId() == correlationId)
                {
                    long controlSessionId = controlResponsePoller.ControlSessionId();
                    if (controlResponsePoller.WasChallenged())
                    {
                        encodedCredentialsFromChallenge = ctx.CredentialsSupplier()
                            .OnChallenge(controlResponsePoller.EncodedChallenge());

                        correlationId = ctx.AeronClient().NextCorrelationId();
                        challengeControlSessionId = controlSessionId;

                        Step(6);
                    }
                    else
                    {
                        ControlResponseCode code = controlResponsePoller.Code();
                        if (code != ControlResponseCode.OK)
                        {
                            if (code == ControlResponseCode.ERROR)
                            {
                                archiveProxy.CloseSession(controlSessionId);

                                throw new ArchiveException("error: " + controlResponsePoller.ErrorMessage(),
                                    (int)controlResponsePoller.RelevantId());
                            }

                            throw new ArchiveException("unexpected response: code=" + code);
                        }

                        archiveProxy.KeepAlive(controlSessionId, Aeron.Aeron.NULL_VALUE);
                        aeronArchive = new AeronArchive(ctx, controlResponsePoller, archiveProxy, controlSessionId);

                        Step(5);
                    }
                }

                return aeronArchive;
            }

            private void CheckDeadline()
            {
                if (deadlineNs - nanoClock.NanoTime() < 0)
                {
                    throw new TimeoutException("Archive connect timeout: step=" + _step +
                                               (_step < 3
                                                   ? " publication.uri=" + ctx.ControlRequestChannel()
                                                   : " subscription.uri=" + ctx.ControlResponseChannel()));
                }

                try
                {
                    Thread.Sleep(0); // allow thread to be interrupted
                }
                catch (ThreadInterruptedException)
                {
                    throw new AeronException("unexpected interrupt");
                }
            }

            private void Step(int step)
            {
                _step = step;
            }
        }
    }
}