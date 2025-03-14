/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Adaptive.Aeron.Command;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using static Adaptive.Agrona.Concurrent.Status.CountersReader;

[assembly: InternalsVisibleTo("Adaptive.Aeron.Tests")]

namespace Adaptive.Aeron
{
    /// <summary>
    /// Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
    /// commands from the Client API to the Media Driver conductor.
    /// </summary>
    internal class ClientConductor : IAgent
    {
        private const long NO_CORRELATION_ID = Aeron.NULL_VALUE;
        private static readonly long EXPLICIT_CLOSE_LINGER_NS = 1000000000;

        private readonly long idleSleepDurationNs;
        private readonly long _keepAliveIntervalNs;
        private readonly long _driverTimeoutMs;
        private readonly long _driverTimeoutNs;
        private readonly long _interServiceTimeoutNs;
        private long _timeOfLastKeepAliveNs;
        private long _timeOfLastServiceNs;
        private bool _isClosed;
        private bool _isInCallback;
        private bool _isTerminating;
        private RegistrationException _driverException;

        private readonly Aeron.Context _ctx;
        private readonly Aeron _aeron;
        private readonly ILock _clientLock;
        private readonly IEpochClock _epochClock;
        private readonly INanoClock _nanoClock;
        private readonly IIdleStrategy _awaitingIdleStrategy;
        private readonly DriverEventsAdapter _driverEventsAdapter;
        private readonly ILogBuffersFactory _logBuffersFactory;

        private readonly Map<long, LogBuffers> _logBuffersByIdMap = new Map<long, LogBuffers>();

        private readonly List<LogBuffers> _lingeringLogBuffers = new List<LogBuffers>();
        private Map<long, object> _resourceByRegIdMap = new Map<long, object>();

        private readonly Map<long, RegistrationException> _asyncExceptionByRegIdMap =
            new Map<long, RegistrationException>();

        private readonly Map<long, string> _stashedChannelByRegistrationId = new Map<long, string>();

        private HashSet<long> _asyncCommandIdSet = new HashSet<long>();
        private readonly AvailableImageHandler _defaultAvailableImageHandler;
        private readonly UnavailableImageHandler _defaultUnavailableImageHandler;

        private readonly Map<long, AvailableCounterHandler> _availableCounterHandlers =
            new Map<long, AvailableCounterHandler>();

        private readonly Map<long, UnavailableCounterHandler> _unavailableCounterHandlers =
            new Map<long, UnavailableCounterHandler>();

        private readonly Map<long, Action> _closeHandlersByIdMap = new Map<long, Action>();
        private readonly DriverProxy _driverProxy;
        private readonly AgentInvoker _driverAgentInvoker;
        private readonly UnsafeBuffer _counterValuesBuffer;
        private readonly CountersReader _countersReader;
        private readonly PublicationErrorFrame publicationErrorFrame = new PublicationErrorFrame();
        private AtomicCounter _heartbeatTimestamp;

        internal ClientConductor(Aeron.Context ctx, Aeron aeron)
        {
            _ctx = ctx;
            _aeron = aeron;

            _clientLock = ctx.ClientLock();
            _epochClock = ctx.EpochClock();
            _nanoClock = ctx.NanoClock();
            _awaitingIdleStrategy = ctx.AwaitingIdleStrategy();
            _driverProxy = ctx.DriverProxy();
            _logBuffersFactory = ctx.LogBuffersFactory();
            idleSleepDurationNs = ctx.IdleSleepDurationNs();
            _keepAliveIntervalNs = ctx.KeepAliveIntervalNs();
            _driverTimeoutMs = ctx.DriverTimeoutMs();
            _driverTimeoutNs = _driverTimeoutMs * 1000000;
            _interServiceTimeoutNs = ctx.InterServiceTimeoutNs();
            _defaultAvailableImageHandler = ctx.AvailableImageHandler();
            _defaultUnavailableImageHandler = ctx.UnavailableImageHandler();
            _driverEventsAdapter =
                new DriverEventsAdapter(ctx.ClientId(), ctx.ToClientBuffer(), this, _asyncCommandIdSet);
            _driverAgentInvoker = ctx.DriverAgentInvoker();
            _counterValuesBuffer = ctx.CountersValuesBuffer();
            _countersReader =
                new CountersReader(ctx.CountersMetaDataBuffer(), ctx.CountersValuesBuffer(), Encoding.ASCII);

            if (null != ctx.AvailableCounterHandler())
            {
                _availableCounterHandlers.Put(aeron.NextCorrelationId(), ctx.AvailableCounterHandler());
            }

            if (null != ctx.UnavailableCounterHandler())
            {
                _unavailableCounterHandlers.Put(aeron.NextCorrelationId(), ctx.UnavailableCounterHandler());
            }

            if (null != ctx.CloseHandler())
            {
                _closeHandlersByIdMap.Put(aeron.NextCorrelationId(), ctx.CloseHandler());
            }

            long nowNs = _nanoClock.NanoTime();
            _timeOfLastKeepAliveNs = nowNs;
            _timeOfLastServiceNs = nowNs;
        }

        public void OnStart()
        {
            // Do Nothing
        }

        public void OnClose()
        {
            bool isInterrupted = false;
            _aeron.InternalClose();

            _clientLock.Lock();
            try
            {
                bool isTerminating = _isTerminating;
                _isTerminating = true;
                ForceCloseResources();
                NotifyCloseHandlers();

                try
                {
                    if (isTerminating)
                    {
                        Thread.Sleep(Aeron.Configuration.IDLE_SLEEP_DEFAULT_MS);
                    }

                    Thread.Sleep((int)TimeUnit.NANOSECONDS.ToMillis(_ctx.CloseLingerDurationNs()));
                }
                catch (ThreadInterruptedException)
                {
                    isInterrupted = true;
                }

                foreach (var lingeringLogBuffer in _lingeringLogBuffers)
                {
                    CloseHelper.Dispose(_ctx.ErrorHandler(), lingeringLogBuffer);
                }

                _driverProxy.ClientClose();
                _ctx.Dispose();

                _ctx.CountersMetaDataBuffer().Wrap(0, 0);
                _ctx.CountersValuesBuffer().Wrap(0, 0);
            }
            finally
            {
                _isClosed = true;

                if (isInterrupted)
                {
                    Thread.CurrentThread.Interrupt();
                }

                _clientLock.Unlock();
            }
        }

        public int DoWork()
        {
            int workCount = 0;

            if (_clientLock.TryLock())
            {
                try
                {
                    if (_isTerminating)
                    {
                        throw new AgentTerminationException();
                    }

                    workCount = Service(NO_CORRELATION_ID);
                }
                finally
                {
                    _clientLock.Unlock();
                }
            }

            return workCount;
        }

        public string RoleName()
        {
            return "aeron-client-conductor";
        }

        internal bool IsClosed()
        {
            return _isClosed;
        }

        internal bool IsTerminating()
        {
            return _isTerminating;
        }

        internal void OnError(long correlationId, int codeValue, ErrorCode errorCode, string message)
        {
            _driverException = new RegistrationException(correlationId, codeValue, errorCode, message);

            var resource = _resourceByRegIdMap.Get(correlationId);
            if (resource is Subscription subscription)
            {
                subscription.InternalClose(Aeron.NULL_VALUE);
                _resourceByRegIdMap.Remove(correlationId);
            }
        }

        internal void OnAsyncError(long correlationId, int codeValue, ErrorCode errorCode, string message)
        {
            _stashedChannelByRegistrationId.Remove(correlationId);
            var ex = new RegistrationException(correlationId, codeValue, errorCode, message);
            _asyncExceptionByRegIdMap.Put(correlationId, ex);
        }

        internal void OnChannelEndpointError(long correlationId, string message)
        {
            int statusIndicatorId = (int)correlationId;

            foreach (var resource in _resourceByRegIdMap.Values)
            {
                if (resource is Subscription subscription)
                {
                    if (subscription.ChannelStatusId == statusIndicatorId)
                    {
                        HandleError(new ChannelEndpointException(statusIndicatorId, message));
                    }
                }
                else if (resource is Publication publication)
                {
                    if (publication.ChannelStatusId == statusIndicatorId)
                    {
                        HandleError(new ChannelEndpointException(statusIndicatorId, message));
                    }
                }
            }

            if (_asyncCommandIdSet.Remove(correlationId))
            {
                _stashedChannelByRegistrationId.Remove(correlationId);
                HandleError(new RegistrationException(
                    correlationId, (int)ErrorCode.CHANNEL_ENDPOINT_ERROR, ErrorCode.CHANNEL_ENDPOINT_ERROR, message));
            }
        }

        internal void OnPublicationError(PublicationErrorFrameFlyweight errorFrameFlyweight)
        {
            foreach (Object resource in _resourceByRegIdMap.Values)
            {
                if (resource is Publication)
                {
                    Publication publication = (Publication)resource;
                    if (publication.OriginalRegistrationId == errorFrameFlyweight.RegistrationId())
                    {
                        publicationErrorFrame.Set(errorFrameFlyweight);
                        _ctx.PublicationErrorFrameHandler().OnPublicationError(publicationErrorFrame);
                    }
                }
            }
        }

        internal void OnNewPublication(
            long correlationId,
            long registrationId,
            int streamId,
            int sessionId,
            int publicationLimitId,
            int statusIndicatorId,
            string logFileName)
        {
            string stashedChannel = _stashedChannelByRegistrationId.Remove(correlationId);
            var publication = new ConcurrentPublication(
                this,
                stashedChannel,
                streamId,
                sessionId,
                new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId),
                statusIndicatorId,
                LogBuffers(registrationId, logFileName, stashedChannel),
                registrationId,
                correlationId
            );

            _resourceByRegIdMap.Put(correlationId, publication);
        }

        internal void OnNewExclusivePublication(
            long correlationId,
            long registrationId,
            int streamId,
            int sessionId,
            int publicationLimitId,
            int statusIndicatorId,
            string logFileName)
        {
            if (correlationId != registrationId)
            {
                HandleError(new InvalidOperationException("correlationId=" + correlationId + " registrationId=" +
                                                          registrationId));
            }

            string stashedChannel = _stashedChannelByRegistrationId.Remove(correlationId);
            var publication = new ExclusivePublication(
                this,
                stashedChannel,
                streamId,
                sessionId,
                new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId),
                statusIndicatorId,
                LogBuffers(registrationId, logFileName, stashedChannel),
                registrationId,
                correlationId
            );

            _resourceByRegIdMap.Put(correlationId, publication);
        }

        internal void OnNewSubscription(long correlationId, int statusIndicatorId)
        {
            var resource = _resourceByRegIdMap.Get(correlationId);
            Subscription subscription;

            if (resource is PendingSubscription)
            {
                subscription = ((PendingSubscription)resource).Subscription;
                _resourceByRegIdMap.Put(correlationId, subscription);
            }
            else
            {
                subscription = (Subscription)resource;
            }

            subscription.ChannelStatusId = statusIndicatorId;
        }

        internal void OnAvailableImage(
            long correlationId,
            int sessionId,
            long subscriptionRegistrationId,
            int subscriberPositionId,
            string logFileName,
            string sourceIdentity)
        {
            Subscription subscription = (Subscription)_resourceByRegIdMap.Get(subscriptionRegistrationId);
            if (null != subscription)
            {
                Image image = new Image(
                    subscription,
                    sessionId,
                    new UnsafeBufferPosition(_counterValuesBuffer, subscriberPositionId),
                    LogBuffers(correlationId, logFileName, subscription.Channel),
                    _ctx.SubscriberErrorHandler(),
                    sourceIdentity,
                    correlationId);

                subscription.AddImage(image);

                AvailableImageHandler handler = subscription.AvailableImageHandler;
                if (null != handler)
                {
                    _isInCallback = true;

                    try
                    {
                        handler(image);
                    }
                    catch (Exception ex)
                    {
                        HandleError(ex);
                    }
                    finally
                    {
                        _isInCallback = false;
                    }
                }
            }
        }

        internal void OnUnavailableImage(long correlationId, long subscriptionRegistrationId)
        {
            Subscription subscription = (Subscription)_resourceByRegIdMap.Get(subscriptionRegistrationId);
            if (null != subscription)
            {
                Image image = subscription.RemoveImage(correlationId);
                if (null != image)
                {
                    UnavailableImageHandler handler = subscription.UnavailableImageHandler;
                    if (null != handler)
                    {
                        NotifyImageUnavailable(handler, image);
                    }
                }
            }
        }

        internal void OnNewCounter(long correlationId, int counterId)
        {
            _resourceByRegIdMap.Put(correlationId, new Counter(correlationId, this, _counterValuesBuffer, counterId));
            OnAvailableCounter(correlationId, counterId);
        }

        internal void OnAvailableCounter(long registrationId, int counterId)
        {
            foreach (var handler in _availableCounterHandlers.Values)
            {
                NotifyCounterAvailable(registrationId, counterId, handler);
            }
        }

        internal void OnUnavailableCounter(long registrationId, int counterId)
        {
            NotifyUnavailableCounterHandlers(registrationId, counterId);
        }

        internal void OnClientTimeout()
        {
            if (!_isClosed)
            {
                TerminateConductor();
                HandleError(new ClientTimeoutException("client timeout from driver"));
            }
        }

        internal CountersReader CountersReader()
        {
            return _countersReader;
        }

        internal void HandleError(Exception ex)
        {
            if (!_isClosed)
            {
                _ctx.ErrorHandler().OnError(ex);
            }
        }

        internal ConcurrentPublication AddPublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _driverProxy.AddPublication(channel, streamId);
                _stashedChannelByRegistrationId.Put(registrationId, channel);
                AwaitResponse(registrationId);

                return (ConcurrentPublication)_resourceByRegIdMap.Get(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal ExclusivePublication AddExclusivePublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _driverProxy.AddExclusivePublication(channel, streamId);
                _stashedChannelByRegistrationId.Put(registrationId, channel);
                AwaitResponse(registrationId);

                return (ExclusivePublication)_resourceByRegIdMap.Get(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncAddPublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _driverProxy.AddPublication(channel, streamId);
                _stashedChannelByRegistrationId.Put(registrationId, channel);
                _asyncCommandIdSet.Add(registrationId);

                return registrationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncAddExclusivePublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _driverProxy.AddExclusivePublication(channel, streamId);
                _stashedChannelByRegistrationId.Put(registrationId, channel);
                _asyncCommandIdSet.Add(registrationId);

                return registrationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal ConcurrentPublication GetPublication(long registrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (_asyncCommandIdSet.Contains(registrationId))
                {
                    Service(NO_CORRELATION_ID);
                }

                return ResourceOrThrow<ConcurrentPublication>(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal ExclusivePublication GetExclusivePublication(long registrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (_asyncCommandIdSet.Contains(registrationId))
                {
                    Service(NO_CORRELATION_ID);
                }

                return ResourceOrThrow<ExclusivePublication>(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemovePublication(Publication publication)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return;
                }

                if (!publication.IsClosed)
                {
                    EnsureNotReentrant();

                    publication.InternalClose();

                    if (publication == _resourceByRegIdMap.Remove(publication.RegistrationId))
                    {
                        ReleaseLogBuffers(publication.LogBuffers, publication.OriginalRegistrationId,
                            EXPLICIT_CLOSE_LINGER_NS);
                        _asyncCommandIdSet.Add(_driverProxy.RemovePublication(publication.RegistrationId));
                    }
                }
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemovePublication(long publicationRegistrationId)
        {
            _clientLock.Lock();
            try
            {
                if (Aeron.NULL_VALUE == publicationRegistrationId || _isTerminating || _isClosed)
                {
                    return;
                }

                EnsureNotReentrant();

                object resource = _resourceByRegIdMap.Get(publicationRegistrationId);
                if (null != resource && !(resource is Publication))
                {
                    throw new AeronException("registration id is not a Publication: " + resource.GetType().Name);
                }

                Publication publication = (Publication)resource;
                if (null != publication)
                {
                    _resourceByRegIdMap.Remove(publicationRegistrationId);
                    publication.InternalClose();
                    ReleaseLogBuffers(publication.LogBuffers, publication.OriginalRegistrationId,
                        EXPLICIT_CLOSE_LINGER_NS);
                }

                if (_asyncCommandIdSet.Remove(publicationRegistrationId) || null != publication)
                {
                    _asyncCommandIdSet.Add(_driverProxy.RemovePublication(publicationRegistrationId));
                    _stashedChannelByRegistrationId.Remove(publicationRegistrationId);
                }
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal Subscription AddSubscription(string channel, int streamId)
        {
            return AddSubscription(channel, streamId, _defaultAvailableImageHandler, _defaultUnavailableImageHandler);
        }

        internal Subscription AddSubscription(string channel, int streamId, AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.AddSubscription(channel, streamId);
                Subscription subscription = new Subscription(this, channel, streamId, correlationId,
                    availableImageHandler, unavailableImageHandler);

                _resourceByRegIdMap.Put(correlationId, subscription);
                AwaitResponse(correlationId);

                return subscription;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncAddSubscription(string channel, int streamId)
        {
            return AsyncAddSubscription(channel, streamId, _defaultAvailableImageHandler,
                _defaultUnavailableImageHandler);
        }

        internal long AsyncAddSubscription(string channel, int streamId, AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _driverProxy.AddSubscription(channel, streamId);
                PendingSubscription subscription = new PendingSubscription(new Subscription(this, channel, streamId,
                    registrationId, availableImageHandler, unavailableImageHandler));

                _resourceByRegIdMap.Put(registrationId, subscription);
                _asyncCommandIdSet.Add(registrationId);

                return registrationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal Subscription GetSubscription(long registrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (_asyncCommandIdSet.Contains(registrationId))
                {
                    Service(NO_CORRELATION_ID);
                }

                return ResourceOrThrow<Subscription>(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemoveSubscription(Subscription subscription)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return;
                }

                if (!subscription.IsClosed)
                {
                    EnsureNotReentrant();

                    subscription.InternalClose(EXPLICIT_CLOSE_LINGER_NS);
                    long registrationId = subscription.RegistrationId;
                    if (subscription == _resourceByRegIdMap.Remove(registrationId))
                    {
                        _asyncCommandIdSet.Add(_driverProxy.RemoveSubscription(registrationId));
                    }
                }
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemoveSubscription(long subscriptionRegistrationId)
        {
            _clientLock.Lock();
            try
            {
                if (Aeron.NULL_VALUE == subscriptionRegistrationId || _isTerminating || _isClosed)
                {
                    return;
                }

                EnsureNotReentrant();

                object resource = _resourceByRegIdMap.Get(subscriptionRegistrationId);
                if (resource != null && !(resource is PendingSubscription || resource is Subscription))
                {
                    throw new AeronException("registration id is not a Subscription: " + resource.GetType().Name);
                }

                Subscription subscription = resource is PendingSubscription
                    ? ((PendingSubscription)resource).Subscription
                    : (Subscription)resource;
                if (null != subscription)
                {
                    _resourceByRegIdMap.Remove(subscriptionRegistrationId);
                    subscription.InternalClose(EXPLICIT_CLOSE_LINGER_NS);
                }

                if (_asyncCommandIdSet.Remove(subscriptionRegistrationId) || null != subscription)
                {
                    _asyncCommandIdSet.Add(_driverProxy.RemoveSubscription(subscriptionRegistrationId));
                }
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void AddDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                AwaitResponse(_driverProxy.AddDestination(registrationId, endpointChannel));
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AddDestinationWithId(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.AddDestination(registrationId, endpointChannel);
                AwaitResponse(correlationId);
                return correlationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemoveDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                AwaitResponse(_driverProxy.RemoveDestination(registrationId, endpointChannel));
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemoveDestination(long publicationRegistrationId, long destinationRegistrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                AwaitResponse(_driverProxy.RemoveDestination(publicationRegistrationId, destinationRegistrationId));
            }
            finally
            {
                _clientLock.Unlock();
            }
        }


        internal void AddRcvDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                AwaitResponse(_driverProxy.AddRcvDestination(registrationId, endpointChannel));
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void RemoveRcvDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                AwaitResponse(_driverProxy.RemoveRcvDestination(registrationId, endpointChannel));
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncAddDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.AddDestination(registrationId, endpointChannel);
                _asyncCommandIdSet.Add(correlationId);
                return correlationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncRemoveDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.RemoveDestination(registrationId, endpointChannel);
                _asyncCommandIdSet.Add(correlationId);
                return correlationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncRemoveDestination(long registrationId, long destinationRegistrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.RemoveDestination(registrationId, destinationRegistrationId);
                _asyncCommandIdSet.Add(correlationId);
                return correlationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }


        internal long AsyncAddRcvDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.AddRcvDestination(registrationId, endpointChannel);
                _asyncCommandIdSet.Add(correlationId);
                return correlationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AsyncRemoveRcvDestination(long registrationId, string endpointChannel)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long correlationId = _driverProxy.RemoveRcvDestination(registrationId, endpointChannel);
                _asyncCommandIdSet.Add(correlationId);
                return correlationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool IsCommandActive(long correlationId)
        {
            _clientLock.Lock();
            try
            {
                if (_isClosed)
                {
                    return false;
                }

                EnsureActive();

                if (_asyncCommandIdSet.Contains(correlationId))
                {
                    Service(NO_CORRELATION_ID);
                }

                return _asyncCommandIdSet.Contains(correlationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool HasActiveCommands()
        {
            _clientLock.Lock();
            try
            {
                if (_isClosed)
                {
                    return false;
                }

                EnsureActive();

                return _asyncCommandIdSet.Count > 0;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal Counter AddCounter(int typeId, IDirectBuffer keyBuffer, int keyOffset, int keyLength,
            IDirectBuffer labelBuffer, int labelOffset, int labelLength)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (keyLength < 0 || keyLength > CountersManager.MAX_KEY_LENGTH)
                {
                    throw new ArgumentException("key length out of bounds: " + keyLength);
                }

                if (labelLength < 0 || labelLength > CountersManager.MAX_LABEL_LENGTH)
                {
                    throw new ArgumentException("label length out of bounds: " + labelLength);
                }

                long registrationId = _driverProxy.AddCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer,
                    labelOffset, labelLength);
                AwaitResponse(registrationId);

                return (Counter)_resourceByRegIdMap.Get(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal Counter AddCounter(int typeId, string label)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (label.Length > CountersManager.MAX_LABEL_LENGTH)
                {
                    throw new ArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.Length);
                }

                long registrationId = _driverProxy.AddCounter(typeId, label);

                AwaitResponse(registrationId);

                return (Counter)_resourceByRegIdMap.Get(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal Counter AddStaticCounter(int typeId, IDirectBuffer keyBuffer, int keyOffset, int keyLength,
            IDirectBuffer labelBuffer, int labelOffset, int labelLength, long registrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (keyLength < 0 || keyLength > MAX_KEY_LENGTH)
                {
                    throw new System.ArgumentException("key length out of bounds: " + keyLength);
                }

                if (labelLength < 0 || labelLength > MAX_LABEL_LENGTH)
                {
                    throw new System.ArgumentException("label length out of bounds: " + labelLength);
                }

                long correlationId = _driverProxy.AddStaticCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer,
                    labelOffset, labelLength, registrationId);

                AwaitResponse(correlationId);

                int counterId = (int)_resourceByRegIdMap.Remove(correlationId);
                return new Counter(_aeron.CountersReader, registrationId, counterId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal Counter AddStaticCounter(int typeId, string label, long registrationId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                if (label.Length > MAX_LABEL_LENGTH)
                {
                    throw new ArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.Length);
                }

                long correlationId = _driverProxy.AddStaticCounter(typeId, label, registrationId);
                AwaitResponse(correlationId);

                int counterId = (int)_resourceByRegIdMap.Remove(correlationId);
                return new Counter(_aeron.CountersReader, registrationId, counterId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AddAvailableCounterHandler(AvailableCounterHandler handler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _aeron.NextCorrelationId();
                _availableCounterHandlers.Put(registrationId, handler);
                return registrationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool RemoveAvailableCounterHandler(long registrationId)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return false;
                }

                EnsureNotReentrant();

                return _availableCounterHandlers.Remove(registrationId) != null;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool RemoveAvailableCounterHandler(AvailableCounterHandler handler)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return false;
                }

                EnsureNotReentrant();

                foreach (var keyValuePair in _availableCounterHandlers.KeyValuePairs.Where(kvp => kvp.Value == handler)
                             .ToList())
                {
                    _availableCounterHandlers.Remove(keyValuePair.Key);
                    return true;
                }

                return false;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AddUnavailableCounterHandler(UnavailableCounterHandler handler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();
                long registrationId = _aeron.NextCorrelationId();
                _unavailableCounterHandlers.Put(registrationId, handler);
                return registrationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool RemoveUnavailableCounterHandler(long registrationId)
        {
            _clientLock.Lock();
            try
            {
                return _unavailableCounterHandlers.Remove(registrationId) != null;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool RemoveUnavailableCounterHandler(UnavailableCounterHandler handler)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return false;
                }

                EnsureNotReentrant();

                foreach (var keyValuePair in _unavailableCounterHandlers.KeyValuePairs
                             .Where(kvp => kvp.Value == handler)
                             .ToList())
                {
                    _unavailableCounterHandlers.Remove(keyValuePair.Key);
                    return true;
                }

                return false;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal long AddCloseHandler(Action handler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                long registrationId = _aeron.NextCorrelationId();
                _closeHandlersByIdMap.Put(registrationId, handler);
                return registrationId;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool RemoveCloseHandler(long registrationId)
        {
            _clientLock.Lock();
            try
            {
                return _closeHandlersByIdMap.Remove(registrationId) != null;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal bool RemoveCloseHandler(Action handler)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return false;
                }

                EnsureNotReentrant();


                foreach (var keyValuePair in _closeHandlersByIdMap.KeyValuePairs
                             .Where(kvp => kvp.Value == handler)
                             .ToList())
                {
                    _closeHandlersByIdMap.Remove(keyValuePair.Key);
                    return true;
                }

                return false;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void ReleaseCounter(Counter counter)
        {
            _clientLock.Lock();
            try
            {
                if (_isTerminating || _isClosed)
                {
                    return;
                }

                EnsureNotReentrant();

                long registrationId = counter.RegistrationId;
                if (counter == _resourceByRegIdMap.Remove(registrationId))
                {
                    _asyncCommandIdSet.Add(_driverProxy.RemoveCounter(registrationId));
                }
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void ReleaseLogBuffers(LogBuffers logBuffers, long registrationId, long lingerDurationNs)
        {
            if (logBuffers.DecRef() == 0)
            {
                _lingeringLogBuffers.Add(logBuffers);
                _logBuffersByIdMap.Remove(registrationId);

                long lingerNs = Aeron.NULL_VALUE == lingerDurationNs
                    ? _ctx.ResourceLingerDurationNs()
                    : lingerDurationNs;
                logBuffers.LingerDeadlineNs(_nanoClock.NanoTime() + lingerNs);
            }
        }

        internal DriverEventsAdapter DriverListenerAdapter()
        {
            return _driverEventsAdapter;
        }

        internal long ChannelStatus(int channelStatusId)
        {
            switch (channelStatusId)
            {
                case 0:
                    return ChannelEndpointStatus.INITIALIZING;

                case ChannelEndpointStatus.NO_ID_ALLOCATED:
                    return ChannelEndpointStatus.ACTIVE;

                default:
                    return _countersReader.GetCounterValue(channelStatusId);
            }
        }

        internal void CloseImages(Image[] images, UnavailableImageHandler unavailableImageHandler, long lingerNs)
        {
            foreach (var image in images)
            {
                image.Close();
                ReleaseLogBuffers(image.LogBuffers, image.CorrelationId, lingerNs);
            }

            if (null != unavailableImageHandler)
            {
                foreach (var image in images)
                {
                    NotifyImageUnavailable(unavailableImageHandler, image);
                }
            }
        }

        internal void OnStaticCounter(long correlationId, int counterId)
        {
            _resourceByRegIdMap.Put(correlationId, (int?)counterId);
        }

        internal void RejectImage(long correlationId, long position, string reason)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                // TODO, check reason length??

                long registrationId = _driverProxy.RejectImage(correlationId, position, reason);
                AwaitResponse(registrationId);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        private void EnsureActive()
        {
            if (_isClosed)
            {
                throw new AeronException("Aeron client is closed");
            }

            if (_isTerminating)
            {
                throw new AeronException("Aeron client is terminating");
            }
        }

        private void EnsureNotReentrant()
        {
            if (_isInCallback)
            {
                throw new AeronException("reentrant calls not permitted during callbacks");
            }
        }

        private LogBuffers LogBuffers(long registrationId, string logFileName, String channel)
        {
            LogBuffers logBuffers = _logBuffersByIdMap.Get(registrationId);
            if (null == logBuffers)
            {
                logBuffers = _logBuffersFactory.Map(logFileName);
                _logBuffersByIdMap.Put(registrationId, logBuffers);

                if (_ctx.PreTouchMappedMemory())
                {
                    logBuffers.PreTouch();
                }
            }

            logBuffers.IncRef();

            return logBuffers;
        }


        private int Service(long correlationId)
        {
            int workCount = 0;

            try
            {
                workCount += CheckTimeouts(_nanoClock.NanoTime());
                workCount += _driverEventsAdapter.Receive(correlationId);
            }
            catch (AgentTerminationException)
            {
                if (IsClientApiCall(correlationId))
                {
                    TerminateConductor();
                }

                throw;
            }
            catch (Exception ex)
            {
                if (_driverEventsAdapter.IsInvalid)
                {
                    TerminateConductor();

                    if (!IsClientApiCall(correlationId))
                    {
                        throw new AeronException("Driver events adapter is invalid");
                    }
                }

                if (IsClientApiCall(correlationId))
                {
                    throw;
                }

                HandleError(ex);
            }

            return workCount;
        }

        private void TerminateConductor()
        {
            _isTerminating = true;
            ForceCloseResources();
        }

        private void AwaitResponse(long correlationId)
        {
            _driverException = null;
            var nowNs = _nanoClock.NanoTime();
            var deadlineNs = nowNs + _driverTimeoutNs;
            CheckTimeouts(nowNs);

            _awaitingIdleStrategy.Reset();
            do
            {
                if (null == _driverAgentInvoker)
                {
                    _awaitingIdleStrategy.Idle();
                }
                else
                {
                    _driverAgentInvoker.Invoke();
                }

                Service(correlationId);

                if (_driverEventsAdapter.ReceivedCorrelationId == correlationId)
                {
                    _stashedChannelByRegistrationId.Remove(correlationId);
                    RegistrationException ex = _driverException;
                    if (null != _driverException)
                    {
                        _driverException = null;
                        throw ex;
                    }

                    return;
                }

                try
                {
                    Thread.Sleep(1);
                }
                catch (ThreadInterruptedException)
                {
                    TerminateConductor();
                    throw new AeronException("thread interrupted");
                }
            } while (deadlineNs - _nanoClock.NanoTime() > 0);

            throw new DriverTimeoutException("no response from MediaDriver within " + _driverTimeoutNs + "ns");
        }

        private int CheckTimeouts(long nowNs)
        {
            int workCount = 0;

            if ((_timeOfLastServiceNs + idleSleepDurationNs) - nowNs < 0)
            {
                CheckServiceInterval(nowNs);
                _timeOfLastServiceNs = nowNs;

                workCount += CheckLiveness(nowNs);
                workCount += CheckLingeringResources(nowNs);
            }

            return workCount;
        }

        private void CheckServiceInterval(long nowNs)
        {
            if ((_timeOfLastServiceNs + _interServiceTimeoutNs) - nowNs < 0)
            {
                TerminateConductor();

                throw new ConductorServiceTimeoutException("service interval exceeded: timeout=" +
                                                           _interServiceTimeoutNs + "ns, actual=" +
                                                           (nowNs - _timeOfLastServiceNs) + "ns");
            }
        }

        private int CheckLiveness(long nowNs)
        {
            if ((_timeOfLastKeepAliveNs + _keepAliveIntervalNs) - nowNs < 0)
            {
                var nowMs = _epochClock.Time();
                long lastKeepAliveMs = _driverProxy.TimeOfLastDriverKeepaliveMs();

                if (nowMs > (lastKeepAliveMs + _driverTimeoutMs))
                {
                    TerminateConductor();

                    if (Aeron.NULL_VALUE == lastKeepAliveMs)
                    {
                        throw new DriverTimeoutException(
                            "MediaDriver (" + _aeron.Ctx.AeronDirectoryName() + ") has been shutdown");
                    }

                    throw new DriverTimeoutException("MediaDriver (" + _aeron.Ctx.AeronDirectoryName() +
                                                     ") keepalive: age=" + (nowMs - lastKeepAliveMs) + "ms > timeout=" +
                                                     _driverTimeoutMs +
                                                     "ms");
                }

                if (null == _heartbeatTimestamp)
                {
                    int counterId = HeartbeatTimestamp.FindCounterIdByRegistrationId(_countersReader,
                        HeartbeatTimestamp.HEARTBEAT_TYPE_ID, _ctx.ClientId());

                    if (NULL_COUNTER_ID != counterId)
                    {
                        try
                        {
                            _heartbeatTimestamp = new AtomicCounter(_counterValuesBuffer, counterId);
                            _heartbeatTimestamp.SetOrdered(nowMs);
                            AeronCounters.AppendToLabel(
                                _countersReader.MetaDataBuffer,
                                counterId,
                                " name=" + _ctx.ClientName()
                                //+ " " + AeronCounters.formatVersionInfo(AeronVersion.VERSION, AeronVersion.GIT_SHA)
                            );
                            _timeOfLastKeepAliveNs = nowNs;
                        }
                        catch (Exception ex) // a race caused by the driver timing out the client
                        {
                            TerminateConductor();
                            throw new AeronException("unexpected close of heartbeat timestamp counter: " + counterId, ex);
                        }
                    }
                }
                else
                {
                    int counterId = _heartbeatTimestamp.Id;
                    if (!HeartbeatTimestamp.IsActive(_countersReader, counterId, HeartbeatTimestamp.HEARTBEAT_TYPE_ID,
                            _ctx.ClientId()))
                    {
                        TerminateConductor();
                        throw new AeronException("unexpected close of heartbeat timestamp counter: " + counterId);
                    }

                    _heartbeatTimestamp.SetOrdered(nowMs);
                    _timeOfLastKeepAliveNs = nowNs;
                }

                return 1;
            }

            return 0;
        }

        private int CheckLingeringResources(long nowNs)
        {
            int workCount = 0;

            for (int lastIndex = _lingeringLogBuffers.Count - 1, i = lastIndex; i >= 0; i--)
            {
                LogBuffers logBuffers = _lingeringLogBuffers[i];

                if (logBuffers.LingerDeadlineNs() - nowNs < 0)
                {
                    ListUtil.FastUnorderedRemove(_lingeringLogBuffers, i, lastIndex--);
                    CloseHelper.Dispose(_ctx.ErrorHandler(), logBuffers);
                    workCount += 1;
                }
            }

            return workCount;
        }

        private void ForceCloseResources()
        {
            foreach (var resource in _resourceByRegIdMap.Values)
            {
                if (resource is Subscription subscription)
                {
                    subscription.InternalClose(Aeron.NULL_VALUE);
                }
                else if (resource is Publication publication)
                {
                    publication.InternalClose();
                    ReleaseLogBuffers(publication.LogBuffers, publication.OriginalRegistrationId, Aeron.NULL_VALUE);
                }
                else if (resource is Counter counter)
                {
                    counter.InternalClose();
                    NotifyUnavailableCounterHandlers(counter.RegistrationId, counter.Id);
                }
            }

            _resourceByRegIdMap.Clear();
        }

        private void NotifyUnavailableCounterHandlers(long registrationId, int counterId)
        {
            foreach (var handler in _unavailableCounterHandlers.Values)
            {
                _isInCallback = true;
                try
                {
                    handler(_countersReader, registrationId, counterId);
                }
                catch (AgentTerminationException ex)
                {
                    if (!_isTerminating)
                    {
                        throw;
                    }

                    HandleError(ex);
                }
                catch (Exception ex)
                {
                    HandleError(ex);
                }
                finally
                {
                    _isInCallback = false;
                }
            }
        }

        private void NotifyImageUnavailable(UnavailableImageHandler handler, Image image)
        {
            _isInCallback = true;
            try
            {
                handler(image);
            }
            catch (AgentTerminationException ex)
            {
                if (!_isTerminating)
                {
                    throw;
                }

                HandleError(ex);
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
            finally
            {
                _isInCallback = false;
            }
        }

        private void NotifyCounterAvailable(long registrationId, int counterId, AvailableCounterHandler handler)
        {
            _isInCallback = true;
            try
            {
                handler(_countersReader, registrationId, counterId);
            }
            catch (AgentTerminationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
            finally
            {
                _isInCallback = false;
            }
        }

        private void NotifyCloseHandlers()
        {
            foreach (var closeHandler in _closeHandlersByIdMap.Values)
            {
                _isInCallback = true;
                try
                {
                    closeHandler();
                }
                catch (Exception ex)
                {
                    HandleError(ex);
                }
                finally
                {
                    _isInCallback = false;
                }
            }
        }

        private T ResourceOrThrow<T>(long registrationId)
        {
            object resource = _resourceByRegIdMap.Get(registrationId);

            if (resource is T r)
            {
                return r;
            }

            RegistrationException ex = _asyncExceptionByRegIdMap.Remove(registrationId);
            if (null != ex)
            {
                throw new RegistrationException(ex);
            }

            return default;
        }

        private static bool IsClientApiCall(long correlationId)
        {
            return correlationId != NO_CORRELATION_ID;
        }

        sealed class PendingSubscription
        {
            internal readonly Subscription Subscription;

            internal PendingSubscription(Subscription subscription)
            {
                Subscription = subscription;
            }
        }
    }
}