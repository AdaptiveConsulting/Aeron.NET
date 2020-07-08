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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

[assembly: InternalsVisibleTo("Adaptive.Aeron.Tests")]

namespace Adaptive.Aeron
{
    /// <summary>
    /// Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
    /// commands from the Client API to the Media Driver conductor.
    /// </summary>
    internal class ClientConductor : IAgent, IDriverEventsListener
    {
        private const long NO_CORRELATION_ID = Aeron.NULL_VALUE;

        private readonly long _resourceLingerDurationNs;
        private readonly long _keepAliveIntervalNs;
        private readonly long _driverTimeoutMs;
        private readonly long _driverTimeoutNs;
        private readonly long _interServiceTimeoutNs;
        private long _timeOfLastKeepAliveNs;
        private long _timeOfLastServiceNs;
        private bool _isClosed;
        private bool _isInCallback;
        private bool _isTerminating;
        private string _stashedChannel;
        private RegistrationException _driverException;

        private readonly Aeron.Context _ctx;
        private readonly Aeron _aeron;
        private readonly ILock _clientLock;
        private readonly IEpochClock _epochClock;
        private readonly INanoClock _nanoClock;
        private readonly IIdleStrategy _awaitingIdleStrategy;
        private readonly DriverEventsAdapter _driverEventsAdapter;
        private readonly ILogBuffersFactory _logBuffersFactory;

        private readonly IDictionary<long, LogBuffers> _logBuffersByIdMap =
            new DefaultDictionary<long, LogBuffers>(null);

        private readonly IDictionary<long, object> _resourceByRegIdMap = new DefaultDictionary<long, object>(null);
        private readonly List<IManagedResource> _lingeringResources = new List<IManagedResource>();
        private readonly HashSet<long> _asyncCommandIdSet = new HashSet<long>();
        private readonly AvailableImageHandler _defaultAvailableImageHandler;
        private readonly UnavailableImageHandler _defaultUnavailableImageHandler;
        private readonly List<AvailableCounterHandler> _availableCounterHandlers = new List<AvailableCounterHandler>();

        private readonly List<UnavailableCounterHandler> _unavailableCounterHandlers =
            new List<UnavailableCounterHandler>();

        private readonly List<Action> _closeHandlers = new List<Action>();
        private readonly DriverProxy _driverProxy;
        private readonly AgentInvoker _driverAgentInvoker;
        private readonly UnsafeBuffer _counterValuesBuffer;
        private readonly CountersReader _countersReader;
        private AtomicCounter _heartbeatTimestamp;

        internal ClientConductor()
        {
        }

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
            _keepAliveIntervalNs = ctx.KeepAliveIntervalNs();
            _resourceLingerDurationNs = ctx.ResourceLingerDurationNs();
            _driverTimeoutMs = ctx.DriverTimeoutMs();
            _driverTimeoutNs = _driverTimeoutMs * 1000000;
            _interServiceTimeoutNs = ctx.InterServiceTimeoutNs();
            _defaultAvailableImageHandler = ctx.AvailableImageHandler();
            _defaultUnavailableImageHandler = ctx.UnavailableImageHandler();
            _driverEventsAdapter =
                new DriverEventsAdapter(ctx.ToClientBuffer(), ctx.ClientId(), this, _asyncCommandIdSet);
            _driverAgentInvoker = ctx.DriverAgentInvoker();
            _counterValuesBuffer = ctx.CountersValuesBuffer();
            _countersReader =
                new CountersReader(ctx.CountersMetaDataBuffer(), ctx.CountersValuesBuffer(), Encoding.ASCII);

            if (null != ctx.AvailableCounterHandler())
            {
                _availableCounterHandlers.Add(ctx.AvailableCounterHandler());
            }

            if (null != ctx.UnavailableCounterHandler())
            {
                _unavailableCounterHandlers.Add(ctx.UnavailableCounterHandler());
            }

            if (null != ctx.CloseHandler())
            {
                _closeHandlers.Add(ctx.CloseHandler());
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

            _clientLock.Lock();
            try
            {
                if (!_isClosed)
                {
                    if (!_aeron.IsClosed)
                    {
                        _aeron.InternalClose();
                    }

                    ForceCloseResources();
                    NotifyCloseHandlers();

                    try
                    {
                        if (_isTerminating)
                        {
                            Thread.Sleep(Aeron.Configuration.IdleSleepMs);
                        }

                        Thread.Sleep((int) TimeUnit.NANOSECONDS.toMillis(_ctx.CloseLingerDurationNs()));
                    }
                    catch (ThreadInterruptedException)
                    {
                        isInterrupted = true;
                    }

                    for (int i = 0, size = _lingeringResources.Count; i < size; i++)
                    {
                        DeleteResource(_lingeringResources[i]);
                    }

                    _driverProxy.ClientClose();
                    _ctx.Dispose();
                }
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

        public void OnError(long correlationId, int codeValue, ErrorCode errorCode, string message)
        {
            _driverException = new RegistrationException(correlationId, codeValue, errorCode, message);

            var resource = _resourceByRegIdMap[correlationId];

            if (resource is Subscription subscription)
            {
                subscription.InternalClose();
                _resourceByRegIdMap.Remove(correlationId);
            }
        }

        public void OnAsyncError(long correlationId, int codeValue, ErrorCode errorCode, string message)
        {
            HandleError(new RegistrationException(correlationId, codeValue, errorCode, message));
        }

        public void OnChannelEndpointError(int statusIndicatorId, string message)
        {
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
        }

        public void OnNewPublication(
            long correlationId,
            long registrationId,
            int streamId,
            int sessionId,
            int publicationLimitId,
            int statusIndicatorId,
            string logFileName)
        {
            var publication = new ConcurrentPublication(
                this,
                _stashedChannel,
                streamId,
                sessionId,
                new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId),
                statusIndicatorId,
                LogBuffers(registrationId, logFileName, _stashedChannel),
                registrationId,
                correlationId
            );

            _resourceByRegIdMap[correlationId] = publication;
        }

        public void OnNewExclusivePublication(
            long correlationId,
            long registrationId,
            int streamId,
            int sessionId,
            int publicationLimitId,
            int statusIndicatorId,
            string logFileName)
        {
            var publication = new ExclusivePublication(
                this,
                _stashedChannel,
                streamId,
                sessionId,
                new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId),
                statusIndicatorId,
                LogBuffers(registrationId, logFileName, _stashedChannel),
                registrationId,
                correlationId
            );

            _resourceByRegIdMap[correlationId] = publication;
        }

        public void OnNewSubscription(long correlationId, int statusIndicatorId)
        {
            Subscription subscription = (Subscription) _resourceByRegIdMap[correlationId];
            subscription.ChannelStatusId = statusIndicatorId;
        }

        public void OnAvailableImage(
            long correlationId,
            int sessionId,
            long subscriptionRegistrationId,
            int subscriberPositionId,
            string logFileName,
            string sourceIdentity)
        {
            Subscription subscription = (Subscription) _resourceByRegIdMap[subscriptionRegistrationId];
            if (null != subscription)
            {
                Image image = new Image(subscription, sessionId,
                    new UnsafeBufferPosition(_counterValuesBuffer, subscriberPositionId),
                    LogBuffers(correlationId, logFileName, subscription.Channel), _ctx.ErrorHandler(), sourceIdentity,
                    correlationId);

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

                subscription.AddImage(image);
            }
        }

        public void OnUnavailableImage(long correlationId, long subscriptionRegistrationId)
        {
            Subscription subscription = (Subscription) _resourceByRegIdMap[subscriptionRegistrationId];
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

        public void OnNewCounter(long correlationId, int counterId)
        {
            _resourceByRegIdMap[correlationId] = new Counter(correlationId, this, _counterValuesBuffer, counterId);
            OnAvailableCounter(correlationId, counterId);
        }

        public void OnAvailableCounter(long registrationId, int counterId)
        {
            for (int i = 0; i < _availableCounterHandlers.Count; i++)
            {
                var handler = _availableCounterHandlers[i];
                NotifyCounterAvailable(registrationId, counterId, handler);
            }
        }

        public void OnUnavailableCounter(long registrationId, int counterId)
        {
            NotifyUnavailableCounterHandlers(registrationId, counterId);
        }

        public void OnClientTimeout()
        {
            if (!_isClosed)
            {
                _isTerminating = true;
                ForceCloseResources();
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
                _ctx.ErrorHandler()(ex);
            }
        }

        internal ConcurrentPublication AddPublication(string channel, int streamId)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();

                _stashedChannel = channel;
                long registrationId = _driverProxy.AddPublication(channel, streamId);
                AwaitResponse(registrationId);

                return
                    (ConcurrentPublication) _resourceByRegIdMap[
                        registrationId]; // TODO dictionary semantics if non-existant
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

                _stashedChannel = channel;
                long registrationId = _driverProxy.AddExclusivePublication(channel, streamId);
                AwaitResponse(registrationId);

                return (ExclusivePublication) _resourceByRegIdMap[registrationId];
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void ReleasePublication(Publication publication)
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

                    var removedPublication = _resourceByRegIdMap[publication.RegistrationId];

                    if (_resourceByRegIdMap.Remove(publication.RegistrationId) && publication == removedPublication)
                    {
                        ReleaseLogBuffers(publication.LogBuffers, publication.OriginalRegistrationId);
                        _asyncCommandIdSet.Add(_driverProxy.RemovePublication(publication.RegistrationId));
                    }
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

                _resourceByRegIdMap[correlationId] = subscription;
                AwaitResponse(correlationId);

                return subscription;
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void ReleaseSubscription(Subscription subscription)
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

                    subscription.InternalClose();

                    long registrationId = subscription.RegistrationId;
                    var resource = _resourceByRegIdMap[registrationId];
                    _resourceByRegIdMap.Remove(registrationId);

                    if (subscription == resource)
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

                return _asyncCommandIdSet.Contains(correlationId);
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

                return (Counter) _resourceByRegIdMap[registrationId];
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

                return (Counter) _resourceByRegIdMap[registrationId];
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void AddAvailableCounterHandler(AvailableCounterHandler handler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();
                _availableCounterHandlers.Add(handler);
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

                return _availableCounterHandlers.Remove(handler);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void AddUnavailableCounterHandler(UnavailableCounterHandler handler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();
                _unavailableCounterHandlers.Add(handler);
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

                return _unavailableCounterHandlers.Remove(handler);
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void AddCloseHandler(Action handler)
        {
            _clientLock.Lock();
            try
            {
                EnsureActive();
                EnsureNotReentrant();
                _closeHandlers.Add(handler);
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

                return _closeHandlers.Remove(handler);
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


                var resource = _resourceByRegIdMap[registrationId];
                _resourceByRegIdMap.Remove(registrationId);
                if (counter == resource)
                {
                    _asyncCommandIdSet.Add(_driverProxy.RemoveCounter(registrationId));
                }
            }
            finally
            {
                _clientLock.Unlock();
            }
        }

        internal void ReleaseLogBuffers(LogBuffers logBuffers, long registrationId)
        {
            if (logBuffers.DecRef() == 0)
            {
                logBuffers.TimeOfLastStateChange(_nanoClock.NanoTime());
                _logBuffersByIdMap.Remove(registrationId);
                _lingeringResources.Add(logBuffers);
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

        internal void CloseImages(Image[] images, UnavailableImageHandler unavailableImageHandler)
        {
            foreach (var image in images)
            {
                image.Close();
                ReleaseLogBuffers(image.LogBuffers, image.CorrelationId);
            }

            if (null != unavailableImageHandler)
            {
                foreach (var image in images)
                {
                    NotifyImageUnavailable(unavailableImageHandler, image);
                }
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
            LogBuffers logBuffers = _logBuffersByIdMap[registrationId];
            if (null == logBuffers)
            {
                logBuffers = _logBuffersFactory.Map(logFileName);
                _logBuffersByIdMap[registrationId] = logBuffers;

                if (_ctx.PreTouchMappedMemory() && !channel.Contains("sparse=true"))
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
                var nowNs = _nanoClock.NanoTime();
                workCount += CheckTimeouts(nowNs);
                workCount += _driverEventsAdapter.Receive(correlationId);
            }
            catch (Exception throwable)
            {
                HandleError(throwable);

                if (_driverEventsAdapter.IsInvalid)
                {
                    _isTerminating = true;
                    ForceCloseResources();

                    if (!IsClientApiCall(correlationId))
                    {
                        throw new InvalidOperationException("Driver events adapter is invalid");
                    }
                }

                if (IsClientApiCall(correlationId))
                {
                    throw;
                }
            }

            return workCount;
        }

        private static bool IsClientApiCall(long correlationId)
        {
            return correlationId != NO_CORRELATION_ID;
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
                    _stashedChannel = null;

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
                    _isTerminating = true;
                    throw new AgentTerminationException("thread interrupted");
                }
            } while (deadlineNs - _nanoClock.NanoTime() > 0);

            throw new DriverTimeoutException("no response from MediaDriver within (ms):" + _driverTimeoutMs);
        }

        private int CheckTimeouts(long nowNs)
        {
            int workCount = 0;

            if ((_timeOfLastServiceNs + Aeron.Configuration.IdleSleepNs) - nowNs < 0)
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
                _isTerminating = true;
                ForceCloseResources();

                throw new ConductorServiceTimeoutException("service interval exceeded (ns): timeout=" +
                                                           _interServiceTimeoutNs + ", actual=" +
                                                           (nowNs - _timeOfLastServiceNs));
            }
        }

        private int CheckLiveness(long nowNs)
        {
            if ((_timeOfLastKeepAliveNs + _keepAliveIntervalNs) - nowNs < 0)
            {
                long lastKeepAliveMs = _driverProxy.TimeOfLastDriverKeepaliveMs();
                var nowMs = _epochClock.Time();
                
                if (nowMs > (lastKeepAliveMs + _driverTimeoutMs))
                {
                    _isTerminating = true;
                    ForceCloseResources();

                    throw new DriverTimeoutException("MediaDriver keepalive age exceeded (ms): timeout= " +
                                                     _driverTimeoutMs + ", actual=" + (nowMs - lastKeepAliveMs));
                }

                if (null == _heartbeatTimestamp)
                {
                    int counterId = HeartbeatTimestamp.FindCounterIdByRegistrationId(_countersReader, HeartbeatTimestamp.CLIENT_HEARTBEAT_TYPE_ID, _ctx.ClientId());

                    if (counterId != Agrona.Concurrent.Status.CountersReader.NULL_COUNTER_ID)
                    {
                        _heartbeatTimestamp = new AtomicCounter(_counterValuesBuffer, counterId);
                        _heartbeatTimestamp.SetOrdered(nowMs);
                        _timeOfLastKeepAliveNs = nowNs;
                    }
                }
                else
                {
                    int counterId = _heartbeatTimestamp.Id;
                    if (!HeartbeatTimestamp.IsActive(_countersReader, counterId, HeartbeatTimestamp.CLIENT_HEARTBEAT_TYPE_ID, _ctx.ClientId()))
                    {
                        _isTerminating = true;
                        ForceCloseResources();

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

            var lingeringResources = _lingeringResources;

            for (int lastIndex = lingeringResources.Count - 1, i = lastIndex; i >= 0; i--)
            {
                IManagedResource resource = lingeringResources[i];

                if ((resource.TimeOfLastStateChange() + _resourceLingerDurationNs) - nowNs < 0)
                {
                    ListUtil.FastUnorderedRemove(lingeringResources, i, lastIndex--);
                    DeleteResource(resource);
                    workCount++;
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
                    subscription.InternalClose();
                }
                else if (resource is Publication publication)
                {
                    publication.InternalClose();
                    ReleaseLogBuffers(publication.LogBuffers, publication.OriginalRegistrationId);
                }
                else if (resource is Counter counter)
                {
                    counter.InternalClose();
                    NotifyUnavailableCounterHandlers(counter.RegistrationId, counter.Id);
                }
            }

            _resourceByRegIdMap.Clear();
        }
        
        private void DeleteResource(IManagedResource resource)
        {
            try
            {
                resource.Delete();
            }
            catch (Exception throwable)
            {
                HandleError(throwable);
            }
        }

        private void NotifyUnavailableCounterHandlers(long registrationId, int counterId)
        {
            for (int i = 0, size = _unavailableCounterHandlers.Count; i < size; i++)
            {
                UnavailableCounterHandler handler = _unavailableCounterHandlers[i];
                _isInCallback = true;
                try
                {
                    handler(_countersReader, registrationId, counterId);
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
            for (int i = _closeHandlers.Count - 1; i >= 0; i--)
            {
                _isInCallback = true;
                try
                {
                    _closeHandlers[i]();
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
}