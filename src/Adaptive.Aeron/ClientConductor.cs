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
using System.Collections.Concurrent;
using System.Collections.Generic;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
    /// commands from the Client API to the Media Driver conductor.
    /// </summary>
    internal class ClientConductor : IAgent, IDriverEventsListener
    {
        private const long NO_CORRELATION_ID = -1;
        private static readonly long RESOURCE_TIMEOUT_NS = 1;
        private static readonly long RESOURCE_LINGER_NS = 3;

        private readonly long _keepAliveIntervalNs;
        private readonly long _driverTimeoutMs;
        private readonly long _driverTimeoutNs;
        private readonly long _interServiceTimeoutNs;
        private readonly long _publicationConnectionTimeoutMs;
        private long _timeOfLastKeepAliveNs;
        private long _timeOfLastResourcesCheckNs;
        private long _timeOfLastServiceNs;
        private volatile bool _isClosed;

        private readonly ILock _clientLock;
        private readonly IEpochClock _epochClock;
        private readonly MapMode _imageMapMode;
        private readonly INanoClock _nanoClock;
        private readonly DriverEventsAdapter _driverEventsAdapter;
        private readonly ILogBuffersFactory _logBuffersFactory;
        private readonly ActivePublications _activePublications = new ActivePublications();
        private readonly ConcurrentDictionary<long, ExclusivePublication> _activeExclusivePublications = new ConcurrentDictionary<long, ExclusivePublication>();
        private readonly ActiveSubscriptions _activeSubscriptions = new ActiveSubscriptions();
        private readonly List<IManagedResource> _lingeringResources = new List<IManagedResource>();
        private readonly UnavailableImageHandler _defaultUnavailableImageHandler;
        private readonly AvailableImageHandler _defaultAvailableImageHandler;
        private readonly UnsafeBuffer _counterValuesBuffer;
        private readonly DriverProxy _driverProxy;
        private readonly ErrorHandler _errorHandler;

        private RegistrationException _driverException;

        internal ClientConductor()
        {
        }

        internal ClientConductor(Aeron.Context ctx)
        {
            _clientLock = ctx.ClientLock();
            _epochClock = ctx.EpochClock();
            _nanoClock = ctx.NanoClock();
            _errorHandler = ctx.ErrorHandler();
            _counterValuesBuffer = ctx.CountersValuesBuffer();
            _driverProxy = ctx.DriverProxy();
            _logBuffersFactory = ctx.LogBuffersFactory();
            _imageMapMode = ctx.ImageMapMode();
            _keepAliveIntervalNs = ctx.KeepAliveInterval();
            _driverTimeoutMs = ctx.DriverTimeoutMs();
            _driverTimeoutNs = _driverTimeoutMs * 1000000;
            _interServiceTimeoutNs = ctx.InterServiceTimeout();
            _publicationConnectionTimeoutMs = ctx.PublicationConnectionTimeout();
            _defaultAvailableImageHandler = ctx.AvailableImageHandler();
            _defaultUnavailableImageHandler = ctx.UnavailableImageHandler();
            _driverEventsAdapter = new DriverEventsAdapter(ctx.ToClientBuffer(), this);

            long nowNs = _nanoClock.NanoTime();
            _timeOfLastKeepAliveNs = nowNs;
            _timeOfLastResourcesCheckNs = nowNs;
            _timeOfLastServiceNs = nowNs;
        }

        public void OnStart()
        {
            // Do Nothing
        }

        public void OnClose()
        {
            if (!_isClosed)
            {
                _isClosed = true;

                int lingeringResourcesSize = _lingeringResources.Count;
                ForceClosePublicationsAndSubscriptions();

                if (_lingeringResources.Count > lingeringResourcesSize)
                {
                    Aeron.Sleep(1);
                }

                for (int i = 0, size = _lingeringResources.Count; i < size; i++)
                {
                    _lingeringResources[i].Delete();
                }

                _lingeringResources.Clear();
            }
        }

        public int DoWork()
        {
            int workCount = 0;

            if (_clientLock.TryLock())
            {
                try
                {
                    if (_isClosed)
                    {
                        throw new AgentTerminationException();
                    }

                    workCount = Service(NO_CORRELATION_ID, null);
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

        public bool IsClosed()
        {
            return _isClosed;
        }

        internal virtual ILock ClientLock()
        {
            return _clientLock;
        }

        internal void HandleError(Exception ex)
        {
            _errorHandler(ex);
        }

        internal Publication AddPublication(string channel, int streamId)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            Publication publication = _activePublications.Get(channel, streamId);
            if (null == publication)
            {
                AwaitResponse(_driverProxy.AddPublication(channel, streamId), channel);
                publication = _activePublications.Get(channel, streamId);
            }

            publication.IncRef();

            return publication;
        }

        internal ExclusivePublication AddExclusivePublication(string channel, int streamId)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            long registrationId = _driverProxy.AddExclusivePublication(channel, streamId);
            AwaitResponse(registrationId, channel);

            return _activeExclusivePublications[registrationId];
        }

        internal virtual void ReleasePublication(Publication publication)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            if (publication == _activePublications.Remove(publication.Channel, publication.StreamId))
            {
                LingerResource(publication.ManagedResource());
                AwaitResponse(_driverProxy.RemovePublication(publication.RegistrationId), null);
            }
        }

        internal void ReleasePublication(ExclusivePublication publication)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            ExclusivePublication publicationToRemove;

            _activeExclusivePublications.TryRemove(publication.RegistrationId, out publicationToRemove);

            if (publication == publicationToRemove)
            {
                LingerResource(publication.ManagedResource());
                AwaitResponse(_driverProxy.RemovePublication(publication.RegistrationId), null);
            }
        }

        internal void AsyncReleasePublication(long registrationId)
        {
            _driverProxy.RemovePublication(registrationId);
        }

        internal Subscription AddSubscription(string channel, int streamId)
        {
            return AddSubscription(channel, streamId, _defaultAvailableImageHandler, _defaultUnavailableImageHandler);
        }

        internal Subscription AddSubscription(string channel, int streamId, AvailableImageHandler availableImageHandler, UnavailableImageHandler unavailableImageHandler)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            long correlationId = _driverProxy.AddSubscription(channel, streamId);
            Subscription subscription = new Subscription(this, channel, streamId, correlationId, availableImageHandler, unavailableImageHandler);
            _activeSubscriptions.Add(subscription);

            AwaitResponse(correlationId, channel);

            return subscription;
        }

        internal virtual void ReleaseSubscription(Subscription subscription)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            AwaitResponse(_driverProxy.RemoveSubscription(subscription.RegistrationId), null);

            _activeSubscriptions.Remove(subscription);
        }

        internal void AsyncReleaseSubscription(Subscription subscription)
        {
            _driverProxy.RemoveSubscription(subscription.RegistrationId);
        }

        internal void AddDestination(long registrationId, string endpointChannel)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            AwaitResponse(_driverProxy.AddDestination(registrationId, endpointChannel), null);
        }

        internal void RemoveDestination(long registrationId, string endpointChannel)
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }

            AwaitResponse(_driverProxy.RemoveDestination(registrationId, endpointChannel), null);
        }

        public void OnError(long correlationId, ErrorCode errorCode, string message)
        {
            _driverException = new RegistrationException(errorCode, message);
        }

        public void OnNewPublication(
            long correlationId,
            long registrationId,
            int streamId,
            int sessionId,
            int publicationLimitId,
            string channel,
            string logFileName)
        {
            Publication publication = new Publication(
                this,
                channel,
                streamId,
                sessionId,
                new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId),
                _logBuffersFactory.Map(logFileName, MapMode.ReadWrite),
                registrationId,
                correlationId);

            _activePublications.Put(channel, streamId, publication);
        }

        public void OnNewExclusivePublication(
            long correlationId,
            long registrationid,
            int streamId,
            int sessionId,
            int publicationLimitId,
            string channel,
            string logFileName)
        {
            ExclusivePublication publication = new ExclusivePublication(
                this,
                channel,
                streamId,
                sessionId,
                new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId),
                _logBuffersFactory.Map(logFileName, MapMode.ReadWrite),
                registrationid,
                correlationId);

            _activeExclusivePublications[correlationId] = publication;
        }

        public void OnAvailableImage(
            long correlationId,
            int streamId,
            int sessionId,
            long subscriberRegistrationId,
            int subscriberPositionId,
            string logFileName,
            string sourceIdentity)
        {
            _activeSubscriptions.ForEach(
                streamId,
                subscription =>
                {
                    if (subscription.RegistrationId == subscriberRegistrationId && !subscription.HasImage(correlationId))
                    {
                        Image image = new Image(
                            subscription,
                            sessionId,
                            new UnsafeBufferPosition(_counterValuesBuffer, (int) subscriberPositionId),
                            _logBuffersFactory.Map(logFileName, _imageMapMode),
                            _errorHandler,
                            sourceIdentity,
                            correlationId);

                        try
                        {
                            AvailableImageHandler handler = subscription.AvailableImageHandler();
                            if (null != handler)
                            {
                                handler(image);
                            }
                        }
                        catch (Exception ex)
                        {
                            _errorHandler(ex);
                        }

                        subscription.AddImage(image);
                    }
                });
        }

        public void OnUnavailableImage(long correlationId, int streamId)
        {
            _activeSubscriptions.ForEach(streamId,
                (subscription) =>
                {
                    Image image = subscription.RemoveImage(correlationId);
                    if (null != image)
                    {
                        try
                        {
                            UnavailableImageHandler handler = subscription.UnavailableImageHandler();
                            if (null != handler)
                            {
                                handler(image);
                            }
                        }
                        catch (Exception ex)
                        {
                            _errorHandler(ex);
                        }
                    }
                });
        }

        internal DriverEventsAdapter DriverListenerAdapter()
        {
            return _driverEventsAdapter;
        }

        internal void LingerResource(IManagedResource managedResource)
        {
            managedResource.TimeOfLastStateChange(_nanoClock.NanoTime());
            _lingeringResources.Add(managedResource);
        }

        internal virtual bool IsPublicationConnected(long timeOfLastStatusMessageMs)
        {
            return _epochClock.Time() <= (timeOfLastStatusMessageMs + _publicationConnectionTimeoutMs);
        }

        private int Service(long correlationId, string expectedChannel)
        {
            int workCount = 0;

            try
            {
                workCount += OnCheckTimeouts();
                workCount += _driverEventsAdapter.Receive(correlationId, expectedChannel);
            }
            catch (Exception throwable)
            {
                _errorHandler(throwable);

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

        private void AwaitResponse(long correlationId, string expectedChannel)
        {
            _driverException = null;
            var deadlineNs = _nanoClock.NanoTime() + _driverTimeoutNs;

            do
            {
                Aeron.Sleep(1);

                Service(correlationId, expectedChannel);

                if (_driverEventsAdapter.LastReceivedCorrelationId() == correlationId)
                {
                    if (null != _driverException)
                    {
                        throw _driverException;
                    }

                    return;
                }
            } while (_nanoClock.NanoTime() < deadlineNs);

            throw new DriverTimeoutException("No response from MediaDriver within (ms):" + _driverTimeoutMs);
        }

        private int OnCheckTimeouts()
        {
            int workCount = 0;
            long nowNs = _nanoClock.NanoTime();

            if (nowNs > (_timeOfLastServiceNs + Aeron.IdleSleepNs))
            {
                checkServiceInterval(nowNs);
                _timeOfLastServiceNs = nowNs;

                workCount += checkLiveness(nowNs);
                workCount += checkLingeringResources(nowNs);
            }

            return workCount;
        }

        private void checkServiceInterval(long nowNs)
        {
            if (nowNs > (_timeOfLastServiceNs + _interServiceTimeoutNs))
            {
                int lingeringResourcesSize = _lingeringResources.Count;

                ForceClosePublicationsAndSubscriptions();

                if (_lingeringResources.Count > lingeringResourcesSize)
                {
                    Aeron.Sleep(1000);
                }

                OnClose();

                throw new ConductorServiceTimeoutException("Exceeded (ns): " + _interServiceTimeoutNs);
            }
        }

        private int checkLiveness(long nowNs)
        {
            if (nowNs > (_timeOfLastKeepAliveNs + _keepAliveIntervalNs))
            {
                if (_epochClock.Time() > (_driverProxy.TimeOfLastDriverKeepaliveMs() + _driverTimeoutMs))
                {
                    OnClose();

                    throw new DriverTimeoutException("MediaDriver keepalive older than (ms): " + _driverTimeoutMs);
                }

                _driverProxy.SendClientKeepalive();
                _timeOfLastKeepAliveNs = nowNs;

                return 1;
            }

            return 0;
        }

        private int checkLingeringResources(long nowNs)
        {
            if (nowNs > (_timeOfLastResourcesCheckNs + RESOURCE_TIMEOUT_NS))
            {
                List<IManagedResource> lingeringResources = _lingeringResources;
                for (int lastIndex = lingeringResources.Count - 1, i = lastIndex; i >= 0; i--)
                {
                    IManagedResource resource = lingeringResources[i];

                    if (nowNs > (resource.TimeOfLastStateChange() + RESOURCE_LINGER_NS))
                    {
                        ListUtil.FastUnorderedRemove(lingeringResources, i, lastIndex);
                        lastIndex--;
                        resource.Delete();
                    }
                }

                _timeOfLastResourcesCheckNs = nowNs;

                return 1;
            }

            return 0;
        }

        private void ForceClosePublicationsAndSubscriptions()
        {
            foreach (ExclusivePublication publication in _activeExclusivePublications.Values)
            {
                publication.ForceClose();
            }

            _activeExclusivePublications.Clear();

            _activePublications.Dispose();
            _activeSubscriptions.Dispose();
        }
    }
}