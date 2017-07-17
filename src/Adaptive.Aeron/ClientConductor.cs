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
using System.Threading;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.io.aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Client conductor takes responses and notifications from Media Driver and acts on them in addition to forwarding
    /// commands from the various Client APIs to the Media Driver.
    /// </summary>
    internal class ClientConductor : IAgent, IDriverListener
    {
        public enum ClientConductorStatus
        {
            ACTIVE,
            CLOSING,
            CLOSED
        }

        private const long NO_CORRELATION_ID = -1;
        private static readonly long RESOURCE_TIMEOUT_NS = 1;
        private static readonly long RESOURCE_LINGER_NS = 3;

        private readonly long _keepAliveIntervalNs;
        private readonly long _driverTimeoutMs;
        private readonly long _driverTimeoutNs;
        private readonly long _interServiceTimeoutNs;
        private readonly long _publicationConnectionTimeoutMs;
        private long _timeOfLastKeepaliveNs;
        private long _timeOfLastCheckResourcesNs;
        private long _timeOfLastWorkNs;
        private bool _isDriverActive = true;
        private volatile ClientConductorStatus _status = ClientConductorStatus.ACTIVE;

        private readonly ILock _clientLock;
        private readonly Aeron.Context _ctx;
        private readonly IEpochClock _epochClock;
        private readonly MapMode _imageMapMode;
        private readonly INanoClock _nanoClock;
        private readonly DriverListenerAdapter _driverListener;
        private readonly ILogBuffersFactory _logBuffersFactory;
        private readonly ActivePublications _activePublications = new ActivePublications();
        private readonly ConcurrentDictionary<long, ExclusivePublication> _activeExclusivePublications = new ConcurrentDictionary<long, ExclusivePublication>();
        private readonly ActiveSubscriptions _activeSubscriptions = new ActiveSubscriptions();
        private readonly List<IManagedResource> _lingeringResources = new List<IManagedResource>();
        private readonly UnsafeBuffer _counterValuesBuffer;
        private readonly DriverProxy _driverProxy;
        private readonly ErrorHandler _errorHandler;
        private readonly AgentInvoker _driverAgentInvoker;

        private RegistrationException _driverException;

        internal ClientConductor()
        {
            
        }

        internal ClientConductor(Aeron.Context ctx)
        {
            _ctx = ctx;

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
            _driverListener = new DriverListenerAdapter(ctx.ToClientBuffer(), this);
            _driverAgentInvoker = ctx.DriverAgentInvoker();

            long nowNs = _nanoClock.NanoTime();
            _timeOfLastKeepaliveNs = nowNs;
            _timeOfLastCheckResourcesNs = nowNs;
            _timeOfLastWorkNs = nowNs;
        }

        public void OnClose()
        {
            if (ClientConductorStatus.ACTIVE == _status)
            {
                _status = ClientConductorStatus.CLOSING;

                foreach (ExclusivePublication publication in _activeExclusivePublications.Values)
                {
                    publication.ForceClose();
                }
                _activeExclusivePublications.Clear();

                _activePublications.Dispose();
                _activeSubscriptions.Dispose();

                Thread.Yield();

                for (int i = 0, size = _lingeringResources.Count; i < size; i++)
                {
                    _lingeringResources[i].Delete();
                }
                _lingeringResources.Clear();

                _ctx.Dispose();

                _status = ClientConductorStatus.CLOSED;
            }
        }

        public int DoWork()
        {
            int workCount = 0;

            if (_clientLock.TryLock())
            {
                try
                {
                    if (ClientConductorStatus.ACTIVE == _status)
                    {
                        workCount = DoWork(NO_CORRELATION_ID, null);
                    }
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

        public Aeron.Context Context()
        {
            return _ctx;
        }

        public ClientConductorStatus Status => _status;

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
            VerifyActive();

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
            VerifyActive();

            long registrationId = _driverProxy.AddExclusivePublication(channel, streamId);
            AwaitResponse(registrationId, channel);

            return _activeExclusivePublications[registrationId];
        }

        internal virtual void ReleasePublication(Publication publication)
        {
            VerifyActive();

            if (publication == _activePublications.Remove(publication.Channel, publication.StreamId))
            {
                LingerResource(publication.ManagedResource());
                AwaitResponse(_driverProxy.RemovePublication(publication.RegistrationId), null);
            }
        }

        internal void ReleasePublication(ExclusivePublication publication)
        {
            VerifyActive();

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
            VerifyActive();

            long correlationId = _driverProxy.AddSubscription(channel, streamId);
            Subscription subscription = new Subscription(this, channel, streamId, correlationId, _ctx.AvailableImageHandler(), _ctx.unavailableImageHandler());
            _activeSubscriptions.Add(subscription);

            AwaitResponse(correlationId, channel);

            return subscription;
        }

        internal Subscription AddSubscription(string channel, int streamId, AvailableImageHandler availableImageHandler, UnavailableImageHandler unavailableImageHandler)
        {
            VerifyActive();

            long correlationId = _driverProxy.AddSubscription(channel, streamId);
            Subscription subscription = new Subscription(this, channel, streamId, correlationId, availableImageHandler, unavailableImageHandler);
            _activeSubscriptions.Add(subscription);

            AwaitResponse(correlationId, channel);

            return subscription;
        }

        internal virtual void ReleaseSubscription(Subscription subscription)
        {
            VerifyActive();

            AwaitResponse(_driverProxy.RemoveSubscription(subscription.RegistrationId), null);

            _activeSubscriptions.Remove(subscription);
        }

        internal void AsyncReleaseSubscription(Subscription subscription)
        {
            _driverProxy.RemoveSubscription(subscription.RegistrationId);
        }

        internal void AddDestination(long registrationId, string endpointChannel)
        {
            VerifyActive();

            AwaitResponse(_driverProxy.AddDestination(registrationId, endpointChannel), null);
        }

        internal void RemoveDestination(long registrationId, string endpointChannel)
        {
            VerifyActive();

            AwaitResponse(_driverProxy.RemoveDestination(registrationId, endpointChannel), null);
        }

        public void OnError(ErrorCode errorCode, string message, long correlationId)
        {
            _driverException = new RegistrationException(errorCode, message);
        }

        public void OnNewPublication(string channel, int streamId, int sessionId, int publicationLimitId, string logFileName, long correlationId)
        {
            Publication publication = new Publication(this, channel, streamId, sessionId, new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId), _logBuffersFactory.Map(logFileName, MapMode.ReadWrite), correlationId);

            _activePublications.Put(channel, streamId, publication);
        }

        public void OnNewExclusivePublication(string channel, int streamId, int sessionId, int publicationLimitId, string logFileName, long correlationId)
        {
            ExclusivePublication publication = new ExclusivePublication(this, channel, streamId, sessionId, new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId), _logBuffersFactory.Map(logFileName, MapMode.ReadWrite), correlationId);

            _activeExclusivePublications[correlationId] = publication;
        }

        public void OnAvailableImage(int streamId, int sessionId, IDictionary<long, long> subscriberPositionMap, string logFileName, string sourceIdentity, long correlationId)
        {
            _activeSubscriptions.ForEach(streamId, (subscription) =>
            {
                if (!subscription.HasImage(correlationId))
                {
                    long positionId = subscriberPositionMap[subscription.RegistrationId];
                    if (Adaptive.Aeron.DriverListenerAdapter.MISSING_REGISTRATION_ID != positionId)
                    {
                        Image image = new Image(subscription, sessionId, new UnsafeBufferPosition(_counterValuesBuffer, (int)positionId), _logBuffersFactory.Map(logFileName, _imageMapMode), _errorHandler, sourceIdentity, correlationId);
                        
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
                }
            });
        }

        public void OnUnavailableImage(int streamId, long correlationId)
        {
            _activeSubscriptions.ForEach(streamId, (subscription) =>
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

        internal DriverListenerAdapter DriverListenerAdapter()
        {
            return _driverListener;
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

        private int DoWork(long correlationId, string expectedChannel)
        {
            int workCount = 0;

            try
            {
                workCount += OnCheckTimeouts();
                workCount += _driverListener.PollMessage(correlationId, expectedChannel);
            }
            catch (Exception throwable)
            {
                _errorHandler(throwable);

                if (correlationId != NO_CORRELATION_ID)
                {
                    // has been called from a user thread and not the conductor duty cycle.
                    throw;
                }
            }

            return workCount;
        }

        private void AwaitResponse(long correlationId, string expectedChannel) 
        {
            _driverException = null;
            var deadlineNs = _nanoClock.NanoTime() + _driverTimeoutNs;

            do
            {
                if (null == _driverAgentInvoker)
                {
                    Aeron.Sleep(1);
                }
                else
                {
                    _driverAgentInvoker.Invoke();
                }

                DoWork(correlationId, expectedChannel);

                if (_driverListener.LastReceivedCorrelationId() == correlationId)
                {
                    if (null != _driverException)
                    {
                        throw _driverException;
                    }

                    return;
                }
            } while (_nanoClock.NanoTime() < deadlineNs);

            throw new DriverTimeoutException("No response from driver wihtout timeout");
        }

        private void VerifyActive()
        {
            if (!_isDriverActive)
            {
                throw new DriverTimeoutException("MediaDriver is inactive");
            }

            if (ClientConductorStatus.CLOSED == _status)
            {
                throw new InvalidOperationException("Aeron client is closed");
            }
        }

        private int OnCheckTimeouts()
        {
            int workCount = 0;
            long nowNs = _nanoClock.NanoTime();

            if (nowNs < (_timeOfLastWorkNs + Aeron.IdleSleepNs))
            {
                return workCount;
            }

            if (nowNs > (_timeOfLastWorkNs + _interServiceTimeoutNs))
            {
                OnClose();

                throw new ConductorServiceTimeoutException("Timeout between service calls over " + _interServiceTimeoutNs + "ns");
            }

            _timeOfLastWorkNs = nowNs;

            if (nowNs > (_timeOfLastKeepaliveNs + _keepAliveIntervalNs))
            {
                _driverProxy.SendClientKeepalive();
                CheckDriverHeartbeat();

                _timeOfLastKeepaliveNs = nowNs;
                workCount++;
            }

            if (nowNs > (_timeOfLastCheckResourcesNs + RESOURCE_TIMEOUT_NS))
            {
                List<IManagedResource> lingeringResources = this._lingeringResources;
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

                _timeOfLastCheckResourcesNs = nowNs;
                workCount++;
            }

            return workCount;
        }

        private void CheckDriverHeartbeat()
        {
            long deadlineMs = _driverProxy.TimeOfLastDriverKeepaliveMs() + _driverTimeoutMs;
            if (_isDriverActive && (_epochClock.Time() > deadlineMs))
            {
                _isDriverActive = false;
                _errorHandler(new DriverTimeoutException("MediaDriver has been inactive for over " + _driverTimeoutMs + "ms"));
            }
        }
    }
}

public interface ILock
{
    void Lock();
    void Unlock();
    bool TryLock();
}

public class ReentrantLock : ILock
{
    private readonly object _lockObj = new object();

    public void Lock()
    {
        Monitor.Enter(_lockObj);
    }

    public void Unlock()
    {
        Monitor.Exit(_lockObj);
    }

    public bool TryLock()
    {
        return Monitor.TryEnter(_lockObj);
    }
}