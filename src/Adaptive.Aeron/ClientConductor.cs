using System;
using System.Collections.Generic;
using System.Threading;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.io.aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Agrona.Util;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Client conductor takes responses and notifications from media driver and acts on them.
    /// As well as passes commands to the media driver.
    /// </summary>
    public class ClientConductor : IAgent, IDriverListener
    {
        private const long NoCorrelationID = -1;
        private static readonly long ResourceTimeoutNs = NanoUtil.FromSeconds(1);
        private static readonly long ResourceLingerNs = NanoUtil.FromSeconds(5);

        private readonly long _keepAliveIntervalNs;
        private readonly long _driverTimeoutMs;
        private readonly long _driverTimeoutNs;
        private readonly long _interServiceTimeoutNs;
        private readonly long _publicationConnectionTimeoutMs;
        private long _timeOfLastKeepalive;
        private long _timeOfLastCheckResources;
        private long _timeOfLastWork;
        private volatile bool _driverActive = true;

        private readonly IEpochClock _epochClock;
        private readonly MapMode _imageMapMode;
        private readonly INanoClock _nanoClock;
        private readonly DriverListenerAdapter _driverListener;
        private readonly ILogBuffersFactory _logBuffersFactory;
        private readonly ActivePublications _activePublications = new ActivePublications();
        private readonly ActiveSubscriptions _activeSubscriptions = new ActiveSubscriptions();
        private readonly List<IManagedResource> _lingeringResources = new List<IManagedResource>();
        private readonly UnsafeBuffer _counterValuesBuffer;
        private readonly DriverProxy _driverProxy;
        private readonly ErrorHandler _errorHandler;
        private readonly AvailableImageHandler _availableImageHandler;
        private readonly UnavailableImageHandler _unavailableImageHandler;

        private RegistrationException _driverException;

        internal ClientConductor()
        {
        }

        internal ClientConductor(
            IEpochClock epochClock,
            INanoClock nanoClock,
            CopyBroadcastReceiver broadcastReceiver,
            ILogBuffersFactory logBuffersFactory,
            UnsafeBuffer counterValuesBuffer,
            DriverProxy driverProxy,
            ErrorHandler errorHandler,
            AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler,
            MapMode imageMapMode,
            long keepAliveIntervalNs,
            long driverTimeoutMs,
            long interServiceTimeoutNs,
            long publicationConnectionTimeoutMs)
        {
            _epochClock = epochClock;
            _nanoClock = nanoClock;
            _timeOfLastKeepalive = nanoClock.NanoTime();
            _timeOfLastCheckResources = nanoClock.NanoTime();
            _timeOfLastWork = nanoClock.NanoTime();
            _errorHandler = errorHandler;
            _counterValuesBuffer = counterValuesBuffer;
            _driverProxy = driverProxy;
            _logBuffersFactory = logBuffersFactory;
            _availableImageHandler = availableImageHandler;
            _unavailableImageHandler = unavailableImageHandler;
            _imageMapMode = imageMapMode;
            _keepAliveIntervalNs = keepAliveIntervalNs;
            _driverTimeoutMs = driverTimeoutMs;
            _driverTimeoutNs = NanoUtil.FromMilliseconds(driverTimeoutMs);
            _interServiceTimeoutNs = interServiceTimeoutNs;
            _publicationConnectionTimeoutMs = publicationConnectionTimeoutMs;

            _driverListener = new DriverListenerAdapter(broadcastReceiver, this);
        }

        public void OnClose()
        {
            lock (this)
            {
                _activePublications.Dispose();
                _activeSubscriptions.Dispose();

                Thread.Yield();

                _lingeringResources.ForEach(mr => mr.Delete());
            }
        }

        public int DoWork()
        {
            if (!Monitor.TryEnter(this))
            {
                return 0;
            }

            try
            {
                return DoWork(NoCorrelationID, null);
            }
            finally
            {
                Monitor.Exit(this);
            }
        }

        public string RoleName()
        {
            return "aeron-client-conductor";
        }

        internal Publication AddPublication(string channel, int streamId)
        {
            VerifyDriverIsActive();

            var publication = _activePublications.Get(channel, streamId);
            if (publication == null)
            {
                AwaitResponse(_driverProxy.AddPublication(channel, streamId), channel);
                publication = _activePublications.Get(channel, streamId);
            }

            publication.IncRef();

            return publication;
        }

#if DEBUG
        internal virtual void ReleasePublication(Publication publication)
#else
        internal void ReleasePublication(Publication publication)
#endif
        {
            VerifyDriverIsActive();

            if (publication == _activePublications.Remove(publication.Channel, publication.StreamId))
            {
                LingerResource(publication.ManagedResource());
                AwaitResponse(_driverProxy.RemovePublication(publication.RegistrationId), publication.Channel);
            }
        }

        internal Subscription AddSubscription(string channel, int streamId)
        {
            VerifyDriverIsActive();

            var correlationId = _driverProxy.AddSubscription(channel, streamId);
                
            var subscription = new Subscription(this, channel, streamId, correlationId);
            _activeSubscriptions.Add(subscription);

            AwaitResponse(correlationId, channel);

            return subscription;
        }

#if DEBUG
        internal virtual void ReleaseSubscription(Subscription subscription)
#else
        internal void ReleaseSubscription(Subscription subscription)
#endif
        {
            VerifyDriverIsActive();

            AwaitResponse(_driverProxy.RemoveSubscription(subscription.RegistrationId), subscription.Channel);

            _activeSubscriptions.Remove(subscription);
        }

        public void OnNewPublication(string channel, int streamId, int sessionId, int publicationLimitId, string logFileName, long correlationId)
        {
            var publication = new Publication(this, channel, streamId, sessionId, new UnsafeBufferPosition(_counterValuesBuffer, publicationLimitId), _logBuffersFactory.Map(logFileName, MapMode.ReadWrite), correlationId);

            _activePublications.Put(channel, streamId, publication);
        }

        public void OnAvailableImage(int streamId, int sessionId, IDictionary<long, long> subscriberPositionMap, string logFileName, string sourceIdentity, long correlationId)
        {
            _activeSubscriptions.ForEach(streamId, (subscription) =>
            {
                if (!subscription.HasImage(correlationId))
                {
                    long positionId;

                    if (subscriberPositionMap.TryGetValue(subscription.RegistrationId, out positionId))
                    {
                        var image = new Image(subscription, sessionId, new UnsafeBufferPosition(_counterValuesBuffer, (int) positionId), _logBuffersFactory.Map(logFileName, _imageMapMode), _errorHandler, sourceIdentity, correlationId);
                        subscription.AddImage(image);
                        _availableImageHandler(image);
                    }
                }
            });
        }

        public void OnError(ErrorCode errorCode, string message, long correlationId)
        {
            _driverException = new RegistrationException(errorCode, message);
        }

        public void OnUnavailableImage(int streamId, long correlationId)
        {
            _activeSubscriptions.ForEach(streamId, (subscription) =>
            {
                var image = subscription.RemoveImage(correlationId);
                if (null != image)
                {
                    _unavailableImageHandler(image);
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

#if DEBUG
        internal virtual bool IsPublicationConnected(long timeOfLastStatusMessage)
#else
        internal bool IsPublicationConnected(long timeOfLastStatusMessage)
#endif
        {
            return _epochClock.Time() <= timeOfLastStatusMessage + _publicationConnectionTimeoutMs;
        }

        internal UnavailableImageHandler UnavailableImageHandler()
        {
            return _unavailableImageHandler;
        }

        private void CheckDriverHeartbeat()
        {
            var now = _epochClock.Time();
            var currentDriverKeepaliveTime = _driverProxy.TimeOfLastDriverKeepalive();

            if (_driverActive && (now > (currentDriverKeepaliveTime + _driverTimeoutMs)))
            {
                _driverActive = false;

                string msg = $"Driver has been inactive for over {_driverTimeoutMs:D}ms";
                _errorHandler(new DriverTimeoutException(msg));
            }
        }

        private void VerifyDriverIsActive()
        {
            if (!_driverActive)
            {
                throw new DriverTimeoutException("Driver is inactive");
            }
        }

        private int DoWork(long correlationId, string expectedChannel)
        {
            var workCount = 0;

            try
            {
                workCount += OnCheckTimeouts();
                workCount += _driverListener.PollMessage(correlationId, expectedChannel);
            }
            catch (Exception ex)
            {
                _errorHandler(ex);
            }

            return workCount;
        }

        private void AwaitResponse(long correlationId, string expectedChannel)
        {
            _driverException = null;
            var timeout = _nanoClock.NanoTime() + _driverTimeoutNs;

            do
            {
                LockSupport.ParkNanos(1);

                DoWork(correlationId, expectedChannel);

                if (_driverListener.LastReceivedCorrelationId() == correlationId)
                {
                    if (null != _driverException)
                    {
                        throw _driverException;
                    }

                    return;
                }
            } while (_nanoClock.NanoTime() < timeout);

            throw new DriverTimeoutException("No response within driver timeout");
        }

        private int OnCheckTimeouts()
        {
            var now = _nanoClock.NanoTime();
            var result = 0;

            if (now > (_timeOfLastWork + _interServiceTimeoutNs))
            {
                OnClose();

                throw new ConductorServiceTimeoutException($"Timeout between service calls over {_interServiceTimeoutNs:D}ns");
            }

            _timeOfLastWork = now;

            if (now > (_timeOfLastKeepalive + _keepAliveIntervalNs))
            {
                _driverProxy.SendClientKeepalive();
                CheckDriverHeartbeat();

                _timeOfLastKeepalive = now;
                result++;
            }

            if (now > _timeOfLastCheckResources + ResourceTimeoutNs)
            {
                var lingeringResources = _lingeringResources;
                for (int lastIndex = lingeringResources.Count - 1, i = lastIndex; i >= 0; i--)
                {
                    var resource = lingeringResources[i];
                    if (now > resource.TimeOfLastStateChange() + ResourceLingerNs)
                    {
                        ListUtil.FastUnorderedRemove(lingeringResources, i, lastIndex);
                        lastIndex--;
                        resource.Delete();
                    }
                }

                _timeOfLastCheckResources = now;
                result++;
            }

            return result;
        }
    }
}