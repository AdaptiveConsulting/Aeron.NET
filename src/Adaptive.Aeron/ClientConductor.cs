using System;
using System.Collections.Generic;
using System.Threading;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Client conductor takes responses and notifications from media driver and acts on them.
    /// As well as passes commands to the media driver.
    /// </summary>
    internal class ClientConductor : IAgent, IDriverListener
    {
        private const long NO_CORRELATION_ID = -1;
        private static readonly TimeSpan RESOURCE_TIMEOUT = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan RESOURCE_LINGER = TimeSpan.FromSeconds(5);

        private readonly long keepAliveIntervalNs;
        private readonly long driverTimeoutMs;
        private readonly long driverTimeoutNs;
        private readonly long interServiceTimeoutNs;
        private readonly long publicationConnectionTimeoutMs;
        private long timeOfLastKeepalive;
        private long timeOfLastCheckResources;
        private long timeOfLastWork;
        private volatile bool driverActive = true;

        private readonly IEpochClock epochClock;
        //private readonly NanoClock nanoClock;
        private readonly DriverListenerAdapter driverListener;
        private readonly ILogBuffersFactory logBuffersFactory;
        private readonly ActivePublications activePublications = new ActivePublications();
        private readonly ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();
        private readonly List<IManagedResource> lingeringResources = new List<IManagedResource>();
        private readonly UnsafeBuffer counterValuesBuffer;
        private readonly DriverProxy driverProxy;
        private readonly IErrorHandler errorHandler;
        private readonly IAvailableImageHandler availableImageHandler;
        private readonly IUnavailableImageHandler unavailableImageHandler;

        private RegistrationException driverException;

        internal ClientConductor(
            IEpochClock epochClock,
            CopyBroadcastReceiver broadcastReceiver, 
            ILogBuffersFactory logBuffersFactory, 
            UnsafeBuffer counterValuesBuffer, 
            DriverProxy driverProxy, 
            IErrorHandler errorHandler, 
            IAvailableImageHandler availableImageHandler, 
            IUnavailableImageHandler unavailableImageHandler, 
            long keepAliveIntervalNs, 
            long driverTimeoutMs, 
            long interServiceTimeoutNs, 
            long publicationConnectionTimeoutMs)
        {
            this.epochClock = epochClock;
            //this.nanoClock = nanoClock;
            this.timeOfLastKeepalive = nanoClock.NanoTime();
            this.timeOfLastCheckResources = nanoClock.NanoTime();
            this.timeOfLastWork = nanoClock.NanoTime();
            this.errorHandler = errorHandler;
            this.counterValuesBuffer = counterValuesBuffer;
            this.driverProxy = driverProxy;
            this.logBuffersFactory = logBuffersFactory;
            this.availableImageHandler = availableImageHandler;
            this.unavailableImageHandler = unavailableImageHandler;
            this.keepAliveIntervalNs = keepAliveIntervalNs;
            this.driverTimeoutMs = driverTimeoutMs;
            this.driverTimeoutNs = MILLISECONDS.ToNanos(driverTimeoutMs);
            this.interServiceTimeoutNs = interServiceTimeoutNs;
            this.publicationConnectionTimeoutMs = publicationConnectionTimeoutMs;

            this.driverListener = new DriverListenerAdapter(broadcastReceiver, this);
        }

        public virtual void OnClose()
        {
            lock (this)
            {
                activePublications.Di();
                activeSubscriptions.di();

                Thread.Yield();

                lingeringResources.ForEach(ManagedResource::delete);
            }
        }

        public virtual int DoWork()
        {
            lock (this)
            {
                return DoWork(NO_CORRELATION_ID, null);
            }
        }

        public virtual string RoleName()
        {
            return "client-conductor";
        }

        internal virtual Publication AddPublication(string channel, int streamId)
        {
            lock (this)
            {
                VerifyDriverIsActive();

                Publication publication = activePublications.Get(channel, streamId);
                if (publication == null)
                {
                    long correlationId = driverProxy.AddPublication(channel, streamId);
                    long timeout = nanoClock.NanoTime() + driverTimeoutNs;

                    DoWorkUntil(correlationId, timeout, channel);

                    publication = activePublications.Get(channel, streamId);
                }

                publication.IncRef();

                return publication;
            }
        }

        internal virtual void ReleasePublication(Publication publication)
        {
            lock (this)
            {
                VerifyDriverIsActive();

                if (publication == activePublications.Remove(publication.Channel(), publication.StreamId()))
                {
                    long correlationId = driverProxy.RemovePublication(publication.RegistrationId());

                    long timeout = nanoClock.NanoTime() + driverTimeoutNs;

                    LingerResource(publication.ManagedResource());
                    DoWorkUntil(correlationId, timeout, publication.Channel());
                }
            }
        }

        internal virtual Subscription AddSubscription(string channel, int streamId)
        {
            lock (this)
            {
                VerifyDriverIsActive();

                long correlationId = driverProxy.AddSubscription(channel, streamId);
                long timeout = nanoClock.NanoTime() + driverTimeoutNs;

                Subscription subscription = new Subscription(this, channel, streamId, correlationId);
                activeSubscriptions.Add(subscription);

                DoWorkUntil(correlationId, timeout, channel);

                return subscription;
            }
        }

        internal virtual void ReleaseSubscription(Subscription subscription)
        {
            lock (this)
            {
                VerifyDriverIsActive();

                long correlationId = driverProxy.RemoveSubscription(subscription.RegistrationId());
                long timeout = nanoClock.NanoTime() + driverTimeoutNs;

                DoWorkUntil(correlationId, timeout, subscription.Channel());

                activeSubscriptions.Remove(subscription);
            }
        }

        public virtual void OnNewPublication(string channel, int streamId, int sessionId, int publicationLimitId, string logFileName, long correlationId)
        {
            Publication publication = new Publication(this, channel, streamId, sessionId, new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId), logBuffersFactory.Map(logFileName), correlationId);

            activePublications.Put(channel, streamId, publication);
        }

        public virtual void OnAvailableImage(int streamId, int sessionId, Long2LongHashMap subscriberPositionMap, string logFileName, string sourceIdentity, long correlationId)
        {
            activeSubscriptions.forEach(streamId, (subscription) =>
            {
                if (!subscription.HasImage(sessionId))
                {
                    long positionId = subscriberPositionMap.Get(subscription.RegistrationId());
                    if (DriverListenerAdapter.MISSING_REGISTRATION_ID != positionId)
                    {
                        Image image = new Image(subscription, sessionId, new UnsafeBufferPosition(counterValuesBuffer, (int)positionId), logBuffersFactory.Map(logFileName), errorHandler, sourceIdentity, correlationId);
                        subscription.AddImage(image);
                        availableImageHandler.OnAvailableImage(image);
                    }
                }
            });
        }

        public virtual void OnError(ErrorCode errorCode, string message, long correlationId)
        {
            driverException = new RegistrationException(errorCode, message);
        }

        public virtual void OnUnavailableImage(int streamId, long correlationId)
        {
            activeSubscriptions.forEach(streamId, (subscription) =>
            {
                Image image = subscription.RemoveImage(correlationId);
                if (null != image)
                {
                    unavailableImageHandler.OnUnavailableImage(image);
                }
            });
        }

        internal virtual DriverListenerAdapter DriverListenerAdapter()
        {
            return driverListener;
        }

        internal virtual void LingerResource(ManagedResource managedResource)
        {
            managedResource.TimeOfLastStateChange(nanoClock.NanoTime());
            lingeringResources.Add(managedResource);
        }

        internal virtual bool IsPublicationConnected(long timeOfLastStatusMessage)
        {
            return (epochClock.Time() <= (timeOfLastStatusMessage + publicationConnectionTimeoutMs));
        }

        internal virtual UnavailableImageHandler UnavailableImageHandler()
        {
            return unavailableImageHandler;
        }

        private void CheckDriverHeartbeat()
        {
            long now = epochClock.Time();
            long currentDriverKeepaliveTime = driverProxy.TimeOfLastDriverKeepalive();

            if (driverActive && (now > (currentDriverKeepaliveTime + driverTimeoutMs)))
            {
                driverActive = false;

                string msg = string.Format("Driver has been inactive for over {0:D}ms", driverTimeoutMs);
                errorHandler.OnError(new DriverTimeoutException(msg));
            }
        }

        private void VerifyDriverIsActive()
        {
            if (!driverActive)
            {
                throw new DriverTimeoutException("Driver is inactive");
            }
        }

        private int DoWork(long correlationId, string expectedChannel)
        {
            int workCount = 0;

            try
            {
                workCount += OnCheckTimeouts();
                workCount += driverListener.PollMessage(correlationId, expectedChannel);
            }
            catch (Exception ex)
            {
                errorHandler.OnError(ex);
            }

            return workCount;
        }

        private void DoWorkUntil(long correlationId, long timeout, string expectedChannel)
        {
            driverException = null;

            do
            {
                DoWork(correlationId, expectedChannel);

                if (driverListener.LastReceivedCorrelationId() == correlationId)
                {
                    if (null != driverException)
                    {
                        throw driverException;
                    }

                    return;
                }
            } while (nanoClock.NanoTime() < timeout);

            throw new DriverTimeoutException("No response from driver within timeout");
        }

        private int OnCheckTimeouts()
        {
            long now = nanoClock.NanoTime();
            int result = 0;

            if (now > (timeOfLastWork + interServiceTimeoutNs))
            {
                OnClose();

                throw new ConductorServiceTimeoutException(string.Format("Timeout between service calls over {0:D}ns", interServiceTimeoutNs));
            }

            timeOfLastWork = now;

            if (now > (timeOfLastKeepalive + keepAliveIntervalNs))
            {
                driverProxy.SendClientKeepalive();
                CheckDriverHeartbeat();

                timeOfLastKeepalive = now;
                result++;
            }

            if (now > (timeOfLastCheckResources + RESOURCE_TIMEOUT))
            {
                for (int i = lingeringResources.Size() - 1; i >= 0; i--)
                {
                    ManagedResource resource = lingeringResources.Get(i);
                    if (now > (resource.TimeOfLastStateChange() + RESOURCE_LINGER))
                    {
                        lingeringResources.Remove(i);
                        resource.Delete();
                    }
                }

                timeOfLastCheckResources = now;
                result++;
            }

            return result;
        }



    }
}