﻿using System.Collections.Generic;
using Adaptive.Aeron.Command;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Analogue of the <see cref="DriverProxy"/> on the client side
    /// </summary>

    internal class DriverListenerAdapter
    {
        public const long MISSING_REGISTRATION_ID = -1L;

        private readonly CopyBroadcastReceiver _broadcastReceiver;

        private readonly ErrorResponseFlyweight _errorResponse = new ErrorResponseFlyweight();
        private readonly PublicationBuffersReadyFlyweight _publicationReady = new PublicationBuffersReadyFlyweight();
        private readonly ImageBuffersReadyFlyweight _imageReady = new ImageBuffersReadyFlyweight();
        private readonly CorrelatedMessageFlyweight _correlatedMessage = new CorrelatedMessageFlyweight();
        private readonly ImageMessageFlyweight _imageMessage = new ImageMessageFlyweight();
        private readonly IDriverListener _listener;
        private readonly IDictionary<long, long> _subscriberPositionMap = new Dictionary<long, long>();

        private long _activeCorrelationId;
        private long _lastReceivedCorrelationId;
        private string _expectedChannel;

        internal DriverListenerAdapter(CopyBroadcastReceiver broadcastReceiver, IDriverListener listener)
        {
            _broadcastReceiver = broadcastReceiver;
            _listener = listener;
        }

        public int PollMessage(long activeCorrelationId, string expectedChannel)
        {
            _activeCorrelationId = activeCorrelationId;
            _lastReceivedCorrelationId = -1;
            _expectedChannel = expectedChannel;

            return _broadcastReceiver.Receive(OnMessage);
        }

        public long LastReceivedCorrelationId()
        {
            return _lastReceivedCorrelationId;
        }

        public void OnMessage(int msgTypeId, UnsafeBuffer buffer, int index, int length)
        {
            switch (msgTypeId)
            {
                case ControlProtocolEvents.ON_PUBLICATION_READY:
                {
                    _publicationReady.Wrap(buffer, index);

                    long correlationId = _publicationReady.CorrelationId();
                    if (correlationId == _activeCorrelationId)
                    {
                        _listener.OnNewPublication(_expectedChannel, _publicationReady.StreamId(),
                            _publicationReady.SessionId(), _publicationReady.PublicationLimitCounterId(),
                            _publicationReady.LogFileName(), correlationId);

                        _lastReceivedCorrelationId = correlationId;
                    }
                    break;
                }

                case ControlProtocolEvents.ON_AVAILABLE_IMAGE:
                {
                    _imageReady.Wrap(buffer, index);

                    _subscriberPositionMap.Clear();
                    for (int i = 0, max = _imageReady.SubscriberPositionCount(); i < max; i++)
                    {
                        long registrationId = _imageReady.PositionIndicatorRegistrationId(i);
                        int positionId = _imageReady.SubscriberPositionId(i);

                        _subscriberPositionMap.Add(registrationId, positionId);
                    }

                    _listener.OnAvailableImage(
                        _imageReady.StreamId(), 
                        _imageReady.SessionId(), 
                        _subscriberPositionMap,
                        _imageReady.LogFileName(), _imageReady.SourceIdentity(), _imageReady.CorrelationId());
                    break;
                }

                case ControlProtocolEvents.ON_OPERATION_SUCCESS:
                {
                    _correlatedMessage.Wrap(buffer, index);

                    long correlationId = _correlatedMessage.CorrelationId();
                    if (correlationId == _activeCorrelationId)
                    {
                        _lastReceivedCorrelationId = correlationId;
                    }
                    break;
                }

                case ControlProtocolEvents.ON_UNAVAILABLE_IMAGE:
                {
                    _imageMessage.Wrap(buffer, index);

                    _listener.OnUnavailableImage(_imageMessage.StreamId(), _imageMessage.CorrelationId());
                    break;
                }

                case ControlProtocolEvents.ON_ERROR:
                {
                    _errorResponse.Wrap(buffer, index);

                    long correlationId = _errorResponse.OffendingCommandCorrelationId();
                    if (correlationId == _activeCorrelationId)
                    {
                        _listener.OnError(_errorResponse.ErrorCode(), _errorResponse.ErrorMessage(), correlationId);

                        _lastReceivedCorrelationId = correlationId;
                    }
                    break;
                }
            }
        }
    }
}