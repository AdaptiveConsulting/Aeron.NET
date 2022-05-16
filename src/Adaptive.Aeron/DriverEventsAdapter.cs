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
using Adaptive.Aeron.Command;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using static Adaptive.Aeron.Command.ControlProtocolEvents;

namespace Adaptive.Aeron
{
    /// <summary>
    /// Analogue of the <see cref="DriverProxy"/> on the client side for dispatching driver events to the client conductor.
    /// </summary>
    internal class DriverEventsAdapter
    {
        private readonly ErrorResponseFlyweight _errorResponse = new ErrorResponseFlyweight();
        private readonly PublicationBuffersReadyFlyweight _publicationReady = new PublicationBuffersReadyFlyweight();
        private readonly SubscriptionReadyFlyweight _subscriptionReady = new SubscriptionReadyFlyweight();
        private readonly ImageBuffersReadyFlyweight _imageReady = new ImageBuffersReadyFlyweight();
        private readonly OperationSucceededFlyweight _operationSucceeded = new OperationSucceededFlyweight();
        private readonly ImageMessageFlyweight _imageMessage = new ImageMessageFlyweight();
        private readonly CounterUpdateFlyweight _counterUpdate = new CounterUpdateFlyweight();
        private readonly ClientTimeoutFlyweight _clientTimeout = new ClientTimeoutFlyweight();
        private readonly CopyBroadcastReceiver _receiver;
        private readonly ClientConductor _conductor;
        private readonly HashSet<long> _asyncCommandIdSet;
        private readonly MessageHandler _messageHandler;

        private long _activeCorrelationId;
        private long _receivedCorrelationId;
        private readonly long _clientId;
        private bool _isInvalid;

        internal DriverEventsAdapter(
            long clientId,
            CopyBroadcastReceiver receiver, 
            ClientConductor conductor,
            HashSet<long> asyncCommandIdSet)
        {
            _clientId = clientId;
            _receiver = receiver;
            _conductor = conductor;
            _asyncCommandIdSet = asyncCommandIdSet;
            _messageHandler = OnMessage;
        }

        public int Receive(long activeCorrelationId)
        {
            _activeCorrelationId = activeCorrelationId;
            _receivedCorrelationId = Aeron.NULL_VALUE;

            try
            {
                return _receiver.Receive(_messageHandler);
            }
            catch (InvalidOperationException)
            {
                _isInvalid = true;
                throw;
            }
        }

        internal long ReceivedCorrelationId => _receivedCorrelationId;

        internal bool IsInvalid => _isInvalid;

        internal long ClientId => _clientId;

        public void OnMessage(int msgTypeId, IMutableDirectBuffer buffer, int index, int length)
        {
            switch (msgTypeId)
            {
                case ON_ERROR:
                {
                    _errorResponse.Wrap(buffer, index);

                    long correlationId = _errorResponse.OffendingCommandCorrelationId();
                    int errorCodeValue = _errorResponse.ErrorCodeValue();
                    var errorCode = GetErrorCode(_errorResponse.ErrorCodeValue());
                    bool notProcessed = true;

                    if (ErrorCode.CHANNEL_ENDPOINT_ERROR == errorCode)
                    {
                        notProcessed = false;
                        _conductor.OnChannelEndpointError(correlationId, _errorResponse.ErrorMessage());
                    }
                    else if (correlationId == _activeCorrelationId)
                    {
                        notProcessed = false;
                        _receivedCorrelationId = correlationId;
                        _conductor.OnError(correlationId, errorCodeValue, errorCode, _errorResponse.ErrorMessage());
                    }
                    
                    if (_asyncCommandIdSet.Remove(correlationId) && notProcessed)
                    {
                        _conductor.OnAsyncError(correlationId, errorCodeValue, errorCode, _errorResponse.ErrorMessage());
                    }

                    break;
                }

                case ON_AVAILABLE_IMAGE:
                {
                    _imageReady.Wrap(buffer, index);

                    _conductor.OnAvailableImage(
                        _imageReady.CorrelationId(),
                        _imageReady.SessionId(),
                        _imageReady.SubscriptionRegistrationId(),
                        _imageReady.SubscriberPositionId(),
                        _imageReady.LogFileName(),
                        _imageReady.SourceIdentity());
                    break;
                }


                case ON_PUBLICATION_READY:
                {
                    _publicationReady.Wrap(buffer, index);

                    long correlationId = _publicationReady.CorrelationId();
                    if (correlationId == _activeCorrelationId || _asyncCommandIdSet.Remove(correlationId))
                    {
                        _receivedCorrelationId = correlationId;
                        _conductor.OnNewPublication(
                            correlationId,
                            _publicationReady.RegistrationId(),
                            _publicationReady.StreamId(),
                            _publicationReady.SessionId(),
                            _publicationReady.PublicationLimitCounterId(),
                            _publicationReady.ChannelStatusCounterId(),
                            _publicationReady.LogFileName());
                    }

                    break;
                }

                case ON_SUBSCRIPTION_READY:
                {
                    _subscriptionReady.Wrap(buffer, index);

                    long correlationId = _subscriptionReady.CorrelationId();
                    if (correlationId == _activeCorrelationId)
                    {
                        _receivedCorrelationId = correlationId;
                        _conductor.OnNewSubscription(correlationId, _subscriptionReady.ChannelStatusCounterId());
                    }

                    break;
                }

                case ON_OPERATION_SUCCESS:
                {
                    _operationSucceeded.Wrap(buffer, index);

                    long correlationId = _operationSucceeded.CorrelationId();
                    _asyncCommandIdSet.Remove(correlationId);
                    if (correlationId == _activeCorrelationId)
                    {
                        _receivedCorrelationId = correlationId;
                    }

                    break;
                }

                case ON_UNAVAILABLE_IMAGE:
                {
                    _imageMessage.Wrap(buffer, index);

                    _conductor.OnUnavailableImage(
                        _imageMessage.CorrelationId(), 
                        _imageMessage.SubscriptionRegistrationId()
                    );
                    break;
                }

                case ON_EXCLUSIVE_PUBLICATION_READY:
                {
                    _publicationReady.Wrap(buffer, index);

                    long correlationId = _publicationReady.CorrelationId();
                    if (correlationId == _activeCorrelationId  || _asyncCommandIdSet.Remove(correlationId))
                    {
                        _receivedCorrelationId = correlationId;
                        _conductor.OnNewExclusivePublication(
                            correlationId,
                            _publicationReady.RegistrationId(),
                            _publicationReady.StreamId(),
                            _publicationReady.SessionId(),
                            _publicationReady.PublicationLimitCounterId(),
                            _publicationReady.ChannelStatusCounterId(),
                            _publicationReady.LogFileName());
                    }

                    break;
                }

                case ON_COUNTER_READY:
                {
                    _counterUpdate.Wrap(buffer, index);

                    int counterId = _counterUpdate.CounterId();
                    long correlationId = _counterUpdate.CorrelationId();
                    if (correlationId == _activeCorrelationId)
                    {
                        _receivedCorrelationId = correlationId;
                        _conductor.OnNewCounter(correlationId, counterId);
                    }
                    else
                    {
                        _conductor.OnAvailableCounter(correlationId, counterId);
                    }

                    break;
                }

                case ON_UNAVAILABLE_COUNTER:
                {
                    _counterUpdate.Wrap(buffer, index);

                    _conductor.OnUnavailableCounter(_counterUpdate.CorrelationId(), _counterUpdate.CounterId());
                    break;
                }

                case ON_CLIENT_TIMEOUT:
                {
                    _clientTimeout.Wrap(buffer, index);

                    if (_clientTimeout.ClientId() == _clientId)
                    {
                        _conductor.OnClientTimeout();
                    }

                    break;
                }
            }
        }

        private static ErrorCode GetErrorCode(int errorCodeValue)
        {
            return Enum.IsDefined(typeof(ErrorCode), errorCodeValue)
                ? (ErrorCode) errorCodeValue
                : ErrorCode.UNKNOWN_CODE_VALUE;
        }
    }
}