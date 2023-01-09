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
using System.Collections.ObjectModel;
using System.Runtime.InteropServices;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Aeron.Status;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;

namespace Adaptive.Aeron
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct SubscriptionFields
    {
        internal static readonly Image[] EMPTY_IMAGES = Array.Empty<Image>();

        // padding to prevent false sharing
        private CacheLinePadding _padding1;

        internal readonly long registrationId;
        internal readonly int streamId;
        internal int roundRobinIndex;
        internal volatile bool isClosed;
        internal volatile Image[] images;
        internal readonly ClientConductor conductor;
        internal readonly string channel;
        internal readonly AvailableImageHandler availableImageHandler;
        internal readonly UnavailableImageHandler unavailableImageHandler;
        internal int channelStatusId;

        // padding to prevent false sharing
        private CacheLinePadding _padding2;

        internal SubscriptionFields(
            long registrationId,
            int streamId,
            ClientConductor clientConductor,
            string channel,
            AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler)
        {
            _padding1 = new CacheLinePadding();
            _padding2 = new CacheLinePadding();
            channelStatusId = ChannelEndpointStatus.NO_ID_ALLOCATED;

            this.registrationId = registrationId;
            this.streamId = streamId;
            conductor = clientConductor;
            this.channel = channel;
            this.availableImageHandler = availableImageHandler;
            this.unavailableImageHandler = unavailableImageHandler;
            roundRobinIndex = 0;
            isClosed = false;
            images = EMPTY_IMAGES;
            channelStatusId = 0;
        }
    }


    /// <summary>
    /// Aeron Subscriber API for receiving a reconstructed <seealso cref="Image"/> for a stream of messages from publishers on
    /// a given channel and streamId pair, i.e. a <see cref="Publication"/>. <seealso cref="Image"/>s are aggregated under a <seealso cref="Subscription"/>.
    /// 
    /// <seealso cref="Subscription"/>s are created via an <seealso cref="Aeron"/> object, and received messages are delivered
    /// to the <seealso cref="FragmentHandler"/>.
    /// 
    /// By default, fragmented messages are not reassembled before delivery. If an application must
    /// receive whole messages, even if they were fragmented, then the Subscriber
    /// should be created with a <seealso cref="FragmentAssembler"/> or a custom implementation.
    /// 
    /// It is an application's responsibility to <seealso cref="Poll(IFragmentHandler, int)"/> the <seealso cref="Subscription"/> for new messages.
    /// 
    /// <b>Note:</b>Subscriptions are not threadsafe and should not be shared between subscribers.
    /// </summary>
    /// <seealso cref="FragmentAssembler"/>
    /// <seealso cref="ControlledFragmentHandler"/>
    /// <seealso cref="Aeron.AddSubscription(string, int)"/>
    /// <seealso cref="Aeron.AddSubscription(string, int, Adaptive.Aeron.AvailableImageHandler, Adaptive.Aeron.UnavailableImageHandler)"/>
    public class Subscription : IDisposable
    {
        private SubscriptionFields _fields;

        internal Subscription(
            ClientConductor conductor,
            string channel,
            int streamId,
            long registrationId,
            AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler)
        {
            _fields = new SubscriptionFields(registrationId, streamId, conductor, channel, availableImageHandler,
                unavailableImageHandler);
        }

        /// <summary>
        /// Media address for delivery to the channel.
        /// </summary>
        /// <returns> Media address for delivery to the channel. </returns>
        public string Channel => _fields.channel;

        /// <summary>
        /// Stream identity for scoping within the channel media address.
        /// </summary>
        /// <returns> Stream identity for scoping within the channel media address. </returns>
        public int StreamId => _fields.streamId;

        /// <summary>
        /// Return the registration id used to register this Subscription with the media driver.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId => _fields.registrationId;

        /// <summary>
        /// Callback used to indicate when an <see cref="Image"/> becomes available under this <see cref="Subscription"/>
        /// </summary>
        /// <returns> callback used to indicate when an <see cref="Image"/> becomes available under this <see cref="Subscription"/>.</returns>
        public AvailableImageHandler AvailableImageHandler => _fields.availableImageHandler;

        /// <summary>
        /// Callback used to indicate when an <see cref="Image"/> goes unavailable under this <see cref="Subscription"/>
        /// </summary>
        /// <returns> callback used to indicate when an <see cref="Image"/> goes unavailable under this <see cref="Subscription"/>.</returns>
        public UnavailableImageHandler UnavailableImageHandler => _fields.unavailableImageHandler;

        /// <summary>
        /// Poll the <seealso cref="Image"/>s under the subscription for available message fragments.
        /// <para>
        /// Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
        /// as a series of fragments ordered within a session.
        /// </para>
        /// <para>
        /// To assemble messages that span multiple fragments then use <seealso cref="FragmentAssembler"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="fragmentHandler"> callback for handling each message fragment as it is read. </param>
        /// <param name="fragmentLimit">   number of message fragments to limit when polling across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
        {
            var handler = HandlerHelper.ToFragmentHandler(fragmentHandler);
            return Poll(handler, fragmentLimit);
        }

        /// <summary>
        /// Poll the <seealso cref="Image"/>s under the subscription for available message fragments.
        /// <para>
        /// Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
        /// as a series of fragments ordered within a session.
        /// </para>
        /// <para>
        /// To assemble messages that span multiple fragments then use <seealso cref="FragmentAssembler"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="fragmentHandler"> callback for handling each message fragment as it is read. </param>
        /// <param name="fragmentLimit">   number of message fragments to limit when polling across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        public int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
        {
            var images = _fields.images;
            var length = images.Length;
            var fragmentsRead = 0;

            var startingIndex = _fields.roundRobinIndex++;
            if (startingIndex >= length)
            {
                _fields.roundRobinIndex = startingIndex = 0;
            }

            for (var i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
            {
                fragmentsRead += images[i].Poll(fragmentHandler, fragmentLimit - fragmentsRead);
            }

            for (var i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
            {
                fragmentsRead += images[i].Poll(fragmentHandler, fragmentLimit - fragmentsRead);
            }

            return fragmentsRead;
        }

        /// <summary>
        /// Poll in a controlled manner the <seealso cref="Image"/>s under the subscription for available message fragments.
        /// Control is applied to message fragments in the stream. If more fragments can be read on another stream
        /// they will even if BREAK or ABORT is returned from the fragment handler.
        /// <para>
        /// Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
        /// as a series of fragments ordered within a session.
        /// </para>
        /// <para>
        /// To assemble messages that span multiple fragments then use <seealso cref="ControlledFragmentAssembler"/>.
        ///     
        /// </para>
        /// </summary>
        /// <param name="fragmentHandler"> callback for handling each message fragment as it is read. </param>
        /// <param name="fragmentLimit">   number of message fragments to limit when polling across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        /// <seealso cref="ControlledFragmentHandler" />
        public int ControlledPoll(IControlledFragmentHandler fragmentHandler, int fragmentLimit)
        {
            var images = _fields.images;
            var length = images.Length;
            var fragmentsRead = 0;

            var startingIndex = _fields.roundRobinIndex++;
            if (startingIndex >= length)
            {
                _fields.roundRobinIndex = startingIndex = 0;
            }

            for (var i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
            {
                fragmentsRead += images[i].ControlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
            }

            for (var i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
            {
                fragmentsRead += images[i].ControlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
            }

            return fragmentsRead;
        }

        /// <summary>
        /// Poll in a controlled manner the <seealso cref="Image"/>s under the subscription for available message fragments.
        /// Control is applied to fragments in the stream. If more fragments can be read on another stream
        /// they will even if BREAK or ABORT is returned from the fragment handler.
        /// <para>
        /// Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
        /// as a series of fragments ordered within a session.
        /// </para>
        /// <para>
        /// To assemble messages that span multiple fragments then use <seealso cref="ControlledFragmentAssembler"/>.
        ///     
        /// </para>
        /// </summary>
        /// <param name="fragmentHandler"> callback for handling each message fragment as it is read. </param>
        /// <param name="fragmentLimit">   number of message fragments to limit when polling across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        /// <seealso cref="ControlledFragmentHandler" />
        public int ControlledPoll(ControlledFragmentHandler fragmentHandler, int fragmentLimit)
        {
            var handler = HandlerHelper.ToControlledFragmentHandler(fragmentHandler);
            return ControlledPoll(handler, fragmentLimit);
        }

        /// <summary>
        /// Poll the <seealso cref="Image"/>s under the subscription for available message fragments in blocks.
        /// <para>
        /// This method is useful for operations like bulk archiving and messaging indexing.
        /// 
        /// </para>
        /// </summary>
        /// <param name="blockHandler">     to receive a block of fragments from each <seealso cref="Image"/>. </param>
        /// <param name="blockLengthLimit"> for each <seealso cref="Image"/> polled. </param>
        /// <returns> the number of bytes consumed. </returns>
        public long BlockPoll(BlockHandler blockHandler, int blockLengthLimit)
        {
            long bytesConsumed = 0;
            foreach (var image in _fields.images)
            {
                bytesConsumed += image.BlockPoll(blockHandler, blockLengthLimit);
            }

            return bytesConsumed;
        }

        /// <summary>
        /// Is this subscription connected by having at least one open publication <seealso cref="Image"/>.
        /// </summary>
        /// <returns> true if this subscription connected by having at least one open publication  <seealso cref="Image"/>. </returns>
        public bool IsConnected
        {
            get
            {
                foreach (var image in _fields.images)
                {
                    if (!image.Closed)
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        /// <summary>
        /// Has this subscription currently no <see cref="Image"/>s?
        /// </summary>
        /// <returns> Has this subscription currently no <see cref="Image"/>s? </returns>
        public bool HasNoImages => _fields.images.Length == 0;

        /// <summary>
        /// Count of <see cref="Image"/>s associated to this subscription.
        /// </summary>
        /// <returns> count of <see cref="Image"/>s associated to this subscription. </returns>
        public int ImageCount => _fields.images.Length;

        /// <summary>
        /// Return the <seealso cref="Image"/> associated with the given sessionId.
        /// </summary>
        /// <param name="sessionId"> associated with the <see cref="Image"/>. </param>
        /// <returns> Image associated with the given sessionId or null if no Image exist. </returns>
        public Image ImageBySessionId(int sessionId)
        {
            Image result = null;

            foreach (var image in _fields.images)
            {
                if (sessionId == image.SessionId)
                {
                    result = image;
                    break;
                }
            }

            return result;
        }

        /// <summary>
        /// Get the <see cref="Image"/> at the given index from the images array.
        /// </summary>
        /// <param name="index"> in the array</param>
        /// <returns> image at given index</returns>
        public Image ImageAtIndex(int index)
        {
            return Images[index];
        }

        /// <summary>
        /// Get a <seealso cref="IList{T}"/> of active <seealso cref="Image"/>s that match this subscription.
        /// </summary>
        /// <returns> an unmodifiable <see cref="List{T}"/> of active <seealso cref="Image"/>s that match this subscription. </returns>
        public IList<Image> Images => new ReadOnlyCollection<Image>(_fields.images);

        /// <summary>
        /// Iterate over the <seealso cref="Image"/>s for this subscription.
        /// </summary>
        /// <param name="consumer"> to handle each <seealso cref="Image"/>. </param>
        public void ForEachImage(Action<Image> consumer)
        {
            foreach (var image in _fields.images)
            {
                consumer(image);
            }
        }

        /// <summary>
        /// Close the Subscription so that associated <seealso cref="Image"/>s can be released.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
        public void Dispose()
        {
            if (!_fields.isClosed)
            {
                _fields.conductor.RemoveSubscription(this);
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool IsClosed => _fields.isClosed;


        /// <summary>
        /// Get the status of the media channel for this Subscription.
        /// <para>
        /// The status will be <seealso cref="ChannelEndpointStatus.ERRORED"/> if a socket exception occurs on setup
        /// and <seealso cref="ChannelEndpointStatus.ACTIVE"/> if all is well.
        /// 
        /// </para>
        /// </summary>
        /// <returns> status for the channel as one of the constants from <seealso cref="ChannelEndpointStatus"/> with it being
        /// <seealso cref="ChannelEndpointStatus.NO_ID_ALLOCATED"/> if the subscription is closed. </returns>
        /// <seealso cref="ChannelEndpointStatus"></seealso>
        public long ChannelStatus
        {
            get
            {
                if (_fields.isClosed)
                {
                    return ChannelEndpointStatus.NO_ID_ALLOCATED;
                }

                return _fields.conductor.ChannelStatus(ChannelStatusId);
            }
        }


        /// <summary>
        /// Get the counter used to represent the channel status for this Subscription.
        /// </summary>
        /// <returns> the counter used to represent the channel status for this Subscription. </returns>
        public int ChannelStatusId
        {
            get => _fields.channelStatusId;
            internal set => _fields.channelStatusId = value;
        }

        /// <summary>
        /// Fetches the local socket addresses for this subscription. If the channel is not
        /// <seealso cref="ChannelEndpointStatus.ACTIVE"/>, then this will return an empty list.
        ///    
        /// The format is as follows:
        /// IPv4: <code>ip address:port</code>
        /// IPv6: <code>[ip6 address]:port</code>
        /// This is to match the formatting used in the Aeron URI.
        /// </summary>
        /// <returns> <see cref="List{T}"/> of socket addresses for this subscription. </returns>
        /// <seealso cref="ChannelStatus"/>
        public List<string> LocalSocketAddresses =>
            LocalSocketAddressStatus.FindAddresses(_fields.conductor.CountersReader(), ChannelStatus,
                ChannelStatusId);

        /// <summary>
        /// Add a destination manually to a multi-destination Subscription.
        /// </summary>
        /// <param name="endpointChannel"> for the destination to add. </param>
        public void AddDestination(string endpointChannel)
        {
            if (_fields.isClosed)
            {
                throw new AeronException("Subscription is closed");
            }

            _fields.conductor.AddRcvDestination(_fields.registrationId, endpointChannel);
        }

        /// <summary>
        /// Remove a previously added destination from a multi-destination Subscription.
        /// </summary>
        /// <param name="endpointChannel"> for the destination to remove. </param>
        public void RemoveDestination(string endpointChannel)
        {
            if (_fields.isClosed)
            {
                throw new AeronException("Subscription is closed");
            }

            _fields.conductor.RemoveRcvDestination(_fields.registrationId, endpointChannel);
        }

        /// <summary>
        /// Asynchronously add a destination manually to a multi-destination Subscription.
        /// <para>
        /// Errors will be delivered asynchronously to the <seealso cref="Aeron.Context.ErrorHandler()"/>. Completion can be
        /// tracked by passing the returned correlation id to <seealso cref="Aeron.IsCommandActive(long)"/>.
        ///    
        /// </para>
        /// </summary>
        /// <param name="endpointChannel"> for the destination to add. </param>
        /// <returns> the correlationId for the command. </returns>
        public long AsyncAddDestination(string endpointChannel)
        {
            if (_fields.isClosed)
            {
                throw new AeronException("Subscription is closed");
            }

            return _fields.conductor.AsyncAddRcvDestination(_fields.registrationId, endpointChannel);
        }

        /// <summary>
        /// Asynchronously remove a previously added destination from a multi-destination Subscription.
        /// <para>
        /// Errors will be delivered asynchronously to the <seealso cref="Aeron.Context.ErrorHandler()"/>. Completion can be
        /// tracked by passing the returned correlation id to <seealso cref="Aeron.IsCommandActive(long)"/>.
        /// 
        /// </para>
        /// </summary>
        /// <param name="endpointChannel"> for the destination to remove. </param>
        /// <returns> the correlationId for the command. </returns>
        public long AsyncRemoveDestination(string endpointChannel)
        {
            if (_fields.isClosed)
            {
                throw new AeronException("Subscription is closed");
            }

            return _fields.conductor.AsyncRemoveRcvDestination(_fields.registrationId, endpointChannel);
        }

        /// <summary>
        /// Resolve channel endpoint and replace it with the port from the ephemeral range when 0 was provided. If there are
        /// no addresses, or if there is more than one, returned from <seealso cref="LocalSocketAddresses"/> then the original
        /// <seealso cref="Channel"/> is returned.
        /// <para>
        /// If the channel is not <seealso cref="ChannelEndpointStatus.ACTIVE"/>, then {@code null} will be returned.
        /// 
        /// </para>
        /// </summary>
        /// <returns> channel URI string with an endpoint being resolved to the allocated port. </returns>
        /// <seealso cref="ChannelStatus"/>
        /// <seealso cref="LocalSocketAddresses"/>
        public string TryResolveChannelEndpointPort()
        {
            long channelStatus = ChannelStatus;

            if (ChannelEndpointStatus.ACTIVE == channelStatus)
            {
                IList<string> localSocketAddresses =
                    LocalSocketAddressStatus.FindAddresses(_fields.conductor.CountersReader(), channelStatus,
                        _fields.channelStatusId);

                if (1 == localSocketAddresses.Count)
                {
                    ChannelUri uri = ChannelUri.Parse(_fields.channel);
                    string endpoint = uri.Get(Aeron.Context.ENDPOINT_PARAM_NAME);

                    if (null != endpoint && endpoint.EndsWith(":0", StringComparison.Ordinal))
                    {
                        uri.ReplaceEndpointWildcardPort(localSocketAddresses[0]);
                        return uri.ToString();
                    }
                }

                return _fields.channel;
            }

            return null;
        }

        /// <summary>
        /// Find the resolved endpoint for the channel. This may be null if MDS is used and no destination is yet added.
        /// The result will similar to taking the first element returned from <seealso cref="LocalSocketAddresses"/>. If more than
        /// one destination is added then the first found is returned.
        /// <para>
        /// If the channel is not <seealso cref="ChannelEndpointStatus.ACTIVE"/>, then {@code null} will be returned.
        /// 
        /// </para>
        /// </summary>
        /// <returns> The resolved endpoint or null if not found. </returns>
        /// <seealso cref="ChannelStatus"/>
        /// <seealso cref="LocalSocketAddresses"/>
        public string ResolvedEndpoint =>
            LocalSocketAddressStatus.FindAddress(_fields.conductor.CountersReader(), ChannelStatus,
                _fields.channelStatusId);


        internal void InternalClose(long lingerDurationNs)
        {
            var images = _fields.images;
            _fields.images = SubscriptionFields.EMPTY_IMAGES;
            _fields.isClosed = true;
            _fields.conductor.CloseImages(images, _fields.unavailableImageHandler, lingerDurationNs);
        }

        internal void AddImage(Image image)
        {
            _fields.images = ArrayUtil.Add(_fields.images, image);
        }

        internal Image RemoveImage(long correlationId)
        {
            var oldArray = _fields.images;
            Image removedImage = null;


            int i = 0;

            foreach (var image in oldArray)
            {
                if (image.CorrelationId == correlationId)
                {
                    removedImage = image;
                    break;
                }

                i++;
            }

            if (null != removedImage)
            {
                removedImage.Close();
                _fields.images = oldArray.Length == 1 ? SubscriptionFields.EMPTY_IMAGES : ArrayUtil.Remove(oldArray, i);
                _fields.conductor.ReleaseLogBuffers(removedImage.LogBuffers, correlationId, Aeron.NULL_VALUE);
            }

            return removedImage;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "Subscription{" +
                   "registrationId=" + RegistrationId +
                   ", isClosed=" + IsClosed +
                   ", streamId=" + StreamId +
                   ", channel='" + Channel + '\'' +
                   ", localSocketAddresses='" + LocalSocketAddresses +
                   ", imageCount=" + ImageCount +
                   '}';
        }
    }
}