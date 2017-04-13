using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Collections;

namespace Adaptive.Aeron
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct SubscriptionFields
    {
        internal static readonly Image[] EMPTY_ARRAY = new Image[0];

        // padding to prevent false sharing
        private CacheLinePadding _padding1;

        internal readonly long registrationId;
        internal int roundRobinIndex;
        internal readonly int streamId;
        internal volatile bool isClosed;
        internal volatile Image[] images;
        internal readonly ClientConductor clientConductor;
        internal readonly string channel;
        
        // padding to prevent false sharing
        private CacheLinePadding _padding2;

        internal SubscriptionFields(long registrationId, int streamId, ClientConductor clientConductor, string channel)
        {
            _padding1 = new CacheLinePadding();
            _padding2 = new CacheLinePadding();

            this.registrationId = registrationId;
            this.streamId = streamId;
            this.clientConductor = clientConductor;
            this.channel = channel;
            roundRobinIndex = 0;
            isClosed = false;
            images = EMPTY_ARRAY;
        }
    }


    /// <summary>
    /// Aeron Subscriber API for receiving a reconstructed <seealso cref="Image"/> for a stream of messages from publishers on
    /// a given channel and streamId pair.
    /// <para>
    /// Subscribers are created via an <seealso cref="Aeron"/> object, and received messages are delivered
    /// to the <seealso cref="FragmentHandler"/>.
    /// </para>
    /// <para>
    /// By default fragmented messages are not reassembled before delivery. If an application must
    /// receive whole messages, whether or not they were fragmented, then the Subscriber
    /// should be created with a <seealso cref="FragmentAssembler"/> or a custom implementation.
    /// </para>
    /// <para>
    /// It is an application's responsibility to <seealso cref="Poll"/> the Subscriber for new messages.
    /// </para>
    /// <para>
    /// Subscriptions are not threadsafe and should not be shared between subscribers.
    /// 
    /// </para>
    /// </summary>
    /// <seealso cref="FragmentAssembler" />
    public class Subscription : IDisposable
    {
        private SubscriptionFields _fields;

        internal Subscription(ClientConductor conductor, string channel, int streamId, long registrationId)
        {
            _fields = new SubscriptionFields(registrationId, streamId, conductor, channel);
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
        /// <param name="fragmentLimit">   number of message fragments to limit for the poll operation across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        public int Poll(FragmentHandler fragmentHandler, int fragmentLimit)
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
        /// <param name="fragmentLimit">   number of message fragments to limit for the poll operation across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        /// <seealso cref="IControlledFragmentHandler" />
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
        /// Poll the <seealso cref="Image"/>s under the subscription for available message fragments in blocks.
        /// <para>
        /// This method is useful for operations like bulk archiving and messaging indexing.
        /// 
        /// </para>
        /// </summary>
        /// <param name="blockHandler">     to receive a block of fragments from each <seealso cref="Image"/>. </param>
        /// <param name="blockLengthLimit"> for each <seealso cref="Image"/> polled. </param>
        /// <returns> the number of bytes consumed. </returns>
        public long BlockPoll(IBlockHandler blockHandler, int blockLengthLimit)
        {
            long bytesConsumed = 0;
            foreach (var image in _fields.images)
            {
                bytesConsumed += image.BlockPoll(blockHandler, blockLengthLimit);
            }

            return bytesConsumed;
        }

        // TODO come back to this
        ///// <summary>
        ///// Poll the <seealso cref="Image"/>s under the subscription for available message fragments in blocks.
        ///// <para>
        ///// This method is useful for operations like bulk archiving a stream to file.
        ///// 
        ///// </para>
        ///// </summary>
        ///// <param name="rawBlockHandler"> to receive a block of fragments from each <seealso cref="Image"/>. </param>
        ///// <param name="blockLengthLimit"> for each <seealso cref="Image"/> polled. </param>
        ///// <returns> the number of bytes consumed. </returns>
        //public long RawPoll(IRawBlockHandler rawBlockHandler, int blockLengthLimit)
        //{
        //    long bytesConsumed = 0;
        //    foreach (Image image in images)
        //    {
        //        bytesConsumed += image.FilePoll(rawBlockHandler, blockLengthLimit);
        //    }

        //    return bytesConsumed;
        //}

        /// <summary>
        /// Count of images connected to this subscription.
        /// </summary>
        /// <returns> count of images connected to this subscription. </returns>
        public int ImageCount => _fields.images.Length;

        /// <summary>
        /// Return the <seealso cref="Image"/> associated with the given sessionId.
        /// </summary>
        /// <param name="sessionId"> associated with the Image. </param>
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
        /// Get a <seealso cref="IList{T}"/> of active <seealso cref="Image"/>s that match this subscription.
        /// </summary>
        /// <returns> a <seealso cref="List{T}"/> of active <seealso cref="Image"/>s that match this subscription. </returns>
        public IList<Image> Images => _fields.images;

        /// <summary>
        /// Iterate over the <seealso cref="Image"/>s for this subscription.
        /// </summary>
        /// <param name="imageConsumer"> to handle each <seealso cref="Image"/>. </param>
        public void ForEachImage(Action<Image> imageConsumer)
        {
            foreach (var image in _fields.images)
            {
                imageConsumer(image);
            }
        }

        /// <summary>
        /// Close the Subscription so that associated <seealso cref="Image"/>s can be released.
        /// <para>
        /// This method is idempotent.
        /// </para>
        /// </summary>
#if DEBUG
        public virtual void Dispose()
#else
        public  void Dispose()
#endif
        {
            lock (_fields.clientConductor)
            {
                if (!_fields.isClosed)
                {
                    _fields.isClosed = true;

                    _fields.clientConductor.ReleaseSubscription(this);

                    foreach (var image in _fields.images)
                    {
                        _fields.clientConductor.UnavailableImageHandler()(image);
                        _fields.clientConductor.LingerResource(image.ManagedResource());
                    }

                    _fields.images = SubscriptionFields.EMPTY_ARRAY;
                }
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool Closed => _fields.isClosed;

        /// <summary>
        /// Return the registration id used to register this Publication with the media driver.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId => _fields.registrationId;

        internal void AddImage(Image image)
        {
            if (_fields.isClosed)
            {
                _fields.clientConductor.LingerResource(image.ManagedResource());
            }
            else
            {
                _fields.images = ArrayUtil.Add(_fields.images, image);
            }
        }

        internal Image RemoveImage(long correlationId)
        {
            var oldArray = _fields.images;
            Image removedImage = null;

            foreach (var image in oldArray)
            {
                if (image.CorrelationId == correlationId)
                {
                    removedImage = image;
                    break;
                }
            }

            if (null != removedImage)
            {
                _fields.images = ArrayUtil.Remove(oldArray, removedImage);
                _fields.clientConductor.LingerResource(removedImage.ManagedResource());
            }

            return removedImage;
        }

        internal bool HasImage(long correlationId)
        {
            var hasImage = false;

            foreach (var image in _fields.images)
            {
                if (correlationId == image.CorrelationId)
                {
                    hasImage = true;
                    break;
                }
            }

            return hasImage;
        }

        internal bool HasNoImages => _fields.images.Length == 0;
    }
}