using System;
using System.Collections.Generic;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Collections;

namespace Adaptive.Aeron
{
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
        private static readonly Image[] EMPTY_ARRAY = new Image[0];

        private readonly long registrationId;
        private readonly int streamId;
        private int roundRobinIndex = 0;
        private volatile bool isClosed = false;

        private volatile Image[] images = EMPTY_ARRAY;
        private readonly ClientConductor clientConductor;
        private readonly string channel;

        internal Subscription(ClientConductor conductor, string channel, int streamId, long registrationId)
        {
            this.clientConductor = conductor;
            this.channel = channel;
            this.streamId = streamId;
            this.registrationId = registrationId;
        }

        /// <summary>
        /// Media address for delivery to the channel.
        /// </summary>
        /// <returns> Media address for delivery to the channel. </returns>
        public string Channel()
        {
            return channel;
        }

        /// <summary>
        /// Stream identity for scoping within the channel media address.
        /// </summary>
        /// <returns> Stream identity for scoping within the channel media address. </returns>
        public int StreamId()
        {
            return streamId;
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
        /// <param name="fragmentLimit">   number of message fragments to limit for the poll operation across multiple <seealso cref="Image"/>s. </param>
        /// <returns> the number of fragments received </returns>
        public int Poll(IFragmentHandler fragmentHandler, int fragmentLimit)
        {
            Image[] images = this.images;
            int length = images.Length;
            int fragmentsRead = 0;

            int startingIndex = roundRobinIndex++;
            if (startingIndex >= length)
            {
                roundRobinIndex = startingIndex = 0;
            }

            for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
            {
                fragmentsRead += images[i].Poll(fragmentHandler, fragmentLimit - fragmentsRead);
            }

            for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
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
            Image[] images = this.images;
            int length = images.Length;
            int fragmentsRead = 0;

            int startingIndex = roundRobinIndex++;
            if (startingIndex >= length)
            {
                roundRobinIndex = startingIndex = 0;
            }

            for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
            {
                fragmentsRead += images[i].ControlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
            }

            for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
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
            foreach (Image image in images)
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
        ///// <param name="fileBlockHandler"> to receive a block of fragments from each <seealso cref="Image"/>. </param>
        ///// <param name="blockLengthLimit"> for each <seealso cref="Image"/> polled. </param>
        ///// <returns> the number of bytes consumed. </returns>
        //public long FilePoll(IFileBlockHandler fileBlockHandler, int blockLengthLimit)
        //{
        //    long bytesConsumed = 0;
        //    foreach (Image image in images)
        //    {
        //        bytesConsumed += image.FilePoll(fileBlockHandler, blockLengthLimit);
        //    }

        //    return bytesConsumed;
        //}

        /// <summary>
        /// Count of images connected to this subscription.
        /// </summary>
        /// <returns> count of images connected to this subscription. </returns>
        public int ImageCount()
        {
            return images.Length;
        }

        /// <summary>
        /// Return the <seealso cref="Image"/> associated with the given sessionId.
        /// </summary>
        /// <param name="sessionId"> associated with the Image. </param>
        /// <returns> Image associated with the given sessionId or null if no Image exist. </returns>
        public Image GetImage(int sessionId)
        {
            Image result = null;

            foreach (Image image in images)
            {
                if (sessionId == image.SessionId())
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
        public IList<Image> Images()
        {
            return images;
        }

        /// <summary>
        /// Iterate over the <seealso cref="Image"/>s for this subscription.
        /// </summary>
        /// <param name="imageConsumer"> to handle each <seealso cref="Image"/>. </param>
        public void ForEachImage(Action<Image> imageConsumer)
        {
            foreach (var image in images)
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
        public virtual void Dispose()
        {
            lock (clientConductor)
            {
                if (!isClosed)
                {
                    isClosed = true;

                    clientConductor.ReleaseSubscription(this);

                    foreach (Image image in images)
                    {
                        clientConductor.UnavailableImageHandler()(image);
                        clientConductor.LingerResource(image.ManagedResource());
                    }

                    images = EMPTY_ARRAY;
                }
            }
        }

        /// <summary>
        /// Has this object been closed and should no longer be used?
        /// </summary>
        /// <returns> true if it has been closed otherwise false. </returns>
        public bool Closed
        {
            get
            {
                return isClosed;
            }
        }

        /// <summary>
        /// Return the registration id used to register this Publication with the media driver.
        /// </summary>
        /// <returns> registration id </returns>
        public long RegistrationId()
        {
            return registrationId;
        }

        internal void AddImage(Image image)
        {
            if (isClosed)
            {
                clientConductor.LingerResource(image.ManagedResource());
            }
            else
            {
                images = ArrayUtil.Add(images, image);
            }
        }

        internal Image RemoveImage(long correlationId)
        {
            Image[] oldArray = images;
            Image removedImage = null;

            foreach (Image image in oldArray)
            {
                if (image.CorrelationId() == correlationId)
                {
                    removedImage = image;
                    break;
                }
            }

            if (null != removedImage)
            {
                images = ArrayUtil.Remove(oldArray, removedImage);
                clientConductor.LingerResource(removedImage.ManagedResource());
            }

            return removedImage;
        }

        internal bool HasImage(int sessionId)
        {
            bool hasImage = false;

            foreach (Image image in images)
            {
                if (sessionId == image.SessionId())
                {
                    hasImage = true;
                    break;
                }
            }

            return hasImage;
        }

        internal bool HasNoImages()
        {
            return images.Length == 0;
        }
    }
}