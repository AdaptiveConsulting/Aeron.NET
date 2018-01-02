using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Status for an Aeron media channel for a <seealso cref="Publication"/> or <seealso cref="Subscription"/>.
    /// </summary>
    public class ChannelEndpointStatus
    {
        /// <summary>
        /// Channel is being initialized.
        /// </summary>
        public const long INITIALIZING = 0;

        /// <summary>
        /// Channel has errored. Check error log for information.
        /// </summary>
        public const long ERRORED = -1;

        /// <summary>
        /// Channel has finished initialization successfully and is active.
        /// </summary>
        public const long ACTIVE = 1;

        /// <summary>
        /// Channel is being closed.
        /// </summary>
        public const long CLOSING = 2;

        /// <summary>
        /// No counter ID is allocated yet.
        /// </summary>
        public const int NO_ID_ALLOCATED = -1;

        /// <summary>
        /// Offset in the key meta data for the channel of the counter.
        /// </summary>
        public const int CHANNEL_OFFSET = 0;

        /// <summary>
        /// String representation of the channel status.
        /// </summary>
        /// <param name="status"> to be converted. </param>
        /// <returns> representation of the channel status. </returns>
        public static string Status(long status)
        {
            if (INITIALIZING == status)
            {
                return "INITIALIZING";
            }

            if (ERRORED == status)
            {
                return "ERRORED";
            }

            if (ACTIVE == status)
            {
                return "ACTIVE";
            }

            if (CLOSING == status)
            {
                return "CLOSING";
            }

            return "unknown id=" + status;
        }

        /// <summary>
        /// The maximum length in bytes of the encoded channel identity.
        /// </summary>
        public static readonly int MAX_CHANNEL_LENGTH = CountersReader.MAX_KEY_LENGTH - (CHANNEL_OFFSET + BitUtil.SIZE_OF_INT);

        /// <summary>
        /// Allocate an indicator for tracking the status of a channel endpoint.
        /// </summary>
        /// <param name="tempBuffer">      to be used for labels and metadata. </param>
        /// <param name="name">            of the counter for the label. </param>
        /// <param name="typeId">          of the counter for classification. </param>
        /// <param name="countersManager"> from which to allocated the underlying storage. </param>
        /// <param name="channel">         for the stream of messages. </param>
        /// <returns> a new <seealso cref="AtomicCounter"/> for tracking the status. </returns>
        public static AtomicCounter Allocate(IMutableDirectBuffer tempBuffer, string name, int typeId,
            CountersManager countersManager, string channel)
        {
            int keyLength =
                tempBuffer.PutStringWithoutLengthAscii(CHANNEL_OFFSET + BitUtil.SIZE_OF_INT, channel, 0, MAX_CHANNEL_LENGTH);
            tempBuffer.PutInt(CHANNEL_OFFSET, keyLength);

            int labelLength = 0;
            labelLength += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelLength, name);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelLength, ": ");
            labelLength += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelLength, channel, 0,
                CountersReader.MAX_LABEL_LENGTH - labelLength);

            return countersManager.NewCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelLength);
        }
    }
}