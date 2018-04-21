using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Allocate a counter for tracking the last heartbeat of an entity.
    /// </summary>
    public class HeartbeatStatus
    {
        /// <summary>
        /// Offset in the key meta data for the registration id of the counter.
        /// </summary>
        public const int REGISTRATION_ID_OFFSET = 0;

        /// <summary>
        /// Allocate a counter for tracking the last heartbeat of an entity.
        /// </summary>
        /// <param name="tempBuffer">      to be used for labels and key. </param>
        /// <param name="name">            of the counter for the label. </param>
        /// <param name="typeId">          of the counter for classification. </param>
        /// <param name="countersManager"> from which to allocated the underlying storage. </param>
        /// <param name="registrationId">  to be associated with the counter. </param>
        /// <returns> a new <seealso cref="AtomicCounter"/> for tracking the last heartbeat. </returns>
        public static AtomicCounter Allocate(
            IMutableDirectBuffer tempBuffer,
            string name,
            int typeId,
            CountersManager countersManager,
            long registrationId)
        {
            return new AtomicCounter(countersManager.ValuesBuffer,
                AllocateCounterId(tempBuffer, name, typeId, countersManager, registrationId), countersManager);
        }

        public static int AllocateCounterId(
            IMutableDirectBuffer tempBuffer,
            string name,
            int typeId,
            CountersManager countersManager,
            long registrationId)
        {
            tempBuffer.PutLong(REGISTRATION_ID_OFFSET, registrationId);
            int keyLength = REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;

            int labelLength = 0;
            labelLength += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelLength, name);
            labelLength += tempBuffer.PutStringWithoutLengthAscii(keyLength + labelLength, ": ");
            labelLength += tempBuffer.PutLongAscii(keyLength + labelLength, registrationId);

            return countersManager.Allocate(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelLength);
        }
    }
}