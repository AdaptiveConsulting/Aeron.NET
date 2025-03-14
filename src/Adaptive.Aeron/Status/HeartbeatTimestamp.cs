using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Aeron.Status
{
    /// <summary>
    /// Allocate a counter for tracking the last heartbeat of an entity with a given registration id.
    /// </summary>
    public class HeartbeatTimestamp
    {
        /// <summary>
        /// Type id of a heartbeat counter.
        /// </summary>
        public const int HEARTBEAT_TYPE_ID = AeronCounters.DRIVER_HEARTBEAT_TYPE_ID;

        /// <summary>
        /// Offset in the key metadata for the registration id of the counter.
        /// </summary>
        public const int REGISTRATION_ID_OFFSET = 0;

        /// <summary>
        /// Find the active counter id for a heartbeat timestamp.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="counterTypeId">  to match on. </param>
        /// <param name="registrationId"> for the active client. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterIdByRegistrationId(CountersReader countersReader, int counterTypeId,
            long registrationId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;

            for (int counterId = 0, maxId = countersReader.MaxCounterId; counterId < maxId; counterId++)
            {
                int counterState = countersReader.GetCounterState(counterId);
                if (counterState == CountersReader.RECORD_ALLOCATED)
                {
                    if (countersReader.GetCounterTypeId(counterId) == counterTypeId &&
                        buffer.GetLong(CountersReader.MetaDataOffset(counterId) + CountersReader.KEY_OFFSET +
                                       REGISTRATION_ID_OFFSET) == registrationId)
                    {
                        return counterId;
                    }
                }
                else if (CountersReader.RECORD_UNUSED == counterState)
                {
                    break;
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }


        /// <summary>
        /// Is the counter active for usage? Checks to see if reclaimed or reused and matches registration id.
        /// </summary>
        /// <param name="countersReader"> to search within. </param>
        /// <param name="counterId">      to test. </param>
        /// <param name="counterTypeId">  to validate type. </param>
        /// <param name="registrationId"> for the entity. </param>
        /// <returns> true if still valid otherwise false. </returns>
        public static bool IsActive(CountersReader countersReader, int counterId, int counterTypeId,
            long registrationId)
        {
            IDirectBuffer buffer = countersReader.MetaDataBuffer;
            int recordOffset = CountersReader.MetaDataOffset(counterId);

            return countersReader.GetCounterTypeId(counterId) == counterTypeId &&
                   buffer.GetLong(recordOffset + CountersReader.KEY_OFFSET + REGISTRATION_ID_OFFSET) ==
                   registrationId && countersReader.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED;
        }
    }
}