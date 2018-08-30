using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Counter representing the heartbeat timestamp from a service.
    /// <para>
    /// Key layout as follows:
    /// <pre>
    ///   0                   1                   2                   3
    ///   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    ///  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    ///  |                         Service ID                            |
    ///  +---------------------------------------------------------------+
    ///  |                      Cluster Member ID                        |
    ///  +---------------------------------------------------------------+
    /// </pre>
    /// </para>
    /// </summary>
    public class ServiceHeartbeat
    {
        /// <summary>
        /// Type id of a service heartbeat counter.
        /// </summary>
        public const int SERVICE_HEARTBEAT_TYPE_ID = 206;

        public const int SERVICE_ID_OFFSET = 0;
        public const int MEMBER_ID_OFFSET = SERVICE_ID_OFFSET + BitUtil.SIZE_OF_INT;
        
        
        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "service-heartbeat: serviceId=";

        public static readonly int KEY_LENGTH = MEMBER_ID_OFFSET + BitUtil.SIZE_OF_INT;

        /// <summary>
        /// Allocate a counter to represent the heartbeat of a clustered service.
        /// </summary>
        /// <param name="aeron">      to allocate the counter. </param>
        /// <param name="tempBuffer"> to use for building the key and label without allocation. </param>
        /// <param name="serviceId">  of the service heartbeat. </param>
        /// <param name="clusterMemberId">  the service will be associated with. </param>
        /// <returns> the <seealso cref="Counter"/> for the commit position. </returns>
        public static Counter Allocate(Aeron.Aeron aeron, IMutableDirectBuffer tempBuffer, int serviceId, int clusterMemberId)
        {
            tempBuffer.PutInt(SERVICE_ID_OFFSET, serviceId);
            tempBuffer.PutInt(MEMBER_ID_OFFSET, clusterMemberId);
            
            int labelOffset = 0;
            labelOffset += tempBuffer.PutStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
            labelOffset += tempBuffer.PutIntAscii(KEY_LENGTH + labelOffset, serviceId);

            return aeron.AddCounter(SERVICE_HEARTBEAT_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
        }

        /// <summary>
        /// Find the active counter id for heartbeat of a given service id.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="serviceId"> to search for. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterId(CountersReader counters, int serviceId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                if (counters.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == SERVICE_HEARTBEAT_TYPE_ID &&
                        buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + SERVICE_ID_OFFSET) == serviceId)
                    {
                        return i;
                    }
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }
        
        /// <summary>
        /// Get the cluster member id this service is associated with.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="counterId"> for the active service heartbeat counter. </param>
        /// <returns> if found otherwise <seealso cref="Aeron.NULL_VALUE"/>. </returns>
        public static int GetClusterMemberId(CountersReader counters, int counterId)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            if (counters.GetCounterState(counterId) == CountersReader.RECORD_ALLOCATED)
            {
                int recordOffset = CountersReader.MetaDataOffset(counterId);

                if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == SERVICE_HEARTBEAT_TYPE_ID)
                {
                    return buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET + MEMBER_ID_OFFSET);
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }

    }
}