using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// The counter that represent the role a node is playing in a cluster.
    /// </summary>
    public class ClusterNodeRole
    {
        /// <summary>
        /// Counter type id for the cluster node role.
        /// </summary>
        public const int CLUSTER_NODE_ROLE_TYPE_ID = 201;

        /// <summary>
        /// Represents a null counter id when not found.
        /// </summary>
        public const int NULL_COUNTER_ID = -1;

        /// <summary>
        /// Find the active counter id for a cluster node role.
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <returns> the counter id if found otherwise <seealso cref="NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterId(CountersReader counters)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                if (counters.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == CLUSTER_NODE_ROLE_TYPE_ID)
                    {
                        return i;
                    }
                }
            }

            return NULL_COUNTER_ID;
        }
    }

}