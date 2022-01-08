using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// For allocating and finding cluster associated counters identified by
    /// <seealso cref="ClusteredServiceContainer.Context.ClusterId()"/>.
    /// </summary>
    public class ClusterCounters
    {
        /// <summary>
        /// Find the counter id for a type of counter in a cluster.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="typeId">    of the counter. </param>
        /// <param name="clusterId"> to which the allocated counter belongs. </param>
        /// <returns> the matching counter id or <seealso cref="Aeron.Aeron.NULL_VALUE"/> if not found. </returns>
        public static int Find(CountersReader counters, int typeId, int clusterId)
        {
            IAtomicBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                int recordOffset = CountersReader.MetaDataOffset(i);

                var counterState = counters.GetCounterState(i);

                if (CountersReader.RECORD_ALLOCATED == counterState)
                {
                    if (counters.GetCounterTypeId(i) == typeId &&
                        buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET) == clusterId)
                    {
                        return i;
                    }
                }
                else if (CountersReader.RECORD_UNUSED == counterState)
                {
                    break;
                }
            }

            return Aeron.Aeron.NULL_VALUE;
        }
    }
}