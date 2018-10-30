using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent.Status;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Counter representing the commit position that can consumed by a state machine on a stream, it is the consensus
    /// position reached by the cluster.
    /// </summary>
    public class CommitPos
    {
        /// <summary>
        /// Type id of a commit position counter.
        /// </summary>
        public const int COMMIT_POSITION_TYPE_ID = 203;

        /// <summary>
        /// Human readable name for the counter.
        /// </summary>
        public const string NAME = "cluster-commit-pos";

        /// <summary>
        /// Allocate a counter to represent the commit position on stream for the current leadership term.
        /// </summary>
        /// <param name="aeron"> to allocate the counter. </param>
        /// <returns> the <seealso cref="Counter"/> for the commit position. </returns>
        public static Counter Allocate(Aeron.Aeron aeron)
        {
            return aeron.AddCounter(COMMIT_POSITION_TYPE_ID, NAME);
        }

        /// <summary>
        /// Find the active counter id for a cluster commit position
        /// </summary>
        /// <param name="counters"> to search within. </param>
        /// <returns> the counter id if found otherwise <seealso cref="CountersReader.NULL_COUNTER_ID"/>. </returns>
        public static int FindCounterId(CountersReader counters)
        {
            IDirectBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                if (counters.GetCounterState(i) == CountersReader.RECORD_ALLOCATED)
                {
                    int recordOffset = CountersReader.MetaDataOffset(i);

                    if (buffer.GetInt(recordOffset + CountersReader.TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
                    {
                        return i;
                    }
                }
            }

            return CountersReader.NULL_COUNTER_ID;
        }
    }
}