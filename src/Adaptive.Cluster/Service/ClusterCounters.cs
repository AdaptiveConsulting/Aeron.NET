using Adaptive.Aeron;
using Adaptive.Agrona;
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
		/// <returns> the matching counter id or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not found. </returns>
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

		/// <summary>
		/// Allocate a counter to represent component state within a cluster.
		/// </summary>
		/// <param name="aeron">      to allocate the counter. </param>
		/// <param name="tempBuffer"> temporary storage to create label and metadata. </param>
		/// <param name="name">       of the counter for the label. </param>
		/// <param name="typeId">     for the counter. </param>
		/// <param name="clusterId">  to which the allocated counter belongs. </param>
		/// <param name="serviceId">  to which the allocated counter belongs. </param>
		/// <returns> the <seealso cref="Counter"/> for the commit position. </returns>
		public static Counter AllocateServiceCounter(Aeron.Aeron aeron, IMutableDirectBuffer tempBuffer, string name,
			int typeId, int clusterId, int serviceId)
		{
			int index = 0;
			tempBuffer.PutInt(index, clusterId);
			index += BitUtil.SIZE_OF_INT;
			tempBuffer.PutInt(index, serviceId);
			index += BitUtil.SIZE_OF_INT;
			int keyLength = index;

			index += tempBuffer.PutStringWithoutLengthAscii(index, name);
			index += tempBuffer.PutStringWithoutLengthAscii(index, " - clusterId=" + clusterId);
			index += tempBuffer.PutStringWithoutLengthAscii(index, " serviceId=" + serviceId);

			return aeron.AddCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, index - keyLength);
		}

		internal static Counter AllocateServiceErrorCounter(Aeron.Aeron aeron, IMutableDirectBuffer tempBuffer,
			int clusterId,
			int serviceId)
		{
			int index = 0;
			tempBuffer.PutInt(index, clusterId);
			index += BitUtil.SIZE_OF_INT;
			tempBuffer.PutInt(index, serviceId);
			index += BitUtil.SIZE_OF_INT;
			int keyLength = index;

			index += tempBuffer.PutStringWithoutLengthAscii(index, "Cluster Container Errors");
			index += tempBuffer.PutStringWithoutLengthAscii(index, " - clusterId=" + clusterId);
			index += tempBuffer.PutStringWithoutLengthAscii(index, " serviceId=" + serviceId);
			// index += AeronCounters.AppendVersionInfo(tempBuffer, index, ClusteredServiceContainerVersion.VERSION,
			// 	ClusteredServiceContainerVersion.GIT_SHA);

			return aeron.AddCounter(AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID, tempBuffer, 0,
				keyLength, tempBuffer, keyLength, index - keyLength);
		}
	}
}