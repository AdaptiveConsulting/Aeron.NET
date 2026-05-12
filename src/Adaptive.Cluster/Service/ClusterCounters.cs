/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
    public class ClusterCounters
    {
        /// <summary>
        /// Suffix for `clusterId` in the counter label.
        /// </summary>
        public const string CLUSTER_ID_LABEL_SUFFIX = " - clusterId=";
        internal const string ServiceIdSuffix = " serviceId=";

        /// <summary>
        /// Find the counter id for a type of counter in a cluster.
        /// </summary>
        /// <param name="counters">  to search within. </param>
        /// <param name="typeId">    of the counter. </param>
        /// <param name="clusterId"> to which the allocated counter belongs. </param>
        /// <returns> the matching counter id or <seealso cref="Adaptive.Aeron.Aeron.NULL_VALUE"/> if not found.
        /// </returns>
        public static int Find(CountersReader counters, int typeId, int clusterId)
        {
            IAtomicBuffer buffer = counters.MetaDataBuffer;

            for (int i = 0, size = counters.MaxCounterId; i < size; i++)
            {
                int recordOffset = CountersReader.MetaDataOffset(i);

                var counterState = counters.GetCounterState(i);

                if (CountersReader.RECORD_ALLOCATED == counterState)
                {
                    if (
                        counters.GetCounterTypeId(i) == typeId
                        && buffer.GetInt(recordOffset + CountersReader.KEY_OFFSET) == clusterId
                    )
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
        public static Counter AllocateServiceCounter(
            Aeron.Aeron aeron,
            IMutableDirectBuffer tempBuffer,
            string name,
            int typeId,
            int clusterId,
            int serviceId
        )
        {
            int index = 0;
            tempBuffer.PutInt(index, clusterId);
            index += BitUtil.SIZE_OF_INT;
            tempBuffer.PutInt(index, serviceId);
            index += BitUtil.SIZE_OF_INT;
            int keyLength = index;

            index += tempBuffer.PutStringWithoutLengthAscii(index, name);
            index += tempBuffer.PutStringWithoutLengthAscii(index, CLUSTER_ID_LABEL_SUFFIX + clusterId);
            index += tempBuffer.PutStringWithoutLengthAscii(index, ServiceIdSuffix + serviceId);

            return aeron.AddCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, index - keyLength);
        }

        internal static Counter AllocateServiceErrorCounter(
            Aeron.Aeron aeron,
            IMutableDirectBuffer tempBuffer,
            int clusterId,
            int serviceId
        )
        {
            int index = 0;
            tempBuffer.PutInt(index, clusterId);
            index += BitUtil.SIZE_OF_INT;
            tempBuffer.PutInt(index, serviceId);
            index += BitUtil.SIZE_OF_INT;
            int keyLength = index;

            index += tempBuffer.PutStringWithoutLengthAscii(index, "Cluster Container Errors");
            index += tempBuffer.PutStringWithoutLengthAscii(index, CLUSTER_ID_LABEL_SUFFIX + clusterId);
            index += tempBuffer.PutStringWithoutLengthAscii(index, ServiceIdSuffix + serviceId);
            // index += AeronCounters.AppendVersionInfo(tempBuffer, index, ClusteredServiceContainerVersion.VERSION,
            //  ClusteredServiceContainerVersion.GIT_SHA);

            return aeron.AddCounter(
                AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
                tempBuffer,
                0,
                keyLength,
                tempBuffer,
                keyLength,
                index - keyLength
            );
        }
    }
}
