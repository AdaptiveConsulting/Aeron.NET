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

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Data class for holding the properties used when interacting with a cluster for local admin control.
    /// </summary>
    /// <seealso cref="ClusterMarkFile"></seealso>
    public class ClusterNodeControlProperties
    {
        /// <summary>
        /// Member id of the cluster to which the properties belong.
        /// </summary>
        public readonly int memberId;

        /// <summary>
        /// Stream id in the control channel on which the services listen.
        /// </summary>
        public readonly int serviceStreamId;

        /// <summary>
        /// Stream id in the control channel on which the consensus module listens.
        /// </summary>
        public readonly int consensusModuleStreamId;

        /// <summary>
        /// Directory where the Aeron Media Driver is running.
        /// </summary>
        public readonly string aeronDirectoryName;

        /// <summary>
        /// URI for the control channel.
        /// </summary>
        public readonly string controlChannel;

        /// <summary>
        /// Construct the set of properties for interacting with a cluster.
        /// </summary>
        /// <param name="memberId">                of the cluster to which the properties belong. </param>
        /// <param name="serviceStreamId">         in the control channel on which the services listen. </param>
        /// <param name="consensusModuleStreamId"> in the control channel on which the consensus module listens.
        /// </param>
        /// <param name="aeronDirectoryName">      where the Aeron Media Driver is running. </param>
        /// <param name="controlChannel">          for the services and consensus module. </param>
        public ClusterNodeControlProperties(
            int memberId,
            int serviceStreamId,
            int consensusModuleStreamId,
            string aeronDirectoryName,
            string controlChannel
        )
        {
            this.memberId = memberId;
            this.serviceStreamId = serviceStreamId;
            this.consensusModuleStreamId = consensusModuleStreamId;
            this.aeronDirectoryName = aeronDirectoryName;
            this.controlChannel = controlChannel;
        }
    }
}
