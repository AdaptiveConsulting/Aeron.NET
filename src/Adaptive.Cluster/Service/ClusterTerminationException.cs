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

using Adaptive.Agrona.Concurrent;

namespace Adaptive.Cluster.Service
{
    /// <summary>
    /// Used to terminate the <seealso cref="IAgent"/> within a cluster in an expected fashion.
    /// </summary>
    public class ClusterTerminationException : AgentTerminationException
    {
        private readonly bool _isExpected;

        /// <summary>
        /// Construct an exception used to terminate the cluster with {@link #isExpected()} set to true.
        /// </summary>
        public ClusterTerminationException()
            : this(true) { }

        /// <summary>
        /// Construct an exception used to terminate the cluster.
        /// </summary>
        /// <param name="isExpected"> true if the termination is expected, i.e. it was requested. </param>
        public ClusterTerminationException(bool isExpected)
            : base(isExpected ? "expected termination" : "unexpected termination")
        {
            _isExpected = isExpected;
        }

        /// <summary>
        /// Whether the termination is expected.
        /// </summary>
        /// <returns> true if expected otherwise false. </returns>
        public bool Expected
        {
            get { return _isExpected; }
        }
    }
}
