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

using Adaptive.Aeron.Exceptions;

namespace Adaptive.Cluster.Client
{
    /// <summary>
    /// Exceptions specific to Cluster operation.
    /// </summary>
    public class ClusterException : AeronException
    {
        /// <summary>
        /// Cluster exception with provided message and <seealso cref="Category.ERROR"/> .
        /// </summary>
        /// <param name="message"> to detail the exception. </param>
        public ClusterException(string message)
            : base(message) { }

        /// <summary>
        /// Cluster exception with a detailed message and provided <seealso cref="Category"/> .
        /// </summary>
        /// <param name="message">  providing detail on the error. </param>
        /// <param name="category"> of the exception. </param>
        public ClusterException(string message, Category category)
            : base(message, category) { }
    }
}
