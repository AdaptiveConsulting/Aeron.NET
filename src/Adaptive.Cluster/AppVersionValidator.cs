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

using Adaptive.Agrona;

namespace Adaptive.Cluster
{
    /// <summary>
    /// Class to be used for determining AppVersion compatibility.
    /// <para>
    /// Default is to use <seealso cref="SemanticVersion"/> major version for checking compatibility.
    /// </para>
    /// </summary>
    public class AppVersionValidator
    {
        /// <summary>
        /// Singleton instance of <seealso cref="AppVersionValidator"/> version which can be used to avoid allocation.
        /// </summary>
        public static readonly AppVersionValidator SEMANTIC_VERSIONING_VALIDATOR = new AppVersionValidator();

        /// <summary>
        /// Check version compatibility between configured context appVersion and appVersion in new leadership term or
        /// snapshot.
        /// </summary>
        /// <param name="contextAppVersion">   configured appVersion value from context. </param>
        /// <param name="appVersionUnderTest"> to check against configured appVersion. </param>
        /// <returns> true for compatible or false for not compatible. </returns>
        public bool IsVersionCompatible(int contextAppVersion, int appVersionUnderTest)
        {
            return SemanticVersion.Major(contextAppVersion) == SemanticVersion.Major(appVersionUnderTest);
        }
    }
}
