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

using System;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Store and extract a semantic version in a 4 byte integer.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
    public class SemanticVersion
    {
        /// <summary>
        /// Compose a 4-byte integer with major, minor, and patch version stored in the least significant 3 bytes. The
        /// sum of the components must be greater than zero.
        /// </summary>
        /// <param name="major"> version in the range 0-255. </param>
        /// <param name="minor"> version in the range 0-255 </param>
        /// <param name="patch"> version in the range 0-255. </param>
        /// <returns> the semantic version made from the three components. </returns>
        /// <exception cref="ArgumentException"> if the values are outside acceptable range. </exception>
        public static int Compose(int major, int minor, int patch)
        {
            if (major < 0 || major > 255)
            {
                throw new ArgumentException("major must be 0-255: " + major);
            }

            if (minor < 0 || minor > 255)
            {
                throw new ArgumentException("minor must be 0-255: " + minor);
            }

            if (patch < 0 || patch > 255)
            {
                throw new ArgumentException("patch must be 0-255: " + patch);
            }

            if (major + minor + patch == 0)
            {
                throw new ArgumentException("all parts cannot be zero");
            }

            return (major << 16) | (minor << 8) | patch;
        }

        /// <summary>
        /// Get the major version from a composite value.
        /// </summary>
        /// <param name="version"> as a composite from which to extract the major version. </param>
        /// <returns> the major version value. </returns>
        public static int Major(int version)
        {
            return (version >> 16) & 0xFF;
        }

        /// <summary>
        /// Get the minor version from a composite value.
        /// </summary>
        /// <param name="version"> as a composite from which to extract the minor version. </param>
        /// <returns> the minor version value. </returns>
        public static int Minor(int version)
        {
            return (version >> 8) & 0xFF;
        }

        /// <summary>
        /// Get the patch version from a composite value.
        /// </summary>
        /// <param name="version"> as a composite from which to extract the patch version. </param>
        /// <returns> the patch version value. </returns>
        public static int Patch(int version)
        {
            return version & 0xFF;
        }

        /// <summary>
        /// Generate a <seealso cref="string"/> representation of the semantic version in the format
        /// {@code major.minor.patch}.
        /// </summary>
        /// <param name="version"> to be converted to a string. </param>
        /// <returns> the <seealso cref="string"/> representation of the semantic version in the format {@code
        /// major.minor.patch}. </returns>
        public static string ToString(int version)
        {
            return Major(version) + "." + Minor(version) + "." + Patch(version);
        }
    }
}
