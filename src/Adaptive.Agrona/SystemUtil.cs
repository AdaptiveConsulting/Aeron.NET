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
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S1118:Utility classes should not have public constructors",
        Justification = "Public ctor in shipped API surface; marking static would break consumers."
    )]
    public class SystemUtil
    {
        private const long MaxGValue = 8589934591L;
        private const long MaxMValue = 8796093022207L;
        private const long MaxKValue = 9007199254739968L;

        private const long SecondsToNanos = 1000000000;
        private const long MillsToNanos = 1000000;
        private const long MicrosToNanos = 1000;

        /// <summary>
        /// Parse a string representation of a value with optional suffix of 'g', 'm', and 'k' suffix to indicate
        /// gigabytes, megabytes, or kilobytes respectively.
        /// </summary>
        /// <param name="propertyName">  that associated with the size value. </param>
        /// <param name="propertyValue"> to be parsed. </param>
        /// <returns> the long value. </returns>
        /// <exception cref="FormatException"> if the value is out of range or mal-formatted. </exception>
        public static long ParseSize(string propertyName, string propertyValue)
        {
            int lengthMinusSuffix = propertyValue.Length - 1;
            char lastCharacter = propertyValue[lengthMinusSuffix];
            if (char.IsDigit(lastCharacter))
            {
                return long.Parse(propertyValue);
            }

            long value = Convert.ToInt64(propertyValue.Substring(0, lengthMinusSuffix));

            switch (lastCharacter)
            {
                case 'k':
                case 'K':
                    if (value > MaxKValue)
                    {
                        throw new FormatException(propertyName + " would overflow long: " + propertyValue);
                    }

                    return value * 1024;

                case 'm':
                case 'M':
                    if (value > MaxMValue)
                    {
                        throw new FormatException(propertyName + " would overflow long: " + propertyValue);
                    }

                    return value * 1024 * 1024;

                case 'g':
                case 'G':
                    if (value > MaxGValue)
                    {
                        throw new FormatException(propertyName + " would overflow long: " + propertyValue);
                    }

                    return value * 1024 * 1024 * 1024;

                default:
                    throw new FormatException(propertyName + ": " + propertyValue + " should end with: k, m, or g.");
            }
        }

        /// <summary>
        /// Parse a string representation of a time duration with an optional suffix of 's', 'ms', 'us', or 'ns' to
        /// indicate seconds, milliseconds, microseconds, or nanoseconds respectively.
        /// <para>
        /// If the resulting duration is greater than <seealso cref="long.MaxValue"/> then
        /// <seealso cref="long.MaxValue"/> is used.
        ///
        /// </para>
        /// </summary>
        /// <param name="propertyName">  associated with the duration value. </param>
        /// <param name="propertyValue"> to be parsed. </param>
        /// <returns> the long value. </returns>
        /// <exception cref="FormatException"> if the value is negative or malformed. </exception>
        public static long ParseDuration(string propertyName, string propertyValue)
        {
            char lastCharacter = propertyValue[propertyValue.Length - 1];
            if (char.IsDigit(lastCharacter))
            {
                return long.Parse(propertyValue);
            }

            if (lastCharacter != 's' && lastCharacter != 'S')
            {
                throw new FormatException(propertyName + ": " + propertyValue + " should end with: s, ms, us, or ns.");
            }

            char secondLastCharacter = propertyValue[propertyValue.Length - 2];
            if (char.IsDigit(secondLastCharacter))
            {
                long value = Convert.ToInt64(propertyValue.Substring(0, propertyValue.Length - 1));
                return SecondsToNanos * value;
            }
            else
            {
                long value = Convert.ToInt64(propertyValue.Substring(0, propertyValue.Length - 2));

                switch (secondLastCharacter)
                {
                    case 'n':
                    case 'N':
                        return value;

                    case 'u':
                    case 'U':
                        return MicrosToNanos * value;

                    case 'm':
                    case 'M':
                        return MillsToNanos * value;

                    default:
                        throw new FormatException(
                            propertyName + ": " + propertyValue + " should end with: s, ms, us, or ns."
                        );
                }
            }
        }

        private const long OneKilobyte = 1024;
        private const long OneMegabyte = 1024 * 1024;
        private const long OneGigabyte = 1024 * 1024 * 1024;

        /// <summary>
        /// Format size value as the shortest possible string with a 'k', 'm', or 'g' suffix when the value is an exact
        /// multiple of the corresponding power-of-two. Returns the bare integer otherwise.
        /// </summary>
        /// <param name="size"> to format. Must be non-negative. </param>
        /// <returns> formatted value. </returns>
        /// <exception cref="ArgumentException"> if <paramref name="size"/> is negative. </exception>
        public static string FormatSize(long size)
        {
            if (size < 0)
            {
                throw new ArgumentException("size must be positive: " + size);
            }

            if (size >= OneGigabyte)
            {
                long value = size / OneGigabyte;
                if (size == value * OneGigabyte)
                {
                    return value + "g";
                }
            }

            if (size >= OneMegabyte)
            {
                long value = size / OneMegabyte;
                if (size == value * OneMegabyte)
                {
                    return value + "m";
                }
            }

            if (size >= OneKilobyte)
            {
                long value = size / OneKilobyte;
                if (size == value * OneKilobyte)
                {
                    return value + "k";
                }
            }

            return size.ToString();
        }

        /// <summary>
        /// Format duration value as the shortest possible string with a 'ns', 'us', 'ms', or 's' suffix. Returns the
        /// bare integer with 'ns' suffix otherwise.
        /// </summary>
        /// <param name="durationNs"> value in nanoseconds. Must be non-negative. </param>
        /// <returns> formatted value. </returns>
        /// <exception cref="ArgumentException"> if <paramref name="durationNs"/> is negative. </exception>
        public static string FormatDuration(long durationNs)
        {
            if (durationNs < 0)
            {
                throw new ArgumentException("duration must be positive: " + durationNs);
            }

            if (durationNs >= SecondsToNanos)
            {
                long value = durationNs / SecondsToNanos;
                if (durationNs == value * SecondsToNanos)
                {
                    return value + "s";
                }
            }

            if (durationNs >= MillsToNanos)
            {
                long value = durationNs / MillsToNanos;
                if (durationNs == value * MillsToNanos)
                {
                    return value + "ms";
                }
            }

            if (durationNs >= MicrosToNanos)
            {
                long value = durationNs / MicrosToNanos;
                if (durationNs == value * MicrosToNanos)
                {
                    return value + "us";
                }
            }

            return durationNs + "ns";
        }
    }
}
