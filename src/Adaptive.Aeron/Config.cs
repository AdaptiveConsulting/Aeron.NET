/*
 * Copyright 2014 - 2026 Adaptive Financial Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0S
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using Adaptive.Agrona;

namespace Adaptive.Aeron
{
    public static class Config
    {
        public static readonly Dictionary<string, string> Params = new Dictionary<string, string>();

        static Config()
        {
            var args = Environment.GetCommandLineArgs();

            foreach (var a in args)
            {
                if (!a.StartsWith("-D", StringComparison.Ordinal))
                {
                    continue;
                }

                var directive = a.Substring(2).Split(new[] { '=' }, 2);

                if (directive.Length == 2)
                {
                    Params[directive[0]] = directive[1];
                }
            }
        }

        public static string GetProperty(string propertyName, string defaultValue = null)
        {
            return Params.TryGetValue(propertyName, out string value) ? value : defaultValue;
        }

        public static int GetInteger(string propertyName, int defaultValue)
        {
            return Params.TryGetValue(propertyName, out string strValue)
                && int.TryParse(strValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value)
                ? value
                : defaultValue;
        }

        public static bool GetBoolean(string propertyName)
        {
            var value = GetProperty(propertyName, "false");
            return bool.TryParse(value, out var b) && b;
        }

        public static long GetLong(string propertyName, long defaultValue)
        {
            return Params.TryGetValue(propertyName, out string strValue)
                && long.TryParse(strValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value)
                ? value
                : defaultValue;
        }

        /// <summary>
        /// Returns the property as a duration in nanoseconds, accepting Java suffix notation
        /// (<c>ns</c> / <c>us</c> / <c>ms</c> / <c>s</c>) as supported by
        /// <seealso cref="SystemUtil.ParseDuration"/>. Bare integers are interpreted as nanoseconds.
        /// </summary>
        public static long GetDurationInNanos(string propertyName, long defaultValue)
        {
            return Params.TryGetValue(propertyName, out var strValue)
                ? SystemUtil.ParseDuration(propertyName, strValue)
                : defaultValue;
        }

        /// <summary>
        /// Returns the property as a size in bytes, accepting Java suffix notation
        /// (<c>k</c> / <c>m</c> / <c>g</c>) as supported by <seealso cref="SystemUtil.ParseSize"/>.
        /// Bare integers are interpreted as bytes.
        /// </summary>
        public static int GetSizeAsInt(string propertyName, int defaultValue)
        {
            if (!Params.TryGetValue(propertyName, out var strValue))
            {
                return defaultValue;
            }
            var size = SystemUtil.ParseSize(propertyName, strValue);
            if (size > int.MaxValue || size < int.MinValue)
            {
                throw new FormatException(propertyName + " out of range for int: " + strValue);
            }
            return (int)size;
        }

        /// <summary>
        /// Returns the property as a size in bytes, accepting Java suffix notation
        /// (<c>k</c> / <c>m</c> / <c>g</c>) as supported by <seealso cref="SystemUtil.ParseSize"/>.
        /// Bare integers are interpreted as bytes.
        /// </summary>
        public static long GetSizeAsLong(string propertyName, long defaultValue)
        {
            return Params.TryGetValue(propertyName, out var strValue)
                ? SystemUtil.ParseSize(propertyName, strValue)
                : defaultValue;
        }
    }
}
