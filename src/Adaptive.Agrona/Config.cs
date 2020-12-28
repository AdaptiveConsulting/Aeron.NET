/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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

namespace Adaptive.Agrona
{
    public static class Config
    {
        public static readonly Dictionary<string, string> Params = new Dictionary<string, string>();

        static Config()
        {
            var args = Environment.GetCommandLineArgs();

            foreach (var a in args)
            {
                if (!a.StartsWith("-D")) continue;
                var directive = a.Replace("-D", "").Split('=');

                if (directive.Length == 2)
                    Params.Add(directive[0], directive[1]);
            }
        }

        public static string GetProperty(string propertyName, string defaultValue = null)
        {
            string value;
            return Params.TryGetValue(propertyName, out value) ? value : defaultValue;
        }

        public static int GetInteger(string propertyName, int defaultValue)
        {
            string strValue;
            int value;
            return Params.TryGetValue(propertyName, out strValue) && int.TryParse(strValue, out value) ? value : defaultValue;
        }

        public static bool GetBoolean(string propertyName)
        {
            var value = GetProperty(propertyName, "false");
            return value.ToLower() == "true";
        }

        public static long GetLong(string propertyName, long defaultValue)
        {
            string strValue;
            long value;
            return Params.TryGetValue(propertyName, out strValue) && long.TryParse(strValue, out value) ? value : defaultValue;
        }

        public static long GetDurationInNanos(string propertyName, long defaultValue)
        {
            return GetLong(propertyName, defaultValue);
        }

        public static int GetSizeAsInt(string propertyName, int defaultValue)
        {
            return GetInteger(propertyName, defaultValue);
        }
    }
}