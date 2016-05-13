using System;
using System.Collections.Generic;

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
                if (!a.StartsWith("-D")) continue;
                var directive = a.Replace("-D", "").Split('=');

                if (directive.Length == 2)
                    Params.Add(directive[0], directive[1]);
            }
        }

        public static string GetProperty(string propertyName, string defaultValue)
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
    }
}