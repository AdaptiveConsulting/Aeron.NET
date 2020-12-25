/*
 * Copyright 2014 - 2017 Adaptive Financial Consulting Ltd
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
using System.Reflection;

namespace Adaptive.Aeron.Samples.Common
{
    internal static class RuntimeInformation
    {
        private static bool IsMono() => Type.GetType("Mono.Runtime") != null;

        internal static string GetClrVersion()
        {
            if (IsMono())
            {
                var monoRuntimeType = Type.GetType("Mono.Runtime");
                var monoDisplayName =
                    monoRuntimeType?.GetMethod("GetDisplayName", BindingFlags.NonPublic | BindingFlags.Static);
                if (monoDisplayName != null)
                    return "Mono " + monoDisplayName.Invoke(null, null);
            }

            return "MS.NET " + Environment.Version;
        }

        internal static bool HasRyuJit()
        {
            return !IsMono()
                   && IntPtr.Size == 8
                   && GetConfiguration() != "DEBUG"
                   && !new JitHelper().IsMsX64();
        }

#pragma warning disable 162
        internal static string GetConfiguration()
        {
#if DEBUG
            return "DEBUG";
#endif
            return "RELEASE";
        }
#pragma warning restore 162

        // See http://aakinshin.net/en/blog/dotnet/jit-version-determining-in-runtime/
        private class JitHelper
        {
            // ReSharper disable once NotAccessedField.Local
            private int bar;

            public bool IsMsX64(int step = 1)
            {
                var value = 0;
                for (var i = 0; i < step; i++)
                {
                    bar = i + 10;
                    for (var j = 0; j < 2 * step; j += step)
                        value = j + 10;
                }

                return value == 20 + step;
            }
        }
    }
}