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
using System.Diagnostics;

namespace Adaptive.Archiver.IntegrationTests.Infrastructure
{
    internal static class EmbeddedProcess
    {
        public static void Shutdown(Process process, string name, int shutdownTimeoutMs = 10_000)
        {
            try
            {
                if (!process.HasExited)
                {
                    process.Kill(entireProcessTree: true);
                }
            }
            catch
            {
            }

            try
            {
                process.WaitForExit(shutdownTimeoutMs);
            }
            catch
            {
            }

            try
            {
                if (!process.HasExited)
                {
                    NUnit.Framework.TestContext.Progress.WriteLine(
                        $"WARNING: {name} JVM pid={process.Id} did not exit within {shutdownTimeoutMs}ms after Kill");
                }
            }
            catch
            {
                // Process object may already be disposed; nothing to do here.
            }
        }
    }
}
