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
using System.Diagnostics;
using System.IO;
using System.Threading;
using Adaptive.Agrona.Concurrent;
using NUnit.Framework;

namespace Adaptive.Aeron.Tests
{
    internal sealed class EmbeddedMediaDriver : IDisposable
    {
        private const int StartupTimeoutMs = 15_000;
        private const int ShutdownTimeoutMs = 10_000;

        private readonly Process _driver;
        private readonly string _aeronDir;

        public EmbeddedMediaDriver()
        {
            _aeronDir = Aeron.Context.GetAeronDirectoryName();
            if (Directory.Exists(_aeronDir))
            {
                try
                {
                    Directory.Delete(_aeronDir, recursive: true);
                }
                catch
                {
                }
            }

            var rootDir =
                GetSolutionDirectory(TestContext.CurrentContext.TestDirectory)?.Parent
                ?? throw new FileNotFoundException("could not find root directory of project");
            var jarPath = Path.Combine(rootDir.FullName, "driver", "media-driver.jar");

            var psi = new ProcessStartInfo
            {
                FileName = "java",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            psi.ArgumentList.Add("--add-opens");
            psi.ArgumentList.Add("java.base/jdk.internal.misc=ALL-UNNAMED");
            psi.ArgumentList.Add("--add-opens");
            psi.ArgumentList.Add("java.base/java.util.zip=ALL-UNNAMED");
            psi.ArgumentList.Add("--add-opens");
            psi.ArgumentList.Add("java.base/java.lang.reflect=ALL-UNNAMED");
            psi.ArgumentList.Add("--add-opens");
            psi.ArgumentList.Add("java.base/sun.nio.ch=ALL-UNNAMED");
            psi.ArgumentList.Add("-cp");
            psi.ArgumentList.Add(jarPath);
            psi.ArgumentList.Add($"-Daeron.dir={_aeronDir}");
            psi.ArgumentList.Add(
                "-Daeron.driver.termination.validator=io.aeron.driver.DefaultAllowTerminationValidator"
            );
            psi.ArgumentList.Add("io.aeron.driver.MediaDriver");

            _driver = Process.Start(psi) ?? throw new InvalidOperationException("failed to start media driver");

            WaitForDriverReady();
        }

        public string AeronDirectoryName => _aeronDir;

        public void Dispose()
        {
            try
            {
                var token = new UnsafeBuffer(Array.Empty<byte>());
                Aeron.Context.RequestDriverTermination(new DirectoryInfo(_aeronDir), token, 0, 0);
            }
            catch
            {
                // fall through to forceful kill
            }

            if (!_driver.WaitForExit(ShutdownTimeoutMs))
            {
                try
                {
                    _driver.Kill(entireProcessTree: true);
                }
                catch
                {
                }
                _driver.WaitForExit(ShutdownTimeoutMs);
            }
            _driver.Dispose();
        }

        private static void WaitForDriverReady()
        {
            var clock = new SystemEpochClock();
            var deadline = clock.Time() + StartupTimeoutMs;
            Exception last = null;

            while (clock.Time() < deadline)
            {
                try
                {
                    using var aeron = Aeron.Connect();
                    return;
                }
                catch (Exception e)
                {
                    last = e;
                    Thread.Sleep(50);
                }
            }

            throw new TimeoutException($"media driver did not become ready within {StartupTimeoutMs}ms", last);
        }

        private static DirectoryInfo GetSolutionDirectory(string currentPath)
        {
            var directory = new DirectoryInfo(currentPath ?? Directory.GetCurrentDirectory());
            while (directory != null && directory.GetFiles("*.sln").Length == 0)
            {
                directory = directory.Parent;
            }
            return directory;
        }
    }
}
