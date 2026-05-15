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
using NUnit.Framework;
using AeronClient = Adaptive.Aeron.Aeron;

namespace Adaptive.Archiver.IntegrationTests.Infrastructure
{
    /// <summary>
    /// Launches a Java media driver process bound to a caller-supplied aeron directory.
    /// Modeled on <c>Adaptive.Aeron.Tests.EmbeddedMediaDriver</c>, but accepts an explicit
    /// aeron dir so multiple integration tests can run sequentially without colliding on
    /// the process-wide default returned by <c>Aeron.Context.GetAeronDirectoryName()</c>.
    /// </summary>
    internal sealed class EmbeddedMediaDriver : IDisposable
    {
        private const int StartupTimeoutMs = 15_000;
        private const int ShutdownTimeoutMs = 10_000;

        private readonly Process _driver;
        private readonly string _aeronDir;

        public EmbeddedMediaDriver()
            : this(Path.Combine(Path.GetTempPath(), "aeron-" + Guid.NewGuid().ToString("N")))
        {
        }

        public EmbeddedMediaDriver(string aeronDirectoryName)
            : this(aeronDirectoryName, withLossGenerators: false)
        {
        }

        public EmbeddedMediaDriver(string aeronDirectoryName, bool withLossGenerators)
            : this(aeronDirectoryName, withLossGenerators, imageLivenessTimeout: "2s")
        {
        }

        public EmbeddedMediaDriver(
            string aeronDirectoryName,
            bool withLossGenerators,
            string imageLivenessTimeout)
        {
            _aeronDir = aeronDirectoryName;

            if (Directory.Exists(_aeronDir))
            {
                try { Directory.Delete(_aeronDir, recursive: true); } catch { }
            }

            var rootDir =
                GetSolutionDirectory(TestContext.CurrentContext.TestDirectory)?.Parent
                ?? throw new FileNotFoundException("could not find root directory of project");
            var jarPath = Path.Combine(rootDir.FullName, "driver", "media-driver.jar");
            var lossGenJarPath = Path.Combine(rootDir.FullName, "driver", "aeron-test-loss-generators.jar");

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
            psi.ArgumentList.Add(withLossGenerators ? jarPath + Path.PathSeparator + lossGenJarPath : jarPath);
            psi.ArgumentList.Add($"-Daeron.dir={_aeronDir}");
            psi.ArgumentList.Add(
                "-Daeron.driver.termination.validator=io.aeron.driver.DefaultAllowTerminationValidator");
            // Match Java's PersistentSubscriptionTest.setUp() driverCtxTpl which sets this so
            // that a SourceLocation.LOCAL archive recording (which subscribes via spy prefix)
            // makes the publication report IsConnected=true. Without this, MDC publications
            // recorded with LOCAL source-location stay NOT_CONNECTED and Offer() loops forever.
            psi.ArgumentList.Add("-Daeron.spies.simulate.connection=true");
            // Match Java PersistentSubscriptionTest driverCtxTpl. Short term buffers + short
            // timeouts let tests exercise the live->replay fallback path: a faster consumer
            // races past a slow persistent subscription within ~96KB of publishing, which
            // triggers image unavailable and replay fallback in seconds rather than minutes.
            psi.ArgumentList.Add("-Daeron.term.buffer.sparse.file=true");
            psi.ArgumentList.Add("-Daeron.term.buffer.length=65536");
            psi.ArgumentList.Add("-Daeron.ipc.term.buffer.length=65536");
            psi.ArgumentList.Add("-Daeron.dir.delete.on.shutdown=true");
            psi.ArgumentList.Add("-Daeron.image.liveness.timeout=" + imageLivenessTimeout);
            psi.ArgumentList.Add("-Daeron.timer.interval=100ms");
            psi.ArgumentList.Add("-Daeron.untethered.window.limit.timeout=1s");
            psi.ArgumentList.Add("-Daeron.untethered.linger.timeout=1s");
            psi.ArgumentList.Add("-Daeron.publication.linger.timeout=1s");
            if (withLossGenerators)
            {
                psi.ArgumentList.Add(
                    "-Daeron.SendChannelEndpoint.supplier="
                    + "io.adaptive.aeron.test.lossgen.LossGenSendChannelEndpointSupplier");
                psi.ArgumentList.Add(
                    "-Daeron.ReceiveChannelEndpoint.supplier="
                    + "io.adaptive.aeron.test.lossgen.LossGenReceiveChannelEndpointSupplier");
                psi.ArgumentList.Add("io.adaptive.aeron.test.lossgen.LossGenMediaDriver");
            }
            else
            {
                psi.ArgumentList.Add("io.aeron.driver.MediaDriver");
            }

            _driver = Process.Start(psi) ?? throw new InvalidOperationException("failed to start media driver");

            WaitForDriverReady();
        }

        public string AeronDirectoryName => _aeronDir;

        public void Dispose()
        {
            EmbeddedArchive.ShutdownProcess(_driver, "EmbeddedMediaDriver");

            bool exited = false;
            try { exited = _driver.HasExited; } catch { }

            _driver.Dispose();

            if (exited)
            {
                try
                {
                    if (Directory.Exists(_aeronDir))
                    {
                        Directory.Delete(_aeronDir, recursive: true);
                    }
                }
                catch
                {
                }
            }
        }

        private void WaitForDriverReady()
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(StartupTimeoutMs);
            Exception last = null;
            while (DateTime.UtcNow < deadline)
            {
                if (_driver.HasExited)
                {
                    throw new InvalidOperationException(
                        $"driver process exited prematurely with code {_driver.ExitCode}");
                }
                try
                {
                    using var aeron = AeronClient.Connect(new AeronClient.Context().AeronDirectoryName(_aeronDir));
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
