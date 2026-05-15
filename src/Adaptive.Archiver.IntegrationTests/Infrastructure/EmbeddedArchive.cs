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
    /// Launches a Java Aeron Archive process from the bundled <c>driver/media-driver.jar</c>
    /// (the aeron-all fat jar). Configuration is passed via <c>-Daeron.archive.*</c> system
    /// properties at JVM startup, mirroring the way <c>EmbeddedMediaDriver</c> launches the
    /// Java media driver.
    /// </summary>
    internal sealed class EmbeddedArchive : IDisposable
    {
        private const int StartupTimeoutMs = 30_000;
        private const int ShutdownTimeoutMs = 10_000;

        // Each instance binds the archive's UDP control channel to a unique random port in the
        // ephemeral range so that two tests running back-to-back can't collide if a teardown's
        // socket lingers in TIME_WAIT. The response channel uses endpoint :0 (OS-chosen).
        // Range chosen to not overlap with the MDC control port range used by the test
        // base class (25_000-30_000); see PersistentSubscriptionTest.MdcControlPort.
        private const int MinPort = 20_000;
        private const int MaxPort = 25_000;
        private static readonly Random PortPicker = new();

        private readonly string _controlChannel;
        private const string DefaultControlResponseChannel = "aeron:udp?endpoint=localhost:0";
        private const string DefaultReplicationChannel = "aeron:udp?endpoint=localhost:0";

        private readonly Process _archive;
        private readonly string _archiveDir;
        private AeronArchive _probeClient;

        public bool PreserveArchiveDirOnDispose { get; set; }

        public EmbeddedArchive(
            string aeronDirectoryName,
            string archiveDir = null,
            bool deleteArchiveOnStart = true,
            string controlChannel = null,
            AeronClient aeronClient = null)
        {
            // If the caller asked to preserve the prior recording (deleteOnStart=false),
            // they probably also want to restart against the same dir later; don't blow it
            // away on Dispose.
            PreserveArchiveDirOnDispose = !deleteArchiveOnStart;
            if (string.IsNullOrEmpty(aeronDirectoryName))
            {
                throw new ArgumentException("aeronDirectoryName must be set", nameof(aeronDirectoryName));
            }

            _archiveDir = archiveDir
                ?? Path.Combine(Path.GetTempPath(), "aeron-archive-" + Guid.NewGuid().ToString("N"));

            if (controlChannel != null)
            {
                _controlChannel = controlChannel;
            }
            else
            {
                int port;
                lock (PortPicker) { port = PortPicker.Next(MinPort, MaxPort); }
                _controlChannel = $"aeron:udp?endpoint=localhost:{port}";
            }

            // When restarting on an existing archiveDir (deleteOnStart=false) the caller wants
            // the prior recordings preserved. Don't pre-clean in that case.
            if (deleteArchiveOnStart && Directory.Exists(_archiveDir))
            {
                try
                {
                    Directory.Delete(_archiveDir, recursive: true);
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
            psi.ArgumentList.Add($"-Daeron.dir={aeronDirectoryName}");
            psi.ArgumentList.Add($"-Daeron.archive.dir={_archiveDir}");
            psi.ArgumentList.Add($"-Daeron.archive.control.channel={_controlChannel}");
            psi.ArgumentList.Add($"-Daeron.archive.control.response.channel={DefaultControlResponseChannel}");
            psi.ArgumentList.Add($"-Daeron.archive.replication.channel={DefaultReplicationChannel}");
            psi.ArgumentList.Add("-Daeron.archive.threading.mode=SHARED");
            var deleteFlag = deleteArchiveOnStart ? "true" : "false";
            psi.ArgumentList.Add($"-Daeron.archive.delete.archive.dir.on.start={deleteFlag}");
            psi.ArgumentList.Add("io.aeron.archive.Archive");

            _archive = Process.Start(psi) ?? throw new InvalidOperationException("failed to start aeron archive");

            WaitForArchiveReady(aeronDirectoryName, aeronClient);
        }

        public string ArchiveDir => _archiveDir;

        public string ControlRequestChannel => _controlChannel;

        public string ControlResponseChannel => DefaultControlResponseChannel;

        public AeronArchive.Context CreateClientContext(string aeronDirectoryName) =>
            new AeronArchive.Context()
                .ControlRequestChannel(_controlChannel)
                .ControlResponseChannel(DefaultControlResponseChannel)
                .AeronDirectoryName(aeronDirectoryName);

        /// <summary>
        /// Kills the archive JVM without disposing this wrapper or deleting the archive dir.
        /// Lets tests preserve recordings across an archive restart. The archive-mark.dat
        /// file is removed after the JVM exits because SIGKILL skips Java's graceful
        /// cleanup — a fresh archive restarting on this dir would otherwise see a recent
        /// heartbeat and reject the dir as "active".
        /// </summary>
        public void KillProcess()
        {
            // Dispose the probe before killing the JVM so its pub/sub are cleanly removed
            // from the conductor before the process disappears under them.
            try { _probeClient?.Dispose(); } catch { }
            _probeClient = null;

            ShutdownProcess(_archive, "EmbeddedArchive");

            try
            {
                var markFile = Path.Combine(_archiveDir, "archive-mark.dat");
                if (File.Exists(markFile))
                {
                    File.Delete(markFile);
                }
            }
            catch
            {
            }
        }

        public void Dispose()
        {
            ShutdownProcess(_archive, "EmbeddedArchive");

            // Dispose the probe after the JVM is dead so there is no window between
            // REMOVE_PUBLICATION and a subsequent ADD_PUBLICATION to the same channel.
            try { _probeClient?.Dispose(); } catch { }
            _probeClient = null;

            bool exited = false;
            try { exited = _archive.HasExited; } catch { }

            _archive.Dispose();

            if (exited && !PreserveArchiveDirOnDispose)
            {
                try
                {
                    if (Directory.Exists(_archiveDir))
                    {
                        Directory.Delete(_archiveDir, recursive: true);
                    }
                }
                catch
                {
                }
            }
        }

        internal static void ShutdownProcess(Process process, string name)
        {
            try
            {
                if (!process.HasExited)
                {
                    // Process.Kill maps to SIGKILL on Unix and TerminateProcess on Windows;
                    // neither can be ignored by the target. entireProcessTree is best-effort —
                    // safe on both platforms.
                    process.Kill(entireProcessTree: true);
                }
            }
            catch
            {
            }

            try { process.WaitForExit(ShutdownTimeoutMs); } catch { }

            try
            {
                if (!process.HasExited)
                {
                    NUnit.Framework.TestContext.Progress.WriteLine(
                        $"WARNING: {name} JVM pid={process.Id} did not exit within {ShutdownTimeoutMs}ms after Kill");
                }
            }
            catch
            {
                // Process object may already be disposed (e.g. by an outer using); not our problem.
            }
        }

        private void WaitForArchiveReady(string aeronDirectoryName, AeronClient aeronClient)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(StartupTimeoutMs);
            Exception last = null;

            while (DateTime.UtcNow < deadline)
            {
                if (_archive.HasExited)
                {
                    string stderr = "";
                    try { stderr = _archive.StandardError.ReadToEnd(); } catch { }
                    throw new InvalidOperationException(
                        $"archive process exited prematurely with code {_archive.ExitCode}\nSTDERR:\n{stderr}");
                }

                try
                {
                    var ctx = CreateClientContext(aeronDirectoryName);
                    // When an external aeronClient is supplied the probe AeronArchive uses it
                    // rather than creating its own embedded client. On disposal the probe only
                    // closes its internal pub/sub (fast) instead of closing the whole Aeron
                    // client, which on Windows causes the JVM conductor to spend >10s cleaning
                    // up file-mapped term buffers and would block new publication registrations.
                    if (aeronClient != null)
                    {
                        ctx.AeronClient(aeronClient);
                    }
                    // Keep the probe open for the lifetime of this EmbeddedArchive instance.
                    // Disposing it immediately creates a window between REMOVE_PUBLICATION and
                    // the test's own ADD_PUBLICATION where the archive sees no subscriber and
                    // won't send Status Messages, causing AWAIT_PUBLICATION_CONNECTED timeouts.
                    _probeClient = AeronArchive.Connect(ctx);
                    return;
                }
                catch (Exception e)
                {
                    last = e;
                    Thread.Sleep(100);
                }
            }

            throw new TimeoutException($"aeron archive did not become ready within {StartupTimeoutMs}ms", last);
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
